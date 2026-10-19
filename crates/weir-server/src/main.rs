mod config;
mod drain;
mod logging;
mod metrics;
mod models;
mod preflight;
mod queue;
mod reload;
mod sink;
mod status;
// The whole listener layer is Unix-only, and `tcp`/`tls` live inside it. So every
// `feature = "tls"` site below is gated `all(unix, feature = "tls")`, not on the
// feature alone: `--features tls` on Windows would otherwise name `crate::socket`,
// which does not exist there, and fail to compile rather than degrade. Nothing
// builds that combination today (CI lints `--all-features` on ubuntu only), which
// is exactly why the mistake was invisible.
#[cfg(unix)]
mod socket;
// Also under `dst`: the simulation harness is compiled into a non-test build
// by that feature, and it needs the same umask-immune scratch directories.
#[cfg(any(test, feature = "dst"))]
mod testutil;
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
use drain::{DrainConfig, DrainHeartbeat};
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

// ── wab_dir lock integrity ────────────────────────────────────────────────────

/// How often the integrity thread re-checks `.weir.lock`. Bounds the residual
/// window in which a second daemon, started after the file was deleted, can
/// recover this daemon's live segments before this one stops acking. The
/// thread runs from right after acquisition, so the bound holds through
/// recovery and the startup replay too, not only once the runtime is up.
const LOCK_INTEGRITY_INTERVAL: Duration = Duration::from_secs(1);

/// Applies one [`wab::lock::check_wab_dir_lock`] verdict. Returns the exit code
/// the daemon must leave with NOW, if any; the caller exits, which keeps this
/// testable.
///
/// On `LostToOther` the order matters: the ingest flag first (every push from
/// here on is Nacked before it reaches the queue), then the gauge and the log,
/// then the exit. There is no seal (the segments now belong to the other
/// daemon, and sealing would rename its files) and no grace period (a grace
/// only lengthens the window of knowing false acks).
fn apply_lock_integrity(
    verdict: wab::lock::LockIntegrity,
    wab_dir: &Path,
    metrics: &metrics::Metrics,
    ownership_lost: &std::sync::atomic::AtomicBool,
) -> Option<i32> {
    use wab::lock::LockIntegrity;
    match verdict {
        LockIntegrity::Ok => None,
        LockIntegrity::Relocked => {
            metrics.wab_dir_lock_integrity_failures.inc();
            metrics.wab_dir_lock_held.set(1.0);
            tracing::error!(
                wab_dir = %wab_dir.display(),
                "{} was deleted or replaced while this daemon was running; nobody else \
                 held it, so it was re-created and re-locked. While it was missing a \
                 second weir-server could have started on this wab_dir. Never delete \
                 the lock file of a running daemon",
                wab::lock::LOCK_FILE_NAME
            );
            None
        }
        LockIntegrity::LostToOther { holder } => {
            ownership_lost.store(true, std::sync::atomic::Ordering::Relaxed);
            metrics.wab_dir_lock_held.set(0.0);
            let other = match &holder {
                Some(h) => format!(
                    "pid {} on host {}, version {}, socket {}",
                    h.pid, h.host, h.version, h.socket_path
                ),
                None => "its holder record could not be read".to_string(),
            };
            tracing::error!(
                wab_dir = %wab_dir.display(),
                exit_code = wab::lock::EXIT_WAB_OWNERSHIP_LOST,
                "CRITICAL: wab_dir ownership lost: another weir-server ({other}) holds or held \
                 the lock on this wab_dir and may already have recovered this daemon's live \
                 segments. Refusing every new push and exiting now, without sealing (the \
                 segments are the other daemon's now). Records acked since the lock file \
                 was deleted may not be delivered. Stop one of the two daemons"
            );
            Some(wab::lock::EXIT_WAB_OWNERSHIP_LOST)
        }
        LockIntegrity::Error(errno) => {
            metrics.wab_dir_lock_integrity_failures.inc();
            tracing::error!(
                wab_dir = %wab_dir.display(),
                errno,
                error = %std::io::Error::from_raw_os_error(errno),
                "could not verify the wab_dir lock; retrying in {}s",
                LOCK_INTEGRITY_INTERVAL.as_secs()
            );
            None
        }
    }
}

/// Spawns the once-a-second `wab_dir` lock integrity check (spec §3.1) on its
/// own thread, right after the lock is acquired and only when it is actually
/// held. A plain thread, not a runtime task, because the long startup phases
/// (recovery, and a replay that blocks at sink speed for as long as the
/// backlog lasts) run before the runtime exists: a check that only started
/// with the runtime left the stated ~1 s window unbounded there
/// (conf-durability-3). Not part of the 5 s WAB poll either, which skips
/// everything when its scan returns `None`. The first check runs at once.
fn spawn_lock_integrity_thread(
    wab_dir: std::path::PathBuf,
    metrics: Arc<metrics::Metrics>,
    ownership_lost: Arc<std::sync::atomic::AtomicBool>,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("weir-lock-integrity".into())
        .spawn(move || {
            loop {
                // A panicking check proves nothing: try again next tick, as the
                // runtime task did with a failed spawn_blocking.
                if let Ok(verdict) =
                    std::panic::catch_unwind(|| wab::lock::check_wab_dir_lock(&wab_dir))
                    && let Some(code) =
                        apply_lock_integrity(verdict, &wab_dir, &metrics, &ownership_lost)
                {
                    std::process::exit(code);
                }
                std::thread::sleep(LOCK_INTEGRITY_INTERVAL);
            }
        })
        .map(drop)
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
/// `drain::spawn_with` is generic over the sink but returns the same
/// `JoinHandle<drain::DrainExit>`, so each arm produces a uniform handle for
/// the join sequence.
fn build_and_spawn_drain<S: sink::Sink + 'static>(
    sink: S,
    drain_rx: crossbeam_channel::Receiver<std::path::PathBuf>,
    drain_config: DrainConfig,
    metrics: Arc<metrics::Metrics>,
    heartbeat: DrainHeartbeat,
    runtime: drain::DrainRuntime,
) -> std::thread::JoinHandle<drain::DrainExit> {
    drain::spawn_with(
        drain_rx,
        Arc::new(sink),
        drain_config,
        metrics,
        heartbeat,
        runtime,
    )
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
    // SIGHUP is ignored from here until the runtime installs the reload
    // handler (reload::spawn_sighup_task, first thing inside `block_on`). With
    // the default disposition a HUP (a `systemctl reload`, a log-rotation
    // script, a terminal hang-up) killed the daemon outright, skipping the seal
    // and the drain, and before this only a TLS build with tcp_bind ever
    // installed a handler. Set before Config::load so a HUP during recovery or
    // the blocking replay is dropped too: safe, because the real TLS load
    // happens later and reads the current files (spec §3.2).
    //
    // Safety: libc::umask is always safe; it swaps the process umask and
    // returns the previous value. libc::signal with SIG_IGN installs no handler
    // code, so there is nothing that must be async-signal-safe, and no other
    // thread exists yet to race the disposition change.
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
    }

    let (config, config_warnings, config_source) = Config::load()?;

    // Colour only on a terminal, and a bad `log_level` is said out loud rather
    // than silently falling back to `info` (spec §5.13, SP-D).
    if let Some(w) = logging::init(&config.log_level) {
        warn!("{w}");
    }

    // Replay any advisory warnings collected during Config::load — it runs
    // before this subscriber exists, so emitting them there would silently drop
    // them (a TOML typo would take defaults unnoticed). See Config::load (F54).
    for w in &config_warnings {
        warn!("config: {w}");
    }

    // Name the config source, so "which file did it read?" is answerable from
    // the log alone. A missing file has its own WARN above (spec §3.4).
    info!(
        config_file = %config_source.path.display(),
        origin = if config_source.explicit { "--config" } else { "default path" },
        loaded = config_source.found,
        "config source"
    );

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

    // One line with everything needed to reproduce the run (spec §5.13): the
    // ConfigView carries no secret (the S3 secret is never entered, the sink URL
    // is reduced to scheme://host[:port]).
    let config_view = Arc::new(status::config_view::ConfigView::from(&config));
    // `%` records the version with Display: a bare `&str` field is formatted
    // with Debug by tracing-subscriber and would print `version="4.1.0"`.
    info!(
        version = %env!("CARGO_PKG_VERSION"),
        wire_version = weir_core::WIRE_VERSION,
        socket = %config.socket_path.display(),
        wab_dir = %config.wab_dir.display(),
        shards = config.shard_count,
        workers = config.worker_count,
        config = ?config_view,
        "weir starting"
    );

    // ── wab_dir ownership ─────────────────────────────────────────────────────
    //
    // Before ANYTHING reads the WAB (recovery inside wab::spawn, the replay, the
    // drain) and before every other refuse-to-start check, so a duplicate on
    // the same wab_dir reports "locked by" rather than some later symptom. A
    // second daemon that got past this point would seal this one's ACTIVE
    // segment, deliver and delete it, and turn this daemon's later Durable acks
    // into false acks (spec §3.1). The lock is never released explicitly: the
    // kernel drops it at process exit, after every join below, and also on an
    // early `?` return while detached threads still run.
    let lock_holder = wab::lock::HolderInfo::current(&config);
    let lock_outcome =
        wab::lock::acquire_wab_dir_lock(&config.wab_dir, config.wab_dir_lock, &lock_holder)
            .inspect_err(|e| tracing::error!("{e}"))?;

    // ── Metrics ───────────────────────────────────────────────────────────────
    //
    // Created here, ahead of preflight, only because the integrity check below
    // needs them; nothing is served until the runtime binds the endpoint.

    let (metrics_struct, registry) = metrics::Metrics::new();
    metrics_struct.init_topology(config.shard_count, config.worker_count);
    metrics_struct.publish_build_info(env!("CARGO_PKG_VERSION"), weir_core::WIRE_VERSION);
    metrics_struct.publish_config(&config);
    let metrics = Arc::new(metrics_struct);
    let registry = Arc::new(registry);
    metrics.wab_dir_lock_held.set(match lock_outcome {
        wab::lock::LockOutcome::Held => 1.0,
        wab::lock::LockOutcome::Unsupported { .. } => 0.0,
    });

    // ── wab_dir lock integrity ────────────────────────────────────────────────
    //
    // Set once another instance provably owns the wab_dir (spec §3.1, D4b).
    // Created ONCE and cloned into every listener, like `wab_cap_rejecting`
    // (created with the status board below); the integrity thread is the only
    // writer. The thread starts now,
    // so the check covers preflight, recovery and the startup replay as well
    // as the runtime: a deletion during those phases is caught within a tick,
    // and proven loss exits 3 before anything is served.
    let wab_ownership_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if lock_outcome == wab::lock::LockOutcome::Held {
        spawn_lock_integrity_thread(
            config.wab_dir.clone(),
            Arc::clone(&metrics),
            Arc::clone(&wab_ownership_lost),
        )?;
    }

    // ── Status board (spec §6 step 3) ─────────────────────────────────────────
    //
    // Created before the WAB so the flushers' supervisors and the workers'
    // drop guards publish into the same registry the poll task and (from PR 5)
    // the probes read. `wab_bytes_now` / `wab_cap_rejecting` move up from inside
    // the runtime: the startup cap gate below writes them before any listener
    // exists, and every listener clones these same Arcs.
    #[cfg(all(unix, feature = "tls"))]
    let tcp_configured = config.tcp_bind.is_some();
    #[cfg(not(all(unix, feature = "tls")))]
    let tcp_configured = false;
    // Most recent WAB byte count, refreshed by the startup cap gate and then
    // every 5 s by the poll task. Shared with the connection handlers so the
    // ingest path can consult the WAB size without doing any I/O of its own.
    let wab_bytes_now = Arc::new(AtomicU64::new(0));
    // Whether the WAB cap is currently rejecting pushes. Created ONCE here and
    // cloned into every listener so the hysteresis low-water mark is
    // daemon-wide: a per-listener flag would let the Unix and TCP sides
    // disagree about whether ingest has resumed.
    let wab_cap_rejecting = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let board = status::StatusBoard::new(
        status::board::StaticInfo::capture(&config, Arc::clone(&config_view)),
        config.shard_count,
        config.worker_count,
        tcp_configured,
        Arc::clone(&wab_cap_rejecting),
        Arc::clone(&wab_bytes_now),
    );
    board.enter_phase(weir_status::Phase::Initializing, &metrics);

    // ── Preflight ─────────────────────────────────────────────────────────────
    //
    // Every refuse-to-start check that can run before recovery: the socket
    // path (format, then a connect-only liveness probe that never unlinks), a
    // test-bind of the metrics address, and with tcp_bind the TLS material and
    // a test-bind of the TCP address (spec §3.0). They used to fail only after
    // recovery, replay and the drain had started, so a refused duplicate under
    // Restart=on-failure re-delivered its oldest unconfirmed segment on every
    // restart. The authoritative binds and the TLS load below stay where they
    // are; preflight narrows their late failure to a race.
    //
    // Contract: no refuse-to-start check may run after the drain has begun
    // delivering.
    preflight::preflight(&config).inspect_err(|e| tracing::error!("{e}"))?;

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
        // 0 = maximum-lifetime seal disabled. Independent of the idle timer
        // above; with both set, whichever comes due first seals.
        segment_max_lifetime: (config.wab_segment_max_lifetime_secs > 0)
            .then(|| Duration::from_secs(config.wab_segment_max_lifetime_secs)),
        // "none" (default) keeps segments at format v1, byte-identical to weir
        // 1.x. Validated at config load, so any other string is unreachable.
        compression: match config.wab_compression.as_str() {
            "zstd" => weir_wab::format::Compression::Zstd,
            _ => weir_wab::format::Compression::None,
        },
        compression_level: config.wab_compression_level,
    };
    // The status board's shard registry: flusher supervisors and worker drop
    // guards record each shard's and worker's state on it (spec §5.3).
    // Recovery runs inside `wab::spawn` (until PR 3 splits it out).
    board.enter_phase(weir_status::Phase::Recovering, &metrics);
    let wab_handle = wab::spawn(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
        Arc::clone(&coalesce_hint),
        Arc::clone(&board.shards),
    )?;

    // ── Workers (queue → per-shard Batch channels → flusher directly) ────────
    //
    // Workers send `Batch`es directly to the WAB flusher via the shard channels
    // returned by `wab::spawn`. No bridge thread: the hop is
    // worker → Batch-channel → flusher.

    // The 5 s poll task reports the EWMA as evidence; the workers take the
    // original.
    let coalesce_hint_for_poll = Arc::clone(&coalesce_hint);
    let worker_handles = worker::spawn_workers_with(
        &queue_rx,
        wab_handle.shard_txs,
        config.shard_count,
        config.worker_count,
        config.batch_size,
        Duration::from_millis(config.batch_deadline_ms),
        coalesce_hint,
        Some(Arc::clone(&board.shards)),
    );

    // ── Drain ─────────────────────────────────────────────────────────────────

    // Orphan deletion (spec §4.3, D7) is safe only while this daemon owns the
    // wab_dir: with a second daemon on the same directory, a sealed segment
    // beside a valid sidecar may be that daemon's confirm in progress. The
    // ownership comes from the wab_dir lock: under `wab_dir_lock =
    // "best-effort"` on a filesystem that cannot lock, nothing proves we are
    // alone, so orphans are kept exactly as before.
    let orphan_cleanup = lock_outcome == wab::lock::LockOutcome::Held;
    let drain_config = DrainConfig {
        wab_dir: config.wab_dir.clone(),
        dead_letter_max_bytes: config.dead_letter_max_bytes,
        dead_letter_check_interval: Duration::from_secs(config.dead_letter_check_interval_secs),
        base_retry_delay: Duration::from_millis(config.sink_retry_base_delay_ms),
        max_retries: config.sink_max_retries,
        max_respawns: drain::MAX_DRAIN_RESPAWNS,
        // Backstop >= 60s and >= 2x the sink's own timeout, so it only fires if a
        // sink hangs without honouring its internal timeout.
        commit_timeout: config.commit_timeout(),
        health_poll_interval: Duration::from_secs(config.health_poll_interval_secs),
        // A health probe is a liveness check, not a delivery, so cap it well below
        // commit_timeout: 5s, or the sink's own timeout if that's shorter.
        health_probe_timeout: Duration::from_secs(config.sink_timeout_secs.min(5)),
        orphan_cleanup,
        local_read_escalation_min_age: drain::LOCAL_READ_ESCALATION_MIN_AGE,
    };
    // The stall verdict tells a slow sink call from a sink blocking the drain's
    // thread by how far past this backstop the call has run (spec §4.8).
    let commit_timeout_for_stall = drain_config.commit_timeout;

    // A counter the drain bumps once per loop iteration (see
    // `drain::DrainHeartbeat`), observed from the WAB-growth poll task below —
    // a *different* runtime/thread. The drain runs on a current-thread tokio
    // runtime, so a sink that blocks that thread outright (rather than
    // yielding) defeats both `commit_timeout` and `health_probe_timeout`; this
    // is the only liveness signal that still moves in that case.
    let drain_heartbeat = DrainHeartbeat::new();

    // Surface the configured sink type as a metric so operators (and
    // `weir-ctl metrics`) can see whether records actually go downstream or to
    // the discard-everything noop sink.
    metrics
        .sink_info
        .get_or_create(&metrics::SinkInfoLabel {
            sink_type: config.sink_type.as_str().to_string(),
        })
        .set(1.0);

    // Foundations PR 2: the drain publishes to the status board. It stays
    // Detached ("channel closed = deliver what is queued, then exit") until
    // PR 4 flips it to Attached together with the lifecycle that calls
    // request_stop. No drain-channel handle is created here: the drain owns
    // the only Receiver, the flushers the only Senders.
    let drain_runtime = drain::DrainRuntime {
        control: drain::DrainControl::detached(),
        board: Some(Arc::clone(&board)),
        initial_backlog: Vec::new(),
    };

    // Sink selection. `drain::spawn_with` is generic over the sink but returns
    // the same `JoinHandle<drain::DrainExit>`, so each arm produces a uniform
    // handle for the join sequence. `drain_runtime` moves into whichever arm
    // runs.
    let drain_handle = match config.sink_type {
        SinkType::Noop => {
            warn!(
                "sink: noop — records are acked and DISCARDED, not forwarded anywhere. \
                 This is for soak-testing the pipeline; set --sink-type to deliver downstream."
            );
            build_and_spawn_drain(
                NoopSink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
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
            build_and_spawn_drain(
                sink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
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
                // Logged because its absence is the interesting state: without
                // it the operator's UNIQUE constraint is on payload bytes, and
                // that discards distinct records that share bytes.
                id_column = ?config.sink_mysql_id_column,
                insert_mode = ?config.sink_mysql_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: mysql"
            );
            let mysql_cfg = MySqlSinkConfig {
                url,
                table: config.sink_mysql_table.clone(),
                column: config.sink_mysql_column.clone(),
                id_column: config.sink_mysql_id_column.clone(),
                insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = MySqlSink::new(mysql_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build MySQL sink: {e}"))
            })?;
            build_and_spawn_drain(
                sink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
        }
        #[cfg(feature = "postgres-sink")]
        SinkType::Postgres => {
            let url = require_sink_url(&config, "postgres")?;
            info!(
                // URL omitted from the log line for the same reason as MySQL.
                table = %config.sink_postgres_table,
                column = %config.sink_postgres_column,
                // See the MySQL arm: absence is the interesting state.
                id_column = ?config.sink_postgres_id_column,
                insert_mode = ?config.sink_postgres_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: postgres"
            );
            let pg_cfg = PostgresSinkConfig {
                url,
                table: config.sink_postgres_table.clone(),
                column: config.sink_postgres_column.clone(),
                id_column: config.sink_postgres_id_column.clone(),
                insert_mode: config.sink_postgres_insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = PostgresSink::new(pg_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build Postgres sink: {e}"))
            })?;
            build_and_spawn_drain(
                sink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
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
            build_and_spawn_drain(
                sink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
        }
        #[cfg(feature = "s3-sink")]
        SinkType::S3 => {
            if config.sink_s3_bucket.is_empty() {
                return Err(Box::<dyn std::error::Error>::from(
                    "sink_type = \"s3\" requires sink_s3_bucket",
                ));
            }
            // One object per commit batch. At the default sink_max_batch_size of
            // 100 that is roughly a 50 KB object for a 500-byte record --
            // hundreds of thousands per hour for a busy producer, which is
            // expensive to PUT and pathological for every query engine.
            if config.sink_max_batch_size < 1000 {
                warn!(
                    sink_max_batch_size = config.sink_max_batch_size,
                    "s3 sink: small batches produce many small objects; consider \
                     sink_max_batch_size = 10000. NOTE: this value determines batch \
                     boundaries, and batch boundaries determine object keys -- changing it \
                     after records have been written makes replayed batches land under new \
                     keys and duplicate. Choose it once."
                );
            }
            info!(
                bucket = %config.sink_s3_bucket,
                region = %config.sink_s3_region,
                // Endpoint omitted: it is operator-supplied and may carry a host
                // an operator would rather not see echoed. Bucket + region are
                // enough to identify the target.
                path_style = config.sink_s3_force_path_style,
                prefix = %config.sink_s3_prefix,
                partition = %config.sink_s3_partition,
                framing = %config.sink_s3_framing,
                compression = %config.sink_s3_compression,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: s3"
            );
            let opt = |v: &str| (!v.is_empty()).then(|| v.to_string());
            let s3_cfg = weir_sink_s3::S3SinkConfig {
                endpoint: opt(&config.sink_s3_endpoint),
                bucket: config.sink_s3_bucket.clone(),
                region: config.sink_s3_region.clone(),
                force_path_style: config.sink_s3_force_path_style,
                prefix: config.sink_s3_prefix.clone(),
                partition: config.sink_s3_partition.clone(),
                framing: weir_sink_s3::Framing::parse(&config.sink_s3_framing)
                    .expect("sink_s3_framing validated at config parse"),
                compression: weir_sink_s3::Compression::parse(&config.sink_s3_compression)
                    .expect("sink_s3_compression validated at config parse"),
                access_key_id: opt(&config.sink_s3_access_key_id),
                secret_access_key: opt(&config.sink_s3_secret_access_key),
                storage_class: opt(&config.sink_s3_storage_class),
                sse: opt(&config.sink_s3_sse),
                sse_kms_key_id: opt(&config.sink_s3_sse_kms_key_id),
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = weir_sink_s3::S3Sink::new(s3_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build S3 sink: {e}"))
            })?;
            build_and_spawn_drain(
                sink,
                drain_rx,
                drain_config,
                Arc::clone(&metrics),
                drain_heartbeat.clone(),
                drain_runtime,
            )
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
    board.enter_phase(weir_status::Phase::Handoff, &metrics);
    wab::replay_unconfirmed(
        &config.wab_dir,
        config.shard_count,
        &replay_tx,
        &metrics,
        orphan_cleanup,
    )?;
    drop(replay_tx); // release the startup sender so shutdown can close the drain channel

    // Spec §6 step 9: the first size scan and cap evaluation run here,
    // synchronously, before any producer listener exists (fail-closed).
    status::poll::startup_cap_gate(&config.wab_dir, config.wab_max_bytes, &board, &metrics);

    // ── Tokio runtime: socket accept loop + metrics server ────────────────────

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        // The one reload path (spec §3.2), first in the runtime: the SIGHUP
        // handler replaces the SIG_IGN set at the top of main, and from here a
        // HUP is a counted reload request instead of a no-op drop. The TLS
        // branch below registers its config once it has loaded it.
        let reloader = reload::Reloader::new(Arc::clone(&metrics), tcp_configured);
        let reload_tx = reload::spawn_reload_loop(Arc::clone(&reloader));
        #[cfg(unix)]
        reload::spawn_sighup_task(reload_tx, Arc::clone(&metrics));
        #[cfg(not(unix))]
        drop(reload_tx);

        // Bind metrics listener before starting the socket loop. Default
        // metrics_bind is 127.0.0.1 — see Config::metrics_bind for the
        // security reasoning around exposing this surface on the network.
        let metrics_listener =
            tokio::net::TcpListener::bind((config.metrics_bind, config.metrics_port))
                .await
                .map_err(|e| preflight::metrics_bind_error(&config, &e))?;
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

        // Background: queue depth and the drain channel's high-water stamp
        // every 500 ms (spec §5.11).
        let _queue_depth_task = status::poll::spawn_queue_depth_task(
            queue_tx.clone(),
            Arc::clone(&metrics),
            Arc::clone(&board),
        );

        // Background: scan WAB shard dirs for on-disk byte usage every 5 s.
        // The scan is synchronous I/O (read_dir + metadata per entry) so it
        // runs inside spawn_blocking — putting it directly on a tokio worker
        // would stall the runtime in proportion to the number of segments.
        // It also evaluates the conditions, latches them and publishes the
        // status gauges (spec §5.11), on every tick.
        let wab_dir_bg = config.wab_dir.clone();
        let metrics_w = Arc::clone(&metrics);
        let wab_bytes_bg = Arc::clone(&wab_bytes_now);
        // Captured before the task is spawned so the growth-warning predicate
        // sees the operator's configured cap without reaching back into `config`.
        let cap_for_warning = config.wab_max_bytes;
        // Same idea for the heartbeat-stall check below: the drain's own
        // `health_poll_interval` sets the grace period in
        // `drain_heartbeat_looks_wedged`. The grace period only has to cover an
        // IDLE drain now — the two long deliberate waits inside the drain loop
        // bump the heartbeat themselves, as do commits and segment reads (see
        // `drain::DrainHeartbeat`), and a single long sink call or local I/O
        // operation is named by the busy signal, so a slow sink or a slow disk
        // no longer has to be absorbed by this threshold.
        let health_poll_interval_for_stall = Duration::from_secs(config.health_poll_interval_secs);
        let drain_heartbeat_for_poll = drain_heartbeat;
        let mut poll = status::poll::WabPoll::new(
            metrics_w,
            wab_bytes_bg,
            cap_for_warning,
            drain_heartbeat_for_poll,
            status::poll::DrainLiveness::new(
                std::time::Instant::now(),
                health_poll_interval_for_stall,
                commit_timeout_for_stall,
            ),
        )
        .with_status(status::poll::PollStatus::new(
            Arc::clone(&board),
            config.wab_dir.clone(),
            coalesce_hint_for_poll,
        ));
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let wab_dir = wab_dir_bg.clone();
                poll.tick(std::time::Instant::now(), move || {
                    tokio::task::spawn_blocking(move || status::poll::compute_wab_bytes_on_disk(&wab_dir))
                })
                .await;
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
        board.enter_phase(weir_status::Phase::Binding, &metrics);
        #[cfg(all(unix, feature = "tls"))]
        let (tcp_shutdown_tx, tcp_join) = {
            use socket::tcp::{self, TcpConfig};
            use socket::tls::ReloadableServerConfig;

            match config.tcp_bind {
                Some(bind_addr) => {
                    // Config validation guarantees the three tls_* paths are
                    // present whenever tcp_bind is set; preflight has already
                    // loaded this material once and test-bound this address,
                    // so a failure here is a race (spec §3.0).
                    let (cert, key, ca) = preflight::tls_paths(&config)?;
                    let tls = ReloadableServerConfig::load(cert, key, ca)
                        .map_err(|e| preflight::tls_load_error(&e))?;

                    // Bind in the caller so a bad bind address fails startup
                    // (rather than the spawned accept task) — see socket::tcp
                    // module docs on the bound-addr design.
                    let listener = tokio::net::TcpListener::bind(bind_addr)
                        .await
                        .map_err(|e| preflight::tcp_bind_error(bind_addr, &e))?;
                    let actual_addr = listener.local_addr()?;
                    info!(addr = %actual_addr, "TCP+mTLS listener bound");
                    board.listeners.tcp.set(weir_status::ListenerState::Bound);

                    let tcp_config = TcpConfig {
                        max_connections: config.max_connections,
                        max_payload_bytes: config.max_payload_bytes,
                        max_batch_records: config.max_batch_records,
                        shard_count: config.shard_count,
                        shutdown_timeout_secs: config.shutdown_timeout_secs,
                        connection_read_timeout_secs: config.connection_read_timeout_secs,
                        handshake_timeout_secs: config.tls_handshake_timeout_secs,
                        wab_max_bytes: config.wab_max_bytes,
                        wab_bytes_now: Arc::clone(&wab_bytes_now),
                        wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
                        wab_ownership_lost: Arc::clone(&wab_ownership_lost),
                    };

                    let (tcp_shutdown_tx, tcp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                    let tcp_queue_tx = queue_tx.clone();
                    let tcp_metrics = Arc::clone(&metrics);

                    // Register for reload before moving `tls` into tcp::run.
                    // The clone shares the same inner ArcSwap so a reload from the
                    // reload loop is immediately visible to the accept loop's
                    // `tls.current()` call.
                    reloader.register_tls(tls.clone());

                    // Pass a clone of the shared semaphore so Unix + TCP draw
                    // from the same permit pool — true global cap.
                    let tcp_sem = Arc::clone(&conn_sem);
                    let tcp_board = Arc::clone(&board);
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
                        match res {
                            Ok(()) => tcp_board.listeners.tcp.set(weir_status::ListenerState::Closed),
                            Err(e) => {
                                tcp_board.listeners.tcp.set(weir_status::ListenerState::Failed);
                                tracing::error!(error = %e, "TCP+mTLS listener exited with error");
                            }
                        }
                    });

                    (Some(tcp_shutdown_tx), Some(tcp_join))
                }
                None => (None, None),
            }
        };

        let board_sig = Arc::clone(&board);
        let metrics_sig = Arc::clone(&metrics);
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
            // T0 (spec §7 a-b): the board says shutting_down before any listener
            // closes, so a flusher whose input closes from here on reads as a clean
            // `Stopped`, not `Offline(FlusherExited)`. PR 4's Lifecycle takes this over.
            board_sig.begin_shutdown(&metrics_sig);
            #[cfg(all(unix, feature = "tls"))]
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
                max_batch_records: config.max_batch_records,
                shutdown_timeout_secs: config.shutdown_timeout_secs,
                connection_read_timeout_secs: config.connection_read_timeout_secs,
                shard_count: config.shard_count,
                peer_uid_check: config.peer_uid_check,
                wab_max_bytes: config.wab_max_bytes,
                wab_bytes_now: Arc::clone(&wab_bytes_now),
                wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
                wab_ownership_lost: Arc::clone(&wab_ownership_lost),
                board: Some(Arc::clone(&board)),
            };
            socket::run(socket_config, queue_tx, shutdown_rx, Arc::clone(&metrics), conn_sem).await?;
        }
        #[cfg(not(unix))]
        {
            // On non-Unix builds weir-server is not supported; just wait for shutdown.
            board.enter_phase(weir_status::Phase::Serving, &metrics);
            let _ = shutdown_rx.await;
        }

        // Await the TCP+mTLS listener's graceful drain before leaving the runtime.
        // The signal handler already fired `tcp_shutdown_tx`, so `tcp::run` is
        // draining its in-flight connections; without this await `drop(rt)` below
        // would cancel that drain the moment the Unix loop returns (e.g. when there
        // are no Unix connections), severing live TCP connections instead of
        // letting them finish within `shutdown_timeout_secs` (S09).
        #[cfg(all(unix, feature = "tls"))]
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
mod lock_integrity_tests {
    use super::apply_lock_integrity;
    use crate::metrics::Metrics;
    use crate::wab::lock::{EXIT_WAB_OWNERSHIP_LOST, HolderInfo, LockIntegrity};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn fresh() -> (Metrics, AtomicBool) {
        let (m, _reg) = Metrics::new();
        m.wab_dir_lock_held.set(1.0);
        (m, AtomicBool::new(false))
    }

    /// conf-tests-14: a refusal by the ownership flag and one by the process
    /// exiting look alike from a client, so no system test can tell whether
    /// the listeners read the flag the integrity thread sets. This pins the
    /// wiring in `main` instead: the flag is created once, and the integrity
    /// thread and both listener configs (Unix and TCP) each get a clone of
    /// that one `Arc`, never a fresh `AtomicBool`. A source check, like
    /// `lock_module_never_reopens_path`; the needles are built at run time so
    /// this test's own text never matches them.
    #[test]
    fn ownership_flag_is_one_arc_shared_by_the_integrity_thread_and_the_listeners() {
        let src = include_str!("main.rs");
        let name = "wab_ownership_lost";
        let clone = format!("Arc::clone(&{name})");

        let created = format!("let {name} = ");
        assert_eq!(src.matches(&created).count(), 1, "the flag is created once");
        let line = src[src.find(&created).unwrap()..].lines().next().unwrap();
        assert!(
            line.contains("Arc::new(") && line.contains("AtomicBool::new(false)"),
            "{line}"
        );

        let field = format!("{name}: ");
        let cloned_field = format!("{name}: {clone},");
        assert_eq!(
            src.matches(&field).count(),
            2,
            "one ownership field per listener config (Unix and TCP)"
        );
        assert_eq!(
            src.matches(&cloned_field).count(),
            2,
            "every listener config gets a clone of the one flag"
        );

        let call = format!("{}(", "spawn_lock_integrity_thread");
        let at = src
            .match_indices(&call)
            .map(|(i, _)| i)
            .find(|&i| !src[..i].ends_with("fn "))
            .expect("main spawns the integrity thread");
        let args = &src[at..at + src[at..].find(")?").expect("call ends with ?")];
        assert!(
            args.contains(&clone),
            "the integrity thread gets the same flag: {args}"
        );
    }

    #[test]
    fn exit_code_for_ownership_lost_is_3() {
        // 1 stays "refused to start"; 3 is the new, distinct "ownership lost".
        assert_eq!(EXIT_WAB_OWNERSHIP_LOST, 3);
    }

    #[test]
    fn ok_changes_nothing() {
        let (m, lost) = fresh();
        assert_eq!(
            apply_lock_integrity(LockIntegrity::Ok, Path::new("/w"), &m, &lost),
            None
        );
        assert!(!lost.load(Ordering::Relaxed));
        assert_eq!(m.wab_dir_lock_integrity_failures.get(), 0);
        assert_eq!(m.wab_dir_lock_held.get(), 1.0);
    }

    /// D4a: deleted by hand, nobody else holds it. Loud, counted, no stop.
    #[test]
    fn relocked_counts_keeps_the_gauge_and_keeps_running() {
        let (m, lost) = fresh();
        assert_eq!(
            apply_lock_integrity(LockIntegrity::Relocked, Path::new("/w"), &m, &lost),
            None
        );
        assert!(
            !lost.load(Ordering::Relaxed),
            "a re-lock must not stop ingest"
        );
        assert_eq!(m.wab_dir_lock_integrity_failures.get(), 1);
        assert_eq!(m.wab_dir_lock_held.get(), 1.0);
    }

    #[test]
    fn error_counts_and_retries() {
        let (m, lost) = fresh();
        assert_eq!(
            apply_lock_integrity(LockIntegrity::Error(libc::EIO), Path::new("/w"), &m, &lost),
            None
        );
        assert!(!lost.load(Ordering::Relaxed), "an errno proves nothing");
        assert_eq!(m.wab_dir_lock_integrity_failures.get(), 1);
        assert_eq!(m.wab_dir_lock_held.get(), 1.0);
    }

    /// D4b: proof that a second writer owns the directory. Ingest is refused
    /// first, the gauge drops to 0, and the caller is told to exit 3. It is not
    /// counted as an integrity failure: it is an exit, not a retry.
    #[test]
    fn lost_to_other_stops_ingest_drops_the_gauge_and_exits_3() {
        let (m, lost) = fresh();
        let verdict = LockIntegrity::LostToOther {
            holder: Some(HolderInfo {
                pid: 4242,
                host: "weir-b".into(),
                ..HolderInfo::default()
            }),
        };
        assert_eq!(
            apply_lock_integrity(verdict, Path::new("/w"), &m, &lost),
            Some(EXIT_WAB_OWNERSHIP_LOST)
        );
        assert!(
            lost.load(Ordering::Relaxed),
            "every later push must be Nacked"
        );
        assert_eq!(m.wab_dir_lock_held.get(), 0.0);
        assert_eq!(m.wab_dir_lock_integrity_failures.get(), 0);

        let (m, lost) = fresh();
        assert_eq!(
            apply_lock_integrity(
                LockIntegrity::LostToOther { holder: None },
                Path::new("/w"),
                &m,
                &lost
            ),
            Some(EXIT_WAB_OWNERSHIP_LOST),
            "an unreadable record is still proof of loss"
        );
        assert!(lost.load(Ordering::Relaxed));
    }
}
