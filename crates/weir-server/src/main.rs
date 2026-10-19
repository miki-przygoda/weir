mod config;
mod drain;
// The shutdown level, the signal task and the bounded drain wait (Foundations
// §5.6, §7).
mod lifecycle;
mod logging;
mod metrics;
mod models;
mod preflight;
mod queue;
mod reload;
mod sink;
mod status;
// Test-only fault hooks for the system tests (spec §16); inert unless
// WEIR_TEST_HOOKS=1 is also set.
mod test_hooks;
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

use config::Config;
use drain::{DrainConfig, DrainHeartbeat};
use models::WorkUnit;

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
/// held. A plain thread, not a runtime task, so it already runs during
/// preflight, before the runtime exists: a check that only started with the
/// runtime once left the stated ~1 s window unbounded through the long
/// startup phases (conf-durability-3), and recovery still runs on main's
/// thread, outside any runtime task. Not part of the 5 s WAB poll either, which skips
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

// ── Entry point ───────────────────────────────────────────────────────────────

/// The daemon. Startup is spec §6 (lock, preflight, runtime and signals,
/// metrics, recovery, the replay handed to the drain, producer listeners);
/// shutdown is the bounded §7 sequence:
///
/// - **T0** (the signal task, `Lifecycle::request_shutdown`): readiness off,
///   phase `shutting_down`, drain absorb, the flushers' shutdown flag, and the
///   lame duck (the producer socket unlinked, the TCP listener closed).
/// - **T1** (here): await both listeners' bounded connection drains, abort the
///   queue-depth task, wait up to `QUEUE_SENDERS_GRACE` for every work-queue
///   sender to go, then join the workers and the flushers (each seals, fsyncs
///   and hands off within its 1 s budget).
/// - **T2**: `request_stop(drain_shutdown_budget_secs)`, publish the deadline,
///   and wait for the drain at most `DRAIN_EXIT_GRACE` past it.
/// - **T3**: one no-I/O summary line, `shutdown_timeout(1 s)`, exit 0. A drain
///   that never returned takes the orphan exit, `process::exit(0)`.
///
/// A second SIGTERM/SIGINT forces: connection drains abort and the drain
/// deadline becomes now, but the worker and flusher joins still run. Exit
/// status is 0 for clean, forced and orphaned exits alike.
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
    // SIGHUP is ignored from here until startup step 4 installs the reload
    // handler (reload::spawn_sighup_task, right after the runtime is built and
    // before recovery). With the default disposition a HUP (a `systemctl
    // reload`, a log-rotation script, a terminal hang-up) killed the daemon
    // outright, skipping the seal and the drain, and before this only a TLS
    // build with tcp_bind ever installed a handler. Set before Config::load so
    // a HUP during the config load, the lock or preflight is dropped: safe,
    // because the real TLS load happens later and reads the current files
    // (spec §3.2). From step 4 on, a HUP is served as a reload.
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
    // A test-hooks build says so loudly, armed or not: it must never be the
    // binary a production host runs. A no-op without the feature.
    test_hooks::warn_if_compiled_in();

    // ── wab_dir ownership ─────────────────────────────────────────────────────
    //
    // Before ANYTHING reads the WAB (recovery in wab::recover, the replay, the
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
    // writer. The thread starts now, so the check covers preflight and
    // recovery as well as serving: a deletion during those phases is caught
    // within a tick, and proven loss exits 3 before anything is served.
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

    // ── Step 3: in-memory setup (Foundations §6) ──────────────────────────────
    //
    // Metrics, the status board, the ownership flag and its integrity thread
    // already exist (above preflight). This step adds the queue, the drain
    // channel, the drain's control and the Lifecycle.

    // One queue partition per worker. Pushes are routed by shard_id so every
    // record for a given shard lands on a single worker's partition — see
    // worker::spawn_workers for the per-shard FIFO invariant.
    let (queue_tx, queue_rx) = queue::new::<WorkUnit>(config.worker_count);
    // Held in an Option so every path names where main's sender is dropped
    // (§5.7): `serve` takes it, or the pre-serving path drops it.
    let mut queue_tx = Some(queue_tx);

    // ONE Receiver (the drain), Senders only in the flushers (§4). There is no
    // startup replay sender any more: the replay list goes to the drain in
    // memory (step 10).
    let (drain_tx, drain_rx) =
        crossbeam_channel::bounded::<std::path::PathBuf>(wab::DRAIN_CHANNEL_CAPACITY);
    metrics
        .drain_channel_capacity
        .set(wab::DRAIN_CHANNEL_CAPACITY as f64);

    // ATTACHED (Foundations §5.4): the drain's lifetime is this control, not the
    // channel. A closed channel (every flusher gone, e.g. shard_count=1 and the
    // only flusher offline) no longer ends delivery; only request_stop /
    // force_stop do. Every path past the drain spawn therefore reaches
    // request_stop below, or exits the process.
    let drain_ctl = drain::control::DrainControl::attached();
    // The flushers' shutdown flag (spec §5.5); the Lifecycle sets it at T0.
    let flushers_shutting_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lifecycle = Arc::new(lifecycle::Lifecycle::new(
        Arc::clone(&board),
        Arc::clone(&metrics),
        drain_ctl.clone(),
        Arc::clone(&flushers_shutting_down),
    ));
    // The status-board block above already entered `initializing`.
    let started = std::time::Instant::now();

    // ── Step 4: runtime, signals, reload ──────────────────────────────────────
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // Held for the rest of main: the sync binds (bind_hardened's
    // UnixListener::from_std) and every spawn below need a reactor. Recovery,
    // the scans and the joins run on this thread OUTSIDE any block_on, while
    // the runtime's workers serve HTTP and the signal handlers.
    let rt_guard = rt.enter();

    // The one reload path (spec §3.2). The SIGHUP handler replaces the SIG_IGN
    // set at the top of main; from here, which is now before recovery, a HUP
    // is a counted reload request instead of a dropped signal. The TLS step
    // below registers its config once it has loaded it.
    let reloader = reload::Reloader::new(Arc::clone(&metrics), tcp_configured);
    let reload_tx = reload::spawn_reload_loop(Arc::clone(&reloader));
    // [PR 6 slot] `reloader.attach_board(..)` and a `reload_tx.clone()` for the
    // admin `reload` verb, which reuses this queue.
    #[cfg(unix)]
    reload::spawn_sighup_task(reload_tx, Arc::clone(&metrics));
    #[cfg(not(unix))]
    drop(reload_tx);
    // SIGTERM/SIGINT are handled from here on (§6 step 4), every delivery, not
    // only the first. A signal before this line takes the default disposition:
    // nothing is in flight yet. The lock-integrity thread has run since right
    // after the lock; nothing to start for it here.
    lifecycle.spawn_signal_task()?;

    // ── Step 5: metrics/health listener ───────────────────────────────────────
    //
    // Before recovery, so `/metrics` (and the startup phase in it) answers
    // during a long recovery. Default metrics_bind is 127.0.0.1 — see
    // Config::metrics_bind for the security reasoning around exposing this
    // surface on the network. Preflight test-bound it, so a failure here is a
    // race (spec §3.0).
    let metrics_listener = rt
        .block_on(tokio::net::TcpListener::bind((
            config.metrics_bind,
            config.metrics_port,
        )))
        .map_err(|e| preflight::metrics_bind_error(&config, &e))?;
    // The router (spec §5.8): a 1 s head phase, then separate scrape and
    // probe pools. With no status source every path is still metrics.
    let http_state = metrics::server::HttpState::new(
        Arc::clone(&registry),
        Some(Arc::clone(&metrics)),
        None, // Task 5.10 wires the status board; until then every path is metrics
        config.livez_checks.clone(),
        config.metrics_max_connections,
    );
    metrics::server::spawn(metrics_listener, http_state);
    info!(
        bind = %config.metrics_bind,
        port = config.metrics_port,
        max_connections = config.metrics_max_connections,
        "metrics endpoint listening"
    );

    // ── Step 6: TLS material, before a long recovery ──────────────────────────
    //
    // Config validation guarantees the three tls_* paths are present whenever
    // tcp_bind is set; preflight has already loaded this material once, so a
    // failure here is a race (spec §3.0).
    #[cfg(all(unix, feature = "tls"))]
    let tls = match config.tcp_bind {
        Some(_) => {
            let (cert, key, ca) = preflight::tls_paths(&config)?;
            let tls = socket::tls::ReloadableServerConfig::load(cert, key, ca)
                .map_err(|e| preflight::tls_load_error(&e))?;
            // Register for reload before `tls` moves into tcp::run. The clone
            // shares the same inner ArcSwap, so a reload from the reload loop
            // is immediately visible to the accept loop's `tls.current()`.
            reloader.register_tls(tls.clone());
            Some(tls)
        }
        None => None,
    };

    // ── Step 7: [PR 6 slot] admin socket ──────────────────────────────────────
    // admin path → refuse_if_live → bind_hardened_with → admin::spawn(..,
    // lifecycle.subscribe()). `drain.pause` is accepted from here on.

    // ── Step 8: recovery, on this thread, while HTTP is served ────────────────
    //
    // Every shard dir made private, then each unsealed segment a crash left
    // sealed. A signal here sets the level, but recovery is never interrupted:
    // main finishes the step and skips the producer side below.
    lifecycle.publish_phase(weir_status::Phase::Recovering);
    if let Some(d) = test_hooks::startup_delay() {
        warn!(
            delay_ms = d.as_millis() as u64,
            "test hook: delaying the recovering phase"
        );
        std::thread::sleep(d);
    }
    let recovery = wab::recover(&config.wab_dir, config.shard_count, &metrics)?;
    info!(
        shard_dirs = recovery.shard_dirs,
        quarantined = recovery.segments_quarantined,
        failed = recovery.segments_failed,
        elapsed_ms = recovery.elapsed.as_millis() as u64,
        "WAB recovery complete"
    );

    // ── Step 9: hand-off: the replay list in memory, then the first cap scan ──
    lifecycle.publish_phase(weir_status::Phase::Handoff);
    // Orphan deletion (spec §4.3, D7) is safe only while this daemon owns the
    // wab_dir: with a second daemon on the same directory, a sealed segment
    // beside a valid sidecar may be that daemon's confirm in progress. The
    // ownership comes from the wab_dir lock: under `wab_dir_lock =
    // "best-effort"` on a filesystem that cannot lock, nothing proves we are
    // alone, so orphans are kept exactly as before.
    let orphan_cleanup = lock_outcome == wab::lock::LockOutcome::Held;
    // The sealed-but-unconfirmed backlog from a previous run. It becomes the
    // drain's `backlog`, which alternates with live segments (spec §5.4);
    // the two share no connection, so per-connection order is unchanged. No
    // sends: a backlog larger than the drain channel can no longer block
    // startup (B3), and the producer socket no longer waits on the sink.
    let replay = wab::scan_replay(
        &config.wab_dir,
        config.shard_count,
        &metrics,
        orphan_cleanup,
    )?;
    let replay_len = replay.len();
    // Spec §6 step 9: the first size scan and cap evaluation run here,
    // synchronously, before any producer listener exists (fail-closed): a
    // restart over the cap must not read wab_bytes_now = 0 and admit pushes.
    status::poll::startup_cap_gate(&config.wab_dir, config.wab_max_bytes, &board, &metrics);

    // ── Step 10: pipeline ─────────────────────────────────────────────────────

    // Coalesce hint: the shared EWMA of fsync latency. Flusher threads update
    // it after each fsync; worker threads read it once per batch to size
    // their coalesce window. Lock-free, Relaxed ordering — it is a heuristic,
    // not a correctness signal. Initial value 200 µs matches the old fixed
    // COALESCE_WINDOW constant so behaviour before the first fsync is
    // unchanged.
    let coalesce_hint = Arc::new(AtomicU64::new(200));
    // The 5 s poll task reports the EWMA as evidence; the workers take the
    // original.
    let coalesce_hint_for_poll = Arc::clone(&coalesce_hint);

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
    // One flusher thread per shard. MUST run after recovery: each flusher
    // establishes its segment counter from what recovery left on disk. The
    // flusher supervisors record each shard's state on the board's registry
    // (spec §5.3).
    let wab_handle = wab::spawn_flushers(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
        Arc::clone(&coalesce_hint),
        Arc::clone(&board.shards),
        wab::handoff::HandoffConfig {
            shutting_down: Arc::clone(&flushers_shutting_down),
            drain_depth: Arc::clone(&board.drain_depth),
            marker: Some(Arc::clone(&board) as Arc<dyn wab::handoff::HandoffMarker>),
        },
    )?;

    // Workers send `Batch`es directly to the WAB flusher via the shard channels
    // returned by `wab::spawn_flushers`: worker → Batch-channel → flusher.
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
        // The safety floor's escalation floor (3 strand cycles AND 10 minutes).
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

    // Sink selection (spec §5.12). The drain owns the drain channel's only
    // Receiver; the flushers hold the only Senders.
    let drain_spawner = sink::select::build_drain_spawner(&config, &metrics)?;
    let drain_handle = drain_spawner.spawn(sink::select::DrainSpawnArgs {
        rx: drain_rx,
        config: drain_config,
        metrics: Arc::clone(&metrics),
        heartbeat: drain_heartbeat.clone(),
        runtime: drain::DrainRuntime {
            control: drain_ctl.clone(),
            board: Some(Arc::clone(&board)),
            // The replay list, in memory: no blocking sends (§5.5).
            initial_backlog: replay,
        },
    });

    // Background: queue depth and the drain channel's high-water stamp every
    // 500 ms (spec §5.11). It holds a queue sender, so shutdown aborts it
    // before the worker joins.
    let queue_depth_task = status::poll::spawn_queue_depth_task(
        queue_tx
            .as_ref()
            .expect("queue_tx is still owned by main")
            .clone(),
        Arc::clone(&metrics),
        Arc::clone(&board),
    );

    // Background: scan WAB shard dirs for on-disk byte usage every 5 s. The
    // scan is synchronous I/O (read_dir + metadata per entry) so it runs
    // inside spawn_blocking — putting it directly on a tokio worker would
    // stall the runtime in proportion to the number of segments. It also
    // evaluates the conditions, latches them and publishes the status gauges
    // (spec §5.11), on every tick. The drain's `health_poll_interval` sets
    // the idle grace of the heartbeat-stall check: the long deliberate waits
    // inside the drain loop bump the heartbeat themselves, and a single long
    // sink call or local I/O operation is named by the busy signal.
    let mut poll = status::poll::WabPoll::new(
        Arc::clone(&metrics),
        Arc::clone(&wab_bytes_now),
        config.wab_max_bytes,
        drain_heartbeat,
        status::poll::DrainLiveness::new(
            std::time::Instant::now(),
            Duration::from_secs(config.health_poll_interval_secs),
            commit_timeout_for_stall,
        ),
    )
    .with_status(status::poll::PollStatus::new(
        Arc::clone(&board),
        config.wab_dir.clone(),
        coalesce_hint_for_poll,
    ));
    let wab_dir_bg = config.wab_dir.clone();
    rt.spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let wab_dir = wab_dir_bg.clone();
            poll.tick(std::time::Instant::now(), move || {
                tokio::task::spawn_blocking(move || {
                    status::poll::compute_wab_bytes_on_disk(&wab_dir)
                })
            })
            .await;
        }
    });

    // Routing sends connections only to settled, ingest-capable shards, so
    // the listeners wait for the flushers' counter scans. Routing also needs
    // the shard's worker running (PR 1's `ingest_capable`), so wait for every
    // worker too: a connection accepted before a worker has entered its guard
    // would otherwise be refused as no_live_shard.
    // A shutdown already requested skips the wait: nothing will be bound.
    if !lifecycle::wait_until(lifecycle::SETTLE_WAIT, Duration::from_millis(10), || {
        lifecycle.level() != lifecycle::ShutdownLevel::Running
            || (board.shards.all_settled()
                && (0..board.shards.worker_count()).all(|w| board.shards.worker_running(w)))
    }) {
        warn!(
            timeout_secs = lifecycle::SETTLE_WAIT.as_secs(),
            "not every WAB shard and worker settled before the producer listeners were \
             bound; unsettled shards receive no connections until they come up"
        );
    }

    // ── Steps 11-12: bind ALL producer listeners, then serve ─────────────────
    //
    // A signal during steps 4-10 set Graceful; main finished the step it was
    // in and skips the producer side from here (spec §6 "A signal before
    // serving"): nothing is bound, served or left on disk.
    #[cfg(unix)]
    let mut unix_join: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;
    #[cfg(all(unix, feature = "tls"))]
    let mut tcp_join: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;
    #[cfg(unix)]
    if lifecycle.level() == lifecycle::ShutdownLevel::Running {
        lifecycle.publish_phase(weir_status::Phase::Binding);
        // The producer socket's config (PR 1's `board` hook included), built
        // once: `bind_all` reads it, then it moves into `serve`. Inline, so main
        // keeps exactly two cloned ownership fields (this one and the TCP one).
        let socket_config = socket::SocketConfig {
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
        // Bind EVERY producer listener, TCP first, then Unix, before serving
        // any (spec §5.7, §6 step 11): a bind failure fails startup before any
        // producer is acked. `bind` / `bind_all` mark the listeners Bound or
        // Failed (README Q6); main marks nothing here.
        let listeners = rt.block_on(socket::bind_all(&socket_config, config.tcp_bind))?;
        if lifecycle.level() == lifecycle::ShutdownLevel::Running {
            // ONE shared connection-cap semaphore for ALL listeners (Unix +
            // TCP), so the COMBINED cap is exactly max_connections.
            let conn_sem = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
            // One router for both transports: one refusal counter and one WARN
            // limiter. It sends each connection only to an ingest-capable
            // shard (spec §5.7).
            let router = socket::route::ShardRouter::live(
                Arc::clone(&board) as Arc<dyn socket::route::LiveShards>
            );
            let socket::BoundListeners { unix, tcp } = listeners;
            // Optional TCP+mTLS listener: its own accept loop feeding the SAME
            // pipeline through a cloned queue sender, on the same shutdown
            // level, so SIGTERM puts both listeners into lame duck. `tcp::run`
            // owns its handlers' shutdown watch, and is the one writer of the
            // TCP listener's Closed and Failed marks (README Q6).
            #[cfg(feature = "tls")]
            if let (Some(listener), Some(tls)) = (tcp, tls) {
                let tcp_config = socket::tcp::TcpConfig {
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
                tcp_join = Some(
                    rt.spawn(socket::tcp::run(
                        tcp_config,
                        listener,
                        tls,
                        queue_tx
                            .as_ref()
                            .expect("queue_tx is still owned by main")
                            .clone(),
                        Arc::clone(&conn_sem),
                        lifecycle.subscribe(),
                        Arc::clone(&metrics),
                        router.clone(),
                        Arc::clone(&board),
                    )),
                );
            }
            #[cfg(not(feature = "tls"))]
            drop(tcp); // always None: config validation rejects tcp_bind without tls
            unix_join = Some(rt.spawn(socket::serve(
                socket_config,
                unix,
                queue_tx.take().expect("queue_tx is served exactly once"),
                lifecycle.subscribe(),
                Arc::clone(&metrics),
                conn_sem,
                router,
            )));
            // `serve` enters Serving at its start, the phase's only writer
            // (README Q6). Log once it has; a signal that wins that race makes
            // `enter` refuse, and the line is skipped.
            if lifecycle::wait_until(Duration::from_secs(1), Duration::from_millis(1), || {
                board.life.ever_served()
            }) {
                info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    recovery_shard_dirs = recovery.shard_dirs,
                    recovery_quarantined = recovery.segments_quarantined,
                    replay_segments = replay_len,
                    "weir serving"
                );
            }
        } else {
            // A signal landed between the binds and the spawns: unlink what was
            // bound (inode-checked) and serve nothing.
            discard_listeners(listeners, &board);
        }
    }
    #[cfg(unix)]
    if queue_tx.is_some() {
        warn!("shutdown requested during startup; the producer listeners were not served");
        drop(queue_tx.take());
    }
    #[cfg(not(unix))]
    {
        // weir-server has no producer listener off Unix; it only waits for
        // shutdown.
        let _ = (&recovery, replay_len, started);
        lifecycle.publish_phase(weir_status::Phase::Serving);
    }

    // ── T0 ran in the signal task (Lifecycle::request_shutdown): readiness off,
    //    phase shutting_down, drain absorb, flushers' flag, the lame duck. ──
    let level = rt.block_on(lifecycle.wait_shutdown());
    info!(?level, "shutdown: lame duck; draining producer connections");

    // ── T1: ingest closed. The runtime stays up: /metrics, the poll tasks and
    //    (PR 6) admin `status` keep answering through the joins. Both accept
    //    loops follow the level into lame duck and their bounded connection
    //    drain; wait for both before the joins. ──
    rt.block_on(async {
        #[cfg(unix)]
        if let Some(h) = unix_join {
            log_listener_exit(h.await, "unix socket");
        }
        #[cfg(all(unix, feature = "tls"))]
        if let Some(h) = tcp_join {
            log_listener_exit(h.await, "TCP+mTLS");
        }
    });
    // Off Unix main still holds its sender here; every path ends with none.
    drop(queue_tx.take());
    // The queue-depth task holds the last queue_tx clone: it must go before
    // the worker join (abort + await, which replaces the old `drop(rt)`).
    queue_depth_task.abort();
    let _ = rt.block_on(queue_depth_task);
    // An aborted connection task can still own a sender inside its
    // spawn_blocking push for up to QUEUE_PUSH_TIMEOUT (§5.7).
    if !lifecycle::wait_until(
        lifecycle::QUEUE_SENDERS_GRACE,
        Duration::from_millis(10),
        || queue_rx.live_senders() == 0,
    ) {
        tracing::error!(
            live = queue_rx.live_senders(),
            "a work-queue sender outlived ingest; the worker join waits for it"
        );
    }
    debug_assert_eq!(queue_rx.live_senders(), 0, "a QueueSender outlived ingest");
    info!(
        "shutdown: ingest closed; joining workers and flushers (each flusher seals, \
         fsyncs and hands off)"
    );
    for h in worker_handles {
        h.join().ok();
    }
    // Bounded in every drain state: PR 3's hand_off gives up after the
    // per-flusher 1 s budget once `flushers_shutting_down` is set.
    for h in wab_handle.join_handles {
        h.join().ok();
    }

    // ── T2: the drain's budget starts now that everything ingested is sealed. ──
    let budget = Duration::from_secs(config.drain_shutdown_budget_secs);
    drain_ctl.request_stop(budget); // fetch_min: a forced deadline stays forced
    // The deadline actually in force (a forced one stays earlier), for
    // /status, lifecycle.drain_deadline_ms and the gauge.
    if let Some(deadline_ms) = drain_ctl.deadline_unix_ms() {
        board
            .life
            .set_drain_deadline(deadline_ms, config.drain_shutdown_budget_secs);
        metrics
            .drain_shutdown_deadline_seconds
            .set(deadline_ms as f64 / 1000.0);
    }
    info!(
        budget_secs = config.drain_shutdown_budget_secs,
        "shutdown: everything ingested is sealed; delivering until the budget ends"
    );
    let waited = lifecycle::wait_for_drain(
        drain_handle,
        &drain_ctl,
        lifecycle::DRAIN_EXIT_GRACE,
        lifecycle::DRAIN_JOIN_POLL,
    );

    // ── T3: exit. The summary does no I/O (no scan_unconfirmed_sealed). ──
    // Both `reason` series (budget, disconnected; README Q7).
    let not_handed_off = metrics.wab_segments_not_handed_off_total();
    // The last successful size scan (PR 1's poll task writes both).
    let on_disk = board.wab.last_scan_ok_ms().map(|as_of| lifecycle::OnDisk {
        bytes: board
            .wab_bytes_now
            .load(std::sync::atomic::Ordering::Relaxed),
        as_of_unix_ms: as_of,
    });
    match waited {
        lifecycle::DrainWait::Joined(exit) => {
            let facts = lifecycle::ShutdownFacts::from_exit(exit.as_ref(), not_handed_off, on_disk);
            if facts.left_for_replay() == 0 {
                info!("{}", lifecycle::shutdown_summary(&facts));
            } else {
                warn!("{}", lifecycle::shutdown_summary(&facts));
            }
            drop(rt_guard);
            rt.shutdown_timeout(Duration::from_secs(1));
            // [PR 6 slot] unlink the admin socket here (remove_own_socket on its
            // BoundSocket: inode-checked).
            // The wab_dir lock (wab::lock's process-lifetime static) is released
            // by the kernel at exit, after everything above.
            info!("weir shut down cleanly");
            Ok(())
        }
        lifecycle::DrainWait::Orphaned => {
            let in_flight = board
                .drain
                .in_flight
                .load_full()
                .map(|f| (f.segment.clone(), f.processed));
            let facts = lifecycle::ShutdownFacts::orphaned(in_flight, not_handed_off, on_disk);
            tracing::error!(
                "drain did not stop within budget; exiting with its in-flight segment \
                 unconfirmed. {}",
                lifecycle::shutdown_summary(&facts)
            );
            // Without dropping the runtime or the lock: the kernel releases the
            // flock only once the whole process, orphaned drain thread included,
            // is gone. Exit status 0 (owner decision).
            std::process::exit(0);
        }
    }
}

/// The pre-serving shutdown path (spec §6 "A signal before serving"): unlink
/// the Unix socket we bound (inode-checked), close every listener, and serve
/// nothing. The one place main marks listener state: `serve` / `tcp::run`,
/// which own the `Closed` marks otherwise, never run on this path.
#[cfg(unix)]
fn discard_listeners(listeners: socket::BoundListeners, board: &status::StatusBoard) {
    socket::remove_own_socket(&listeners.unix.path, &listeners.unix.socket);
    drop(listeners.unix);
    board.listeners.unix.set(weir_status::ListenerState::Closed);
    if let Some(l) = listeners.tcp {
        drop(l);
        board.listeners.tcp.set(weir_status::ListenerState::Closed);
    }
}

/// Logs a listener task's exit if it was not a clean `Ok`.
#[cfg(unix)]
fn log_listener_exit(r: Result<std::io::Result<()>, tokio::task::JoinError>, which: &str) {
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "{which} listener exited with an error"),
        Err(e) => tracing::error!(error = %e, "{which} listener task failed"),
    }
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
