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

use std::{path::Path, sync::Arc, time::Duration};

use tracing::info;

use config::Config;
use drain::{DrainConfig, MAX_RETRIES};
use models::WorkUnit;
use sink::{CommitResult, Sink, SinkError, SinkHealth};
use wab::WabRecord;
use weir_core::Payload;

// ── NoopSink ──────────────────────────────────────────────────────────────────

/// Placeholder sink that accepts all records. Replace with a real sink
/// implementation once downstream targets are wired up.
struct NoopSink;

#[derive(Debug)]
struct NoopError;

impl std::fmt::Display for NoopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "noop error (unreachable)")
    }
}

impl std::error::Error for NoopError {}

impl SinkError for NoopError {
    fn is_transient(&self) -> bool {
        false
    }
}

impl Sink for NoopSink {
    type Record = Payload;
    type Error = NoopError;

    async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, NoopError> {
        Ok(CommitResult {
            committed: batch,
            dead_lettered: vec![],
        })
    }

    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

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

    let config = Config::load()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&config.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        socket = %config.socket_path.display(),
        wab_dir = %config.wab_dir.display(),
        shards = config.shard_count,
        workers = config.worker_count,
        "weir starting"
    );

    // ── Metrics ───────────────────────────────────────────────────────────────

    let (metrics_struct, registry) = metrics::Metrics::new();
    let metrics = Arc::new(metrics_struct);
    let registry = Arc::new(registry);

    // ── Queue ─────────────────────────────────────────────────────────────────

    let (queue_tx, queue_rx) = queue::new::<WorkUnit>();

    // ── Drain channel ─────────────────────────────────────────────────────────

    let (drain_tx, drain_rx) = crossbeam_channel::bounded::<std::path::PathBuf>(256);

    // ── WAB (one flusher thread per shard) ────────────────────────────────────

    let wab_config = wab::WabConfig {
        shard_count: config.shard_count,
        batch_size: config.batch_size,
        batch_deadline: Duration::from_millis(config.batch_deadline_ms),
    };
    let wab_handle = wab::spawn(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
    )?;

    // ── Workers (queue → per-shard Batch channels) ────────────────────────────

    let (shard_batch_rxs, worker_handles) = worker::spawn_workers(
        &queue_rx,
        config.shard_count,
        config.worker_count,
        config.batch_size,
        Duration::from_millis(config.batch_deadline_ms),
    );

    // ── Bridge threads (Batch → WabRecord per shard) ──────────────────────────
    //
    // Each bridge thread converts WorkUnit fields directly — both sides share
    // `tokio::sync::oneshot::Sender<bool>` for the ack channel.

    let mut bridge_handles = Vec::with_capacity(config.shard_count);
    for (batch_rx, wab_tx) in shard_batch_rxs.into_iter().zip(wab_handle.shard_txs) {
        let handle = std::thread::Builder::new()
            .name("weir-bridge".into())
            .spawn(move || {
                while let Ok(batch) = batch_rx.recv() {
                    for unit in batch.records {
                        let record = WabRecord {
                            payload: unit.payload,
                            durability: unit.durability,
                            ack_tx: unit.ack_tx,
                        };
                        if wab_tx.send(record).is_err() {
                            return;
                        }
                    }
                }
            })
            .expect("failed to spawn bridge thread");
        bridge_handles.push(handle);
    }

    // ── Drain ─────────────────────────────────────────────────────────────────

    let drain_config = DrainConfig {
        wab_dir: config.wab_dir.clone(),
        dead_letter_max_bytes: config.dead_letter_max_bytes,
        dead_letter_check_interval: Duration::from_secs(config.dead_letter_check_interval_secs),
        base_retry_delay: Duration::from_millis(100),
        max_retries: MAX_RETRIES,
    };
    let drain_handle = drain::spawn(
        drain_rx,
        Arc::new(NoopSink),
        drain_config,
        Arc::clone(&metrics),
    );

    // ── Tokio runtime: socket accept loop + metrics server ────────────────────

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        // Bind metrics listener before starting the socket loop.
        let metrics_listener =
            tokio::net::TcpListener::bind(("0.0.0.0", config.metrics_port)).await?;
        metrics::server::spawn(metrics_listener, Arc::clone(&registry));

        info!(port = config.metrics_port, "metrics endpoint listening");

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
        let wab_dir_bg = config.wab_dir.clone();
        let metrics_w = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let bytes = compute_wab_bytes_on_disk(&wab_dir_bg);
                metrics_w.wab_bytes_on_disk.set(bytes as f64);
            }
        });

        // Shutdown coordination: signal handler → shutdown_tx → socket::run exits.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

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
            };
            socket::run(socket_config, queue_tx, shutdown_rx, Arc::clone(&metrics)).await?;
        }
        #[cfg(not(unix))]
        {
            // On non-Unix builds weir-server is not supported; just wait for shutdown.
            let _ = shutdown_rx.await;
        }

        Ok::<(), std::io::Error>(())
    })?;

    // ── Graceful pipeline drain ───────────────────────────────────────────────
    //
    // queue_tx moved into socket::run and dropped when it returns.
    // Workers observe Disconnected → flush remaining batches → exit.
    // Bridge threads observe shard_rx Disconnected → exit → drop wab_tx.
    // WAB flushers observe wab_rx Disconnected → seal segments → exit → drop drain_tx.
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
    for h in bridge_handles {
        h.join().ok();
    }
    for h in wab_handle.join_handles {
        h.join().ok();
    }
    drain_handle.join().ok();

    info!("weir shut down cleanly");
    Ok(())
}
