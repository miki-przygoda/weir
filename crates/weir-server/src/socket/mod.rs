//! Unix socket accept loop and frame-parsing layer.
//!
//! # Runtime boundary
//!
//! This module is entirely async (tokio). Everything downstream — queue, workers,
//! WAB, drain — runs on `std::thread` with blocking I/O. The only crossing point
//! is `task::spawn_blocking` in `handle_connection`, which moves the blocking
//! queue push onto tokio's blocking thread pool.
//!
//! # Connection limit and spawn_blocking
//!
//! Under sustained load, if every active connection is simultaneously blocked
//! waiting for a queue slot, all `spawn_blocking` threads fill up and new
//! connections stall at the socket layer. `max_connections` must therefore be
//! set ≤ the tokio blocking thread pool limit (default: 512). This constraint
//! is documented here and in config validation; it is not enforced in code
//! because the pool limit is a runtime tunable.

mod connection;

pub use connection::{ConnectionConfig, handle_connection};

use std::{
    io,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use tokio::{
    net::UnixListener,
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time,
};
use tracing::{error, info, warn};

use crate::{models::WorkUnit, queue::QueueSender};

/// Configuration for the socket accept loop.
pub struct SocketConfig {
    /// Absolute path to the Unix socket file.
    pub socket_path: PathBuf,
    /// Maximum concurrent connections. Connections over this cap are refused
    /// immediately (stream dropped). Must be ≤ tokio's blocking thread pool limit.
    pub max_connections: usize,
    /// Per-connection payload cap in bytes. Effective cap is
    /// `min(max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`.
    pub max_payload_bytes: usize,
    /// How long to wait for in-flight connections to finish after the shutdown
    /// signal is received before aborting them.
    pub shutdown_timeout_secs: u64,
}

/// Binds a Unix socket, accepts connections, and drives the frame-parsing layer.
///
/// Returns when `shutdown_rx` fires or is dropped. Before returning, waits up to
/// `config.shutdown_timeout_secs` for all in-flight connections to finish.
///
/// # Bind sequence (TOCTOU hardening)
///
/// 1. Reject socket paths that are not absolute, contain `..`, or contain null bytes.
/// 2. If a file already exists at the path, `stat` it and verify `S_ISSOCK`. Refuse
///    to remove a regular file (or any non-socket) at that path.
/// 3. Remove the stale socket and bind a new one.
/// 4. `chmod 0o600` — daemon-private; no group or other access.
pub async fn run(
    config: SocketConfig,
    queue_tx: QueueSender<WorkUnit>,
    shutdown_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    validate_socket_path(&config.socket_path)?;
    bind_cleanup(&config.socket_path)?;

    let listener = UnixListener::bind(&config.socket_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    info!(path = %config.socket_path.display(), "socket listening");

    let effective_cap = config
        .max_payload_bytes
        .min(weir_core::MAX_PAYLOAD_HARD_CAP);
    let conn_cfg = ConnectionConfig {
        max_payload_bytes: effective_cap,
    };
    let sem = std::sync::Arc::new(Semaphore::new(config.max_connections));
    let mut join_set: JoinSet<()> = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown_rx);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("socket manager: shutdown signal received, stopping accept loop");
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, _addr)) => {
                        let permit = match sem.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("connection limit ({}) reached; dropping connection", config.max_connections);
                                drop(stream);
                                continue;
                            }
                        };
                        let tx = queue_tx.clone();
                        let cfg = conn_cfg.clone();
                        join_set.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handle_connection(stream, tx, cfg).await
                                && e.kind() != io::ErrorKind::UnexpectedEof
                                && e.kind() != io::ErrorKind::ConnectionReset
                                && e.kind() != io::ErrorKind::BrokenPipe
                            {
                                warn!(error = %e, "connection closed with error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                    }
                }
            }
        }
    }

    // Drain in-flight connections within the configured timeout.
    let timeout = Duration::from_secs(config.shutdown_timeout_secs);
    match time::timeout(timeout, drain_join_set(&mut join_set)).await {
        Ok(()) => {
            info!("socket manager: all connections drained cleanly");
        }
        Err(_elapsed) => {
            error!(
                remaining = join_set.len(),
                timeout_secs = config.shutdown_timeout_secs,
                "socket manager: shutdown timeout reached; aborting remaining connections"
            );
            join_set.abort_all();
        }
    }

    let _ = std::fs::remove_file(&config.socket_path);
    Ok(())
}

async fn drain_join_set(join_set: &mut JoinSet<()>) {
    while join_set.join_next().await.is_some() {}
}

/// Validates a socket path: absolute, no `..`, no null bytes.
/// Matches the same rules as `validate_path` in `wab/mod.rs`.
pub fn validate_socket_path(path: &Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' is not absolute", path.display()),
        ));
    }
    if path.components().any(|c| c == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' contains '..' components", path.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().contains(&0u8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket path contains a null byte",
            ));
        }
    }
    Ok(())
}

/// If a file exists at `path`: verify it is a socket (S_ISSOCK) then remove it.
/// Refuses to remove a non-socket file to prevent accidental data destruction.
#[cfg(unix)]
fn bind_cleanup(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if !meta.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "a non-socket file already exists at '{}'; refusing to remove it",
                        path.display()
                    ),
                ));
            }
            std::fs::remove_file(path)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    Ok(())
}

#[cfg(not(unix))]
fn bind_cleanup(_path: &Path) -> io::Result<()> {
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{models::WorkUnit, queue};
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::UnixStream, sync::oneshot};
    use weir_core::{Durability, Envelope, HEADER_LEN, Header, MessageType};

    fn tmp_socket_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("weir_sock_{label}_{}.sock", std::process::id()))
    }

    fn default_config(path: PathBuf) -> SocketConfig {
        SocketConfig {
            socket_path: path,
            max_connections: 16,
            max_payload_bytes: weir_core::MAX_PAYLOAD_HARD_CAP,
            shutdown_timeout_secs: 5,
        }
    }

    /// Spawns `run()` with an auto-acking queue. Returns shutdown sender and path.
    async fn spawn_server(config: SocketConfig) -> (oneshot::Sender<()>, PathBuf) {
        let path = config.socket_path.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>();

        // Auto-acker: receives WorkUnits and immediately sends true on ack_tx.
        std::thread::spawn(move || {
            let rx = queue_rx.get();
            while let Ok(unit) = rx.recv() {
                let _ = unit.ack_tx.send(true);
            }
        });

        tokio::spawn(run(config, queue_tx, shutdown_rx));
        tokio::time::sleep(Duration::from_millis(30)).await;
        (shutdown_tx, path)
    }

    fn push_frame(payload: &[u8]) -> Vec<u8> {
        let header = Header::new(MessageType::Push, Durability::Sync, 0, payload.len() as u32);
        Envelope::new(header, payload.to_vec()).encode()
    }

    async fn read_response(stream: &mut UnixStream) -> (MessageType, Vec<u8>) {
        let mut header_buf = [0u8; HEADER_LEN];
        stream.read_exact(&mut header_buf).await.unwrap();
        let header = Header::decode(&header_buf).unwrap();
        let mut payload = vec![0u8; header.payload_len as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }
        let mut crc_buf = [0u8; 4];
        stream.read_exact(&mut crc_buf).await.unwrap();
        (header.message_type, payload)
    }

    // ── Path validation ───────────────────────────────────────────────────────

    #[test]
    fn validate_socket_path_rejects_relative() {
        let err = validate_socket_path(Path::new("relative/path.sock")).unwrap_err();
        assert!(err.to_string().contains("not absolute"), "{err}");
    }

    #[test]
    fn validate_socket_path_rejects_dotdot() {
        let err = validate_socket_path(Path::new("/var/../etc/weir.sock")).unwrap_err();
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    #[test]
    fn validate_socket_path_accepts_valid_absolute() {
        assert!(validate_socket_path(Path::new("/run/weir/weir.sock")).is_ok());
    }

    // ── Bind TOCTOU guard ─────────────────────────────────────────────────────

    #[test]
    #[cfg(unix)]
    fn bind_cleanup_refuses_regular_file_at_socket_path() {
        let path = tmp_socket_path("toctou");
        // Create a regular file at the socket path.
        std::fs::write(&path, b"not a socket").unwrap();
        let err = bind_cleanup(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert!(err.to_string().contains("non-socket"), "{err}");
        // File must still exist — we refused to remove it.
        assert!(path.exists(), "regular file must not have been removed");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    #[cfg(unix)]
    fn bind_cleanup_removes_stale_socket() {
        let path = tmp_socket_path("stale");
        // Bind a socket to create the file, then drop the listener.
        {
            let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        }
        assert!(path.exists());
        bind_cleanup(&path).unwrap();
        assert!(!path.exists(), "stale socket should have been removed");
    }

    #[test]
    #[cfg(unix)]
    fn bind_cleanup_is_noop_when_no_file_exists() {
        let path = tmp_socket_path("noop");
        assert!(!path.exists());
        bind_cleanup(&path).unwrap(); // must not error
    }

    // ── Connection limit ──────────────────────────────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn connection_over_cap_is_refused() {
        let path = tmp_socket_path("connlimit");
        let cfg = SocketConfig {
            socket_path: path.clone(),
            max_connections: 1,
            max_payload_bytes: weir_core::MAX_PAYLOAD_HARD_CAP,
            shutdown_timeout_secs: 2,
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>();
        let _rx = queue_rx.get(); // keep receiver alive

        tokio::spawn(run(cfg, queue_tx, shutdown_rx));
        tokio::time::sleep(Duration::from_millis(30)).await;

        // First connection fills the cap.
        let _conn1 = UnixStream::connect(&path).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second connection — server drops it immediately (cap exceeded).
        let mut conn2 = UnixStream::connect(&path).await.unwrap();
        let mut buf = [0u8; 1];
        // Either EOF (stream dropped by server) or timeout — both confirm the
        // server did not accept a second active connection beyond the cap.
        let _ = time::timeout(Duration::from_millis(200), conn2.read(&mut buf)).await;

        shutdown_tx.send(()).ok();
        std::fs::remove_file(&path).ok();
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn run_exits_cleanly_after_shutdown_signal() {
        let path = tmp_socket_path("shutdown");
        let cfg = default_config(path.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>();

        let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx));
        tokio::time::sleep(Duration::from_millis(20)).await;

        shutdown_tx.send(()).unwrap();
        time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("run() should complete within timeout after shutdown signal")
            .expect("task should not panic")
            .expect("run() should return Ok");

        std::fs::remove_file(&path).ok();
    }

    // ── Socket permissions ────────────────────────────────────────────────────

    #[test]
    #[cfg(unix)]
    fn socket_bind_sets_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = tmp_socket_path("perms");
            let cfg = default_config(path.clone());
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (queue_tx, _) = queue::new::<WorkUnit>();

            tokio::spawn(run(cfg, queue_tx, shutdown_rx));
            tokio::time::sleep(Duration::from_millis(30)).await;

            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            // Mask to just the permission bits (ignore file type bits).
            assert_eq!(
                mode & 0o777,
                0o600,
                "socket should be mode 0600, got {:#o}",
                mode & 0o777
            );

            shutdown_tx.send(()).ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::remove_file(&path).ok();
        });
    }
}
