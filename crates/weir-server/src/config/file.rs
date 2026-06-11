use std::path::{Path, PathBuf};

use serde::Deserialize;
use tracing::warn;

use super::{ConfigError, PartialConfig};

#[derive(Deserialize)]
struct RawConfig {
    #[serde(default)]
    server: Option<RawServer>,
}

#[derive(Deserialize, Default)]
struct RawServer {
    socket_path: Option<String>,
    wab_dir: Option<String>,
    shard_count: Option<usize>,
    worker_count: Option<usize>,
    batch_size: Option<usize>,
    batch_deadline_ms: Option<u64>,
    wab_segment_max_bytes: Option<u64>,
    max_connections: Option<usize>,
    max_payload_bytes: Option<usize>,
    metrics_port: Option<u16>,
    metrics_bind: Option<String>,
    metrics_max_connections: Option<usize>,
    peer_uid_check: Option<bool>,
    shutdown_timeout_secs: Option<u64>,
    connection_read_timeout_secs: Option<u64>,
    sink_type: Option<String>,
    sink_url: Option<String>,
    sink_timeout_secs: Option<u64>,
    sink_max_batch_size: Option<usize>,
    sink_send_idempotency_key: Option<bool>,
    #[cfg(feature = "mysql-sink")]
    sink_mysql_table: Option<String>,
    #[cfg(feature = "mysql-sink")]
    sink_mysql_column: Option<String>,
    #[cfg(feature = "mysql-sink")]
    sink_mysql_insert_mode: Option<String>,
    #[cfg(feature = "postgres-sink")]
    sink_postgres_table: Option<String>,
    #[cfg(feature = "postgres-sink")]
    sink_postgres_column: Option<String>,
    #[cfg(feature = "postgres-sink")]
    sink_postgres_insert_mode: Option<String>,
    dead_letter_max_bytes: Option<u64>,
    dead_letter_check_interval_secs: Option<u64>,
    log_level: Option<String>,
    #[serde(default)]
    tcp_bind: Option<String>,
    #[serde(default)]
    tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    tls_key_path: Option<PathBuf>,
    #[serde(default)]
    tls_client_ca_path: Option<PathBuf>,
    #[serde(default)]
    tls_handshake_timeout_secs: Option<u64>,
}

const KNOWN_SERVER_KEYS: &[&str] = &[
    "socket_path",
    "wab_dir",
    "shard_count",
    "worker_count",
    "batch_size",
    "batch_deadline_ms",
    "wab_segment_max_bytes",
    "max_connections",
    "max_payload_bytes",
    "metrics_port",
    "metrics_bind",
    "metrics_max_connections",
    "peer_uid_check",
    "shutdown_timeout_secs",
    "connection_read_timeout_secs",
    "sink_type",
    "sink_url",
    "sink_timeout_secs",
    "sink_max_batch_size",
    "sink_send_idempotency_key",
    "sink_mysql_table",
    "sink_mysql_column",
    "sink_mysql_insert_mode",
    "sink_postgres_table",
    "sink_postgres_column",
    "sink_postgres_insert_mode",
    "dead_letter_max_bytes",
    "dead_letter_check_interval_secs",
    "log_level",
    "tcp_bind",
    "tls_cert_path",
    "tls_key_path",
    "tls_client_ca_path",
    "tls_handshake_timeout_secs",
];

pub(super) fn read(path: &Path) -> Result<PartialConfig, ConfigError> {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PartialConfig::empty());
        }
        Err(e) => return Err(ConfigError::IoError { source: e }),
    };

    let value: toml::Value = toml::from_str(&contents).map_err(|e| ConfigError::ParseError {
        field: "config file",
        source: Box::new(e),
    })?;

    warn_unknown_keys(&value);

    let raw: RawConfig =
        value
            .try_into()
            .map_err(|e: toml::de::Error| ConfigError::ParseError {
                field: "config file",
                source: Box::new(e),
            })?;

    let s = raw.server.unwrap_or_default();
    Ok(PartialConfig {
        socket_path: s.socket_path.map(PathBuf::from),
        wab_dir: s.wab_dir.map(PathBuf::from),
        shard_count: s.shard_count,
        worker_count: s.worker_count,
        batch_size: s.batch_size,
        batch_deadline_ms: s.batch_deadline_ms,
        wab_segment_max_bytes: s.wab_segment_max_bytes,
        max_connections: s.max_connections,
        max_payload_bytes: s.max_payload_bytes,
        metrics_port: s.metrics_port,
        metrics_bind: s.metrics_bind,
        metrics_max_connections: s.metrics_max_connections,
        peer_uid_check: s.peer_uid_check,
        shutdown_timeout_secs: s.shutdown_timeout_secs,
        connection_read_timeout_secs: s.connection_read_timeout_secs,
        sink_type: s.sink_type,
        sink_url: s.sink_url,
        sink_timeout_secs: s.sink_timeout_secs,
        sink_max_batch_size: s.sink_max_batch_size,
        sink_send_idempotency_key: s.sink_send_idempotency_key,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_table: s.sink_mysql_table,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_column: s.sink_mysql_column,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_insert_mode: s.sink_mysql_insert_mode,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_table: s.sink_postgres_table,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_column: s.sink_postgres_column,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_insert_mode: s.sink_postgres_insert_mode,
        dead_letter_max_bytes: s.dead_letter_max_bytes,
        dead_letter_check_interval_secs: s.dead_letter_check_interval_secs,
        log_level: s.log_level,
        tcp_bind: s.tcp_bind,
        tls_cert_path: s.tls_cert_path,
        tls_key_path: s.tls_key_path,
        tls_client_ca_path: s.tls_client_ca_path,
        tls_handshake_timeout_secs: s.tls_handshake_timeout_secs,
    })
}

fn warn_unknown_keys(value: &toml::Value) {
    let Some(top) = value.as_table() else { return };
    for key in top.keys() {
        if key != "server" {
            warn!(key = %key, "unknown top-level config key; ignoring");
        }
    }
    if let Some(server_val) = top.get("server")
        && let Some(server_table) = server_val.as_table()
    {
        for key in server_table.keys() {
            if !KNOWN_SERVER_KEYS.contains(&key.as_str()) {
                warn!(key = %key, "unknown [server] config key; ignoring");
            }
        }
    }
}
