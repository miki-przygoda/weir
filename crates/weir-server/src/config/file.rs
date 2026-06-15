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
    sink_http_concurrency: Option<usize>,
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
    #[cfg(feature = "clickhouse-sink")]
    sink_clickhouse_database: Option<String>,
    #[cfg(feature = "clickhouse-sink")]
    sink_clickhouse_table: Option<String>,
    #[cfg(feature = "clickhouse-sink")]
    sink_clickhouse_column: Option<String>,
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

/// `[server]` keys present in `RawServer` on every build, regardless of which
/// sink features are compiled in.
const BASE_SERVER_KEYS: &[&str] = &[
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
    "sink_http_concurrency",
    "dead_letter_max_bytes",
    "dead_letter_check_interval_secs",
    "log_level",
    "tcp_bind",
    "tls_cert_path",
    "tls_key_path",
    "tls_client_ca_path",
    "tls_handshake_timeout_secs",
];

/// `[server]` keys that exist in `RawServer` only when their sink feature is
/// compiled in, mapped to that feature. On a build WITHOUT the feature, serde
/// silently drops the key (RawServer has no field for it). Previously such a
/// key was also in the flat `KNOWN_SERVER_KEYS` list, so `warn_unknown_keys`
/// stayed silent — the operator got no signal that their setting was ignored
/// (F55). We now warn that the key needs a feature.
const FEATURE_GATED_SERVER_KEYS: &[(&str, &str)] = &[
    ("sink_mysql_table", "mysql-sink"),
    ("sink_mysql_column", "mysql-sink"),
    ("sink_mysql_insert_mode", "mysql-sink"),
    ("sink_postgres_table", "postgres-sink"),
    ("sink_postgres_column", "postgres-sink"),
    ("sink_postgres_insert_mode", "postgres-sink"),
    ("sink_clickhouse_database", "clickhouse-sink"),
    ("sink_clickhouse_table", "clickhouse-sink"),
    ("sink_clickhouse_column", "clickhouse-sink"),
];

/// Whether the named sink feature is compiled into this binary.
// The arms map each feature name to its own `cfg!`, which differ per build
// (e.g. on the default build clickhouse-sink is false). clippy only sees the
// match-like-matches shape on an all-features build where every `cfg!` happens
// to be `true`; collapsing to `matches!` there would silently change behaviour
// on every other feature set, so the lint is suppressed rather than applied.
#[allow(clippy::match_like_matches_macro)]
fn feature_compiled(feature: &str) -> bool {
    match feature {
        "mysql-sink" => cfg!(feature = "mysql-sink"),
        "postgres-sink" => cfg!(feature = "postgres-sink"),
        "clickhouse-sink" => cfg!(feature = "clickhouse-sink"),
        _ => false,
    }
}

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
        sink_http_concurrency: s.sink_http_concurrency,
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
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_database: s.sink_clickhouse_database,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_table: s.sink_clickhouse_table,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_column: s.sink_clickhouse_column,
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
            let k = key.as_str();
            if BASE_SERVER_KEYS.contains(&k) {
                continue;
            }
            if let Some(entry) = FEATURE_GATED_SERVER_KEYS
                .iter()
                .find(|(name, _)| *name == k)
            {
                let feature = entry.1;
                // Feature compiled in ⇒ RawServer has the field and serde applied
                // it; nothing to warn about. Feature absent ⇒ the value was
                // silently dropped, so tell the operator which feature it needs.
                if !feature_compiled(feature) {
                    warn!(
                        key = %k,
                        feature,
                        "[server] config key requires a Cargo feature this binary was \
                         built without; ignoring"
                    );
                }
                continue;
            }
            warn!(key = %k, "unknown [server] config key; ignoring");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F55 drift guard: the base and feature-gated key sets must stay disjoint
    /// (a key in both would make the gated branch unreachable), and every gated
    /// key must name a feature `feature_compiled` actually knows about (a typo'd
    /// feature name would silently warn on every build).
    #[test]
    fn base_and_feature_gated_key_sets_are_disjoint_and_well_formed() {
        for (key, feature) in FEATURE_GATED_SERVER_KEYS {
            assert!(
                !BASE_SERVER_KEYS.contains(key),
                "{key} is in both BASE and FEATURE_GATED"
            );
            assert!(
                matches!(*feature, "mysql-sink" | "postgres-sink" | "clickhouse-sink"),
                "{key} names an unknown feature {feature}"
            );
        }
    }

    /// A `[server]` table containing a feature-gated key and a wholly-unknown key
    /// is tolerated (warn-only) — `read` must not error on either. This is the
    /// long-standing behaviour; F55 only changed which keys get a warning, not
    /// whether parsing fails.
    #[test]
    fn read_tolerates_feature_gated_and_unknown_keys() {
        let dir = std::env::temp_dir().join(format!("weir_filecfg_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("weir.toml");
        std::fs::write(
            &path,
            "[server]\n\
             shard_count = 2\n\
             sink_clickhouse_table = \"events\"\n\
             totally_made_up_key = 99\n",
        )
        .unwrap();

        let partial = read(&path).expect("warn-only keys must not fail the parse");
        // The recognised base key still applies.
        assert_eq!(partial.shard_count, Some(2));
        std::fs::remove_file(&path).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}
