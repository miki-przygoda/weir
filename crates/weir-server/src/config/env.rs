use std::path::PathBuf;

use super::{ConfigError, PartialConfig};

pub(super) fn read() -> Result<PartialConfig, ConfigError> {
    Ok(PartialConfig {
        socket_path: env_path("WEIR_SOCKET_PATH")?,
        wab_dir: env_path("WEIR_WAB_DIR")?,
        shard_count: env_parse("WEIR_SHARD_COUNT")?,
        worker_count: env_parse("WEIR_WORKER_COUNT")?,
        batch_size: env_parse("WEIR_BATCH_SIZE")?,
        batch_deadline_ms: env_parse("WEIR_BATCH_DEADLINE_MS")?,
        wab_segment_max_bytes: env_parse("WEIR_WAB_SEGMENT_MAX_BYTES")?,
        max_connections: env_parse("WEIR_MAX_CONNECTIONS")?,
        max_payload_bytes: env_parse("WEIR_MAX_PAYLOAD_BYTES")?,
        metrics_port: env_parse("WEIR_METRICS_PORT")?,
        metrics_bind: env_string("WEIR_METRICS_BIND")?,
        metrics_max_connections: env_parse("WEIR_METRICS_MAX_CONNECTIONS")?,
        peer_uid_check: env_bool("WEIR_PEER_UID_CHECK")?,
        shutdown_timeout_secs: env_parse("WEIR_SHUTDOWN_TIMEOUT_SECS")?,
        connection_read_timeout_secs: env_parse("WEIR_CONNECTION_READ_TIMEOUT_SECS")?,
        sink_type: env_string("WEIR_SINK_TYPE")?,
        sink_url: env_string("WEIR_SINK_URL")?,
        sink_timeout_secs: env_parse("WEIR_SINK_TIMEOUT_SECS")?,
        sink_max_batch_size: env_parse("WEIR_SINK_MAX_BATCH_SIZE")?,
        sink_send_idempotency_key: env_bool("WEIR_SINK_SEND_IDEMPOTENCY_KEY")?,
        sink_http_concurrency: env_parse("WEIR_SINK_HTTP_CONCURRENCY")?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_table: env_string("WEIR_SINK_MYSQL_TABLE")?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_column: env_string("WEIR_SINK_MYSQL_COLUMN")?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_insert_mode: env_string("WEIR_SINK_MYSQL_INSERT_MODE")?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_table: env_string("WEIR_SINK_POSTGRES_TABLE")?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_column: env_string("WEIR_SINK_POSTGRES_COLUMN")?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_insert_mode: env_string("WEIR_SINK_POSTGRES_INSERT_MODE")?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_database: env_string("WEIR_SINK_CLICKHOUSE_DATABASE")?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_table: env_string("WEIR_SINK_CLICKHOUSE_TABLE")?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_column: env_string("WEIR_SINK_CLICKHOUSE_COLUMN")?,
        dead_letter_max_bytes: env_parse("WEIR_DEAD_LETTER_MAX_BYTES")?,
        dead_letter_check_interval_secs: env_parse("WEIR_DEAD_LETTER_CHECK_INTERVAL_SECS")?,
        log_level: env_string("WEIR_LOG_LEVEL")?,
        tcp_bind: env_string("WEIR_TCP_BIND")?,
        tls_cert_path: env_path("WEIR_TLS_CERT")?,
        tls_key_path: env_path("WEIR_TLS_KEY")?,
        tls_client_ca_path: env_path("WEIR_TLS_CLIENT_CA")?,
        tls_handshake_timeout_secs: env_parse("WEIR_TLS_HANDSHAKE_TIMEOUT_SECS")?,
    })
}

fn env_string(key: &'static str) -> Result<Option<String>, ConfigError> {
    match std::env::var(key) {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(ConfigError::ParseError {
            field: key,
            source: Box::new(e),
        }),
    }
}

fn env_path(key: &'static str) -> Result<Option<PathBuf>, ConfigError> {
    Ok(env_string(key)?.map(PathBuf::from))
}

/// Parses a boolean env var leniently (`true/false`, `1/0`, `yes/no`, `on/off`).
/// See [`super::parse_bool`] — `env_parse::<bool>` would reject everything but
/// exact `true`/`false` (F57).
fn env_bool(key: &'static str) -> Result<Option<bool>, ConfigError> {
    match env_string(key)? {
        None => Ok(None),
        Some(v) => super::parse_bool(key, &v).map(Some),
    }
}

fn env_parse<T>(key: &'static str) -> Result<Option<T>, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env_string(key)? {
        None => Ok(None),
        Some(v) => v
            .parse::<T>()
            .map(Some)
            .map_err(|e| ConfigError::ParseError {
                field: key,
                source: Box::new(e),
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Coverage gap (T15 / F57): env bools go through the lenient parse_bool
    /// (1/0, yes/no, on/off, case-insensitive), not bool::from_str; an
    /// unrecognised spelling errors; an unset var is None.
    #[test]
    fn env_bool_accepts_lenient_spellings() {
        // Unique key so this doesn't clobber real config env vars.
        let k = "WEIR_TEST_LENIENT_BOOL_COVERAGE";
        // SAFETY: serial test, key is unique and removed at the end.
        unsafe { std::env::set_var(k, "1") }
        assert_eq!(env_bool(k).unwrap(), Some(true));
        unsafe { std::env::set_var(k, "off") }
        assert_eq!(env_bool(k).unwrap(), Some(false));
        unsafe { std::env::set_var(k, "TRUE") }
        assert_eq!(
            env_bool(k).unwrap(),
            Some(true),
            "env bools must use lenient parse_bool, not bool::from_str"
        );
        unsafe { std::env::set_var(k, "garbage") }
        assert!(
            env_bool(k).is_err(),
            "an unrecognised bool spelling must surface a ConfigError"
        );
        unsafe { std::env::remove_var(k) }
        assert_eq!(env_bool(k).unwrap(), None, "an unset var is None");
    }

    /// Coverage gap (T15): a non-numeric integer env var surfaces a
    /// ConfigError::ParseError naming the field, not a panic or silent default.
    #[test]
    fn env_parse_wraps_parse_errors_with_field_name() {
        let k = "WEIR_TEST_PARSE_INT_COVERAGE";
        unsafe { std::env::set_var(k, "not-a-number") }
        let err = env_parse::<usize>(k).unwrap_err();
        match err {
            ConfigError::ParseError { field, .. } => assert_eq!(field, k),
            other => panic!("expected ParseError, got {other:?}"),
        }
        unsafe { std::env::remove_var(k) }
    }
}
