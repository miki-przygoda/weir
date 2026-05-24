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
        max_connections: env_parse("WEIR_MAX_CONNECTIONS")?,
        max_payload_bytes: env_parse("WEIR_MAX_PAYLOAD_BYTES")?,
        metrics_port: env_parse("WEIR_METRICS_PORT")?,
        shutdown_timeout_secs: env_parse("WEIR_SHUTDOWN_TIMEOUT_SECS")?,
        dead_letter_max_bytes: env_parse("WEIR_DEAD_LETTER_MAX_BYTES")?,
        dead_letter_check_interval_secs: env_parse("WEIR_DEAD_LETTER_CHECK_INTERVAL_SECS")?,
        log_level: env_string("WEIR_LOG_LEVEL")?,
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
