//! Three-layer configuration: CLI > env > file > defaults.
//!
//! Call `Config::load()` once at startup. It reads CLI flags (pico-args), env
//! vars (`WEIR_*`), and an optional TOML config file, merges them in precedence
//! order, then validates all values.

mod cli;
mod env;
mod file;

use std::{
    fmt,
    path::{Component, Path, PathBuf},
};

use tracing::warn;
use weir_core::MAX_PAYLOAD_HARD_CAP;

// ── Error type ────────────────────────────────────────────────────────────────

pub enum ConfigError {
    InvalidValue {
        field: &'static str,
        reason: String,
    },
    ParseError {
        field: &'static str,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    IoError {
        source: std::io::Error,
    },
    PathInvalid {
        field: &'static str,
        reason: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::InvalidValue { field, reason } => {
                write!(f, "invalid config value for '{field}': {reason}")
            }
            ConfigError::ParseError { field, source } => {
                write!(f, "failed to parse config for '{field}': {source}")
            }
            ConfigError::IoError { source } => write!(f, "config I/O error: {source}"),
            ConfigError::PathInvalid { field, reason } => {
                write!(f, "invalid path for '{field}': {reason}")
            }
        }
    }
}

impl fmt::Debug for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::ParseError { source, .. } => Some(source.as_ref()),
            ConfigError::IoError { source } => Some(source),
            _ => None,
        }
    }
}

// ── Partial config (one layer) ────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct PartialConfig {
    pub socket_path: Option<PathBuf>,
    pub wab_dir: Option<PathBuf>,
    pub shard_count: Option<usize>,
    pub worker_count: Option<usize>,
    pub batch_size: Option<usize>,
    pub batch_deadline_ms: Option<u64>,
    pub max_connections: Option<usize>,
    pub max_payload_bytes: Option<usize>,
    pub metrics_port: Option<u16>,
    pub shutdown_timeout_secs: Option<u64>,
    pub dead_letter_max_bytes: Option<u64>,
    pub dead_letter_check_interval_secs: Option<u64>,
    pub log_level: Option<String>,
}

impl PartialConfig {
    pub(crate) fn empty() -> Self {
        Self::default()
    }
}

// ── Final config ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Config {
    /// Absolute path to the Unix socket file.
    pub socket_path: PathBuf,
    /// Absolute canonicalized path to the WAB directory (must exist at startup).
    pub wab_dir: PathBuf,
    pub shard_count: usize,
    pub worker_count: usize,
    pub batch_size: usize,
    pub batch_deadline_ms: u64,
    pub max_connections: usize,
    pub max_payload_bytes: usize,
    pub metrics_port: u16,
    pub shutdown_timeout_secs: u64,
    pub dead_letter_max_bytes: u64,
    pub dead_letter_check_interval_secs: u64,
    pub log_level: String,
}

impl Config {
    /// Loads config from all three layers (CLI > env > file > defaults) and validates.
    pub fn load() -> Result<Self, ConfigError> {
        let (cli, config_path) = cli::parse()?;
        let file = file::read(&config_path)?;
        let env = env::read()?;
        Self::from_layers(cli, env, file)
    }

    /// Merges three partial-config layers (CLI > env > file) against defaults.
    /// Testable without touching real CLI args or env.
    pub(crate) fn from_layers(
        cli: PartialConfig,
        env: PartialConfig,
        file: PartialConfig,
    ) -> Result<Self, ConfigError> {
        macro_rules! merge {
            ($field:ident) => {
                cli.$field.or(env.$field).or(file.$field)
            };
        }

        // ── Paths ─────────────────────────────────────────────────────────────

        let socket_path =
            merge!(socket_path).unwrap_or_else(|| PathBuf::from("/run/weir/weir.sock"));
        validate_path_format("socket_path", &socket_path)?;

        let wab_dir_raw = merge!(wab_dir).unwrap_or_else(|| PathBuf::from("/var/lib/weir/wab"));
        let wab_dir = validate_path("wab_dir", &wab_dir_raw)?;

        // ── Integer fields with bounds ────────────────────────────────────────

        let shard_count = merge!(shard_count).unwrap_or(1);
        check_range("shard_count", shard_count, 1, 256)?;

        let worker_count = merge!(worker_count).unwrap_or(2);
        check_range("worker_count", worker_count, 1, 64)?;

        // Defaults from docs/benchmarks/batch-tuning.md: (256, 1ms) is the sweet
        // spot for both latency and throughput across the sweep grid.
        let batch_size = merge!(batch_size).unwrap_or(256);
        check_range("batch_size", batch_size, 1, 100_000)?;

        let batch_deadline_ms = merge!(batch_deadline_ms).unwrap_or(1);
        check_range("batch_deadline_ms", batch_deadline_ms as usize, 1, 60_000)?;

        let max_connections = merge!(max_connections).unwrap_or(256);
        check_range("max_connections", max_connections, 1, 512)?;

        let max_payload_bytes = merge!(max_payload_bytes).unwrap_or(MAX_PAYLOAD_HARD_CAP);
        if max_payload_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                field: "max_payload_bytes",
                reason: "must be at least 1".into(),
            });
        }
        if max_payload_bytes > MAX_PAYLOAD_HARD_CAP {
            return Err(ConfigError::InvalidValue {
                field: "max_payload_bytes",
                reason: format!(
                    "{max_payload_bytes} exceeds MAX_PAYLOAD_HARD_CAP ({MAX_PAYLOAD_HARD_CAP})"
                ),
            });
        }

        let metrics_port = merge!(metrics_port).unwrap_or(9185);
        if metrics_port == 0 {
            return Err(ConfigError::InvalidValue {
                field: "metrics_port",
                reason: "port 0 is not valid; use 1-65535".into(),
            });
        }

        let shutdown_timeout_secs = merge!(shutdown_timeout_secs).unwrap_or(30);
        if shutdown_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "shutdown_timeout_secs",
                reason: "must be at least 1".into(),
            });
        }

        let dead_letter_max_bytes = merge!(dead_letter_max_bytes).unwrap_or(1_073_741_824);
        if dead_letter_max_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                field: "dead_letter_max_bytes",
                reason: "must be greater than 0".into(),
            });
        }
        if dead_letter_max_bytes < 1_048_576 {
            warn!(
                dead_letter_max_bytes,
                "dead_letter_max_bytes is very small; consider increasing to avoid frequent \
                 BlockedDeadLetterFull transitions"
            );
        }

        let dead_letter_check_interval_secs = merge!(dead_letter_check_interval_secs).unwrap_or(30);
        check_range(
            "dead_letter_check_interval_secs",
            dead_letter_check_interval_secs as usize,
            1,
            3_600,
        )?;

        let log_level = merge!(log_level).unwrap_or_else(|| "info".into());

        Ok(Config {
            socket_path,
            wab_dir,
            shard_count,
            worker_count,
            batch_size,
            batch_deadline_ms,
            max_connections,
            max_payload_bytes,
            metrics_port,
            shutdown_timeout_secs,
            dead_letter_max_bytes,
            dead_letter_check_interval_secs,
            log_level,
        })
    }
}

// ── Path validation ───────────────────────────────────────────────────────────

/// Validates a directory path that must already exist.
/// Returns the canonicalized path.
pub(crate) fn validate_path(field: &'static str, path: &Path) -> Result<PathBuf, ConfigError> {
    validate_path_format_inner(field, path)?;

    let canonical = std::fs::canonicalize(path).map_err(|e| ConfigError::PathInvalid {
        field,
        reason: format!("cannot canonicalize '{}': {e}", path.display()),
    })?;

    if !canonical.is_absolute() {
        return Err(ConfigError::PathInvalid {
            field,
            reason: format!(
                "canonicalized path '{}' is not absolute",
                canonical.display()
            ),
        });
    }
    if canonical.components().any(|c| c == Component::ParentDir) {
        return Err(ConfigError::PathInvalid {
            field,
            reason: format!(
                "canonicalized path '{}' contains '..' components",
                canonical.display()
            ),
        });
    }

    Ok(canonical)
}

/// Format-only check (absolute, no `..`, no null bytes). Does not require the
/// path to exist — used for socket_path which is created at bind time.
fn validate_path_format(field: &'static str, path: &Path) -> Result<(), ConfigError> {
    validate_path_format_inner(field, path).map(|_| ())
}

fn validate_path_format_inner<'a>(
    field: &'static str,
    path: &'a Path,
) -> Result<&'a Path, ConfigError> {
    if !path.is_absolute() {
        return Err(ConfigError::PathInvalid {
            field,
            reason: format!("'{}' is not absolute", path.display()),
        });
    }
    if path.components().any(|c| c == Component::ParentDir) {
        return Err(ConfigError::PathInvalid {
            field,
            reason: format!("'{}' contains '..' components", path.display()),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().contains(&0u8) {
            return Err(ConfigError::PathInvalid {
                field,
                reason: "path contains a null byte".into(),
            });
        }
    }
    Ok(path)
}

// ── Bound checking helper ─────────────────────────────────────────────────────

fn check_range<T>(field: &'static str, value: T, min: T, max: T) -> Result<(), ConfigError>
where
    T: PartialOrd + fmt::Display,
{
    if value < min || value > max {
        Err(ConfigError::InvalidValue {
            field,
            reason: format!("{value} is out of range [{min}, {max}]"),
        })
    } else {
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weir_cfg_{label}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn layers_with_wab(wab_dir: PathBuf) -> Result<Config, ConfigError> {
        Config::from_layers(
            PartialConfig {
                wab_dir: Some(wab_dir),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
    }

    #[test]
    fn defaults_applied_for_non_path_fields() {
        let dir = tmp_dir("defaults");
        let c = layers_with_wab(dir.clone()).unwrap();
        assert_eq!(c.shard_count, 1);
        assert_eq!(c.worker_count, 2);
        assert_eq!(c.batch_size, 256);
        assert_eq!(c.batch_deadline_ms, 1);
        assert_eq!(c.max_connections, 256);
        assert_eq!(c.max_payload_bytes, MAX_PAYLOAD_HARD_CAP);
        assert_eq!(c.metrics_port, 9185);
        assert_eq!(c.shutdown_timeout_secs, 30);
        assert_eq!(c.dead_letter_max_bytes, 1_073_741_824);
        assert_eq!(c.dead_letter_check_interval_secs, 30);
        assert_eq!(c.log_level, "info");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn file_layer_overrides_defaults() {
        let dir = tmp_dir("file_override");
        let c = Config::from_layers(
            PartialConfig::empty(),
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(4),
                worker_count: Some(8),
                log_level: Some("debug".into()),
                ..PartialConfig::empty()
            },
        )
        .unwrap();
        assert_eq!(c.shard_count, 4);
        assert_eq!(c.worker_count, 8);
        assert_eq!(c.log_level, "debug");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn env_layer_overrides_file() {
        let dir = tmp_dir("env_override");
        let c = Config::from_layers(
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(2),
                ..PartialConfig::empty()
            },
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(8),
                ..PartialConfig::empty()
            },
        )
        .unwrap();
        assert_eq!(c.shard_count, 2, "env should override file");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn cli_layer_overrides_env() {
        let dir = tmp_dir("cli_override");
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(1),
                ..PartialConfig::empty()
            },
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(4),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
        )
        .unwrap();
        assert_eq!(c.shard_count, 1, "CLI should override env");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn shard_count_out_of_range_rejected() {
        let dir = tmp_dir("range_shard");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                shard_count: Some(0),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("shard_count"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn max_payload_bytes_above_hard_cap_rejected() {
        let dir = tmp_dir("payload_cap");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                max_payload_bytes: Some(MAX_PAYLOAD_HARD_CAP + 1),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("MAX_PAYLOAD_HARD_CAP"), "{msg}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dead_letter_max_bytes_zero_rejected() {
        let dir = tmp_dir("dl_zero");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                dead_letter_max_bytes: Some(0),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("dead_letter_max_bytes"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dead_letter_check_interval_out_of_range_rejected() {
        let dir = tmp_dir("dl_interval");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                dead_letter_check_interval_secs: Some(3601),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("dead_letter_check_interval_secs"),
            "{err}"
        );
        fs::remove_dir_all(dir).ok();
    }

    // ── validate_path ─────────────────────────────────────────────────────────

    #[test]
    fn validate_path_rejects_relative() {
        let err = validate_path("wab_dir", Path::new("relative/path")).unwrap_err();
        assert!(err.to_string().contains("wab_dir"), "{err}");
        assert!(err.to_string().contains("not absolute"), "{err}");
    }

    #[test]
    fn validate_path_rejects_dotdot() {
        let err = validate_path("wab_dir", Path::new("/valid/../escape")).unwrap_err();
        assert!(err.to_string().contains("wab_dir"), "{err}");
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    #[test]
    fn validate_path_rejects_nonexistent() {
        let err =
            validate_path("wab_dir", Path::new("/weir_cfg_test_no_such_dir_xyzzy")).unwrap_err();
        assert!(err.to_string().contains("wab_dir"), "{err}");
    }

    #[test]
    fn validate_path_accepts_existing_dir() {
        let dir = tmp_dir("vp_ok");
        assert!(validate_path("wab_dir", &dir).is_ok());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn validate_path_format_rejects_relative() {
        let err = validate_path_format("socket_path", Path::new("relative.sock")).unwrap_err();
        assert!(err.to_string().contains("socket_path"), "{err}");
    }

    #[test]
    fn validate_path_format_accepts_nonexistent_absolute() {
        assert!(validate_path_format("socket_path", Path::new("/run/weir/weir.sock")).is_ok());
    }

    #[test]
    #[cfg(unix)]
    fn missing_config_file_returns_empty_partial() {
        let result =
            file::read(Path::new("/weir_test_no_such_config_file_xyzzy_12345.toml")).unwrap();
        assert!(result.shard_count.is_none());
        assert!(result.wab_dir.is_none());
    }
}
