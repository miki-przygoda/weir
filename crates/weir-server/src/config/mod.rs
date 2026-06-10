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
    net::{IpAddr, SocketAddr},
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

/// Which built-in sink the daemon should run with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    Noop,
    #[cfg(feature = "http-sink")]
    Http,
    #[cfg(feature = "mysql-sink")]
    Mysql,
    #[cfg(feature = "postgres-sink")]
    Postgres,
    #[cfg(feature = "clickhouse-sink")]
    ClickHouse,
}

impl SinkType {
    fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "noop" => Ok(SinkType::Noop),
            #[cfg(feature = "http-sink")]
            "http" => Ok(SinkType::Http),
            #[cfg(not(feature = "http-sink"))]
            "http" => Err(ConfigError::InvalidValue {
                field: "sink_type",
                reason: "sink_type 'http' requires the 'http-sink' feature; \
                         this binary was built without it"
                    .to_string(),
            }),
            #[cfg(feature = "mysql-sink")]
            "mysql" => Ok(SinkType::Mysql),
            #[cfg(not(feature = "mysql-sink"))]
            "mysql" => Err(ConfigError::InvalidValue {
                field: "sink_type",
                reason: "sink_type 'mysql' requires the 'mysql-sink' feature; \
                         this binary was built without it"
                    .to_string(),
            }),
            #[cfg(feature = "postgres-sink")]
            "postgres" => Ok(SinkType::Postgres),
            #[cfg(not(feature = "postgres-sink"))]
            "postgres" => Err(ConfigError::InvalidValue {
                field: "sink_type",
                reason: "sink_type 'postgres' requires the 'postgres-sink' feature; \
                         this binary was built without it"
                    .to_string(),
            }),
            #[cfg(feature = "clickhouse-sink")]
            "clickhouse" => Ok(SinkType::ClickHouse),
            #[cfg(not(feature = "clickhouse-sink"))]
            "clickhouse" => Err(ConfigError::InvalidValue {
                field: "sink_type",
                reason: "sink_type 'clickhouse' requires the 'clickhouse-sink' feature; \
                         this binary was built without it"
                    .to_string(),
            }),
            other => Err(ConfigError::InvalidValue {
                field: "sink_type",
                reason: format!(
                    "'{other}' is not a valid sink type; expected 'noop', 'http', \
                     'mysql', 'postgres', or 'clickhouse'"
                ),
            }),
        }
    }
}

/// Parses the `sink_mysql_insert_mode` config string into the sink-layer enum.
///
/// `InsertMode` lives in `sink::mysql` (where the variants are used to phrase
/// SQL); this function is the config-layer adapter that turns user-facing
/// strings into that enum so the config doesn't have to maintain a parallel
/// copy of the variants.
#[cfg(feature = "mysql-sink")]
fn parse_mysql_insert_mode(s: &str) -> Result<crate::sink::mysql::InsertMode, ConfigError> {
    use crate::sink::mysql::InsertMode;
    match s {
        "ignore" => Ok(InsertMode::Ignore),
        "plain" => Ok(InsertMode::Plain),
        other => Err(ConfigError::InvalidValue {
            field: "sink_mysql_insert_mode",
            reason: format!("'{other}' is not a valid insert mode; expected 'ignore' or 'plain'"),
        }),
    }
}

/// Parses the `sink_postgres_insert_mode` config string into the sink-layer
/// enum. Mirror of [`parse_mysql_insert_mode`] for the Postgres sink.
#[cfg(feature = "postgres-sink")]
fn parse_postgres_insert_mode(s: &str) -> Result<crate::sink::postgres::InsertMode, ConfigError> {
    use crate::sink::postgres::InsertMode;
    match s {
        "on_conflict_do_nothing" => Ok(InsertMode::OnConflictDoNothing),
        "plain" => Ok(InsertMode::Plain),
        other => Err(ConfigError::InvalidValue {
            field: "sink_postgres_insert_mode",
            reason: format!(
                "'{other}' is not a valid insert mode; expected \
                 'on_conflict_do_nothing' or 'plain'"
            ),
        }),
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
    pub wab_segment_max_bytes: Option<u64>,
    pub max_connections: Option<usize>,
    pub max_payload_bytes: Option<usize>,
    pub metrics_port: Option<u16>,
    pub metrics_bind: Option<String>,
    pub metrics_max_connections: Option<usize>,
    pub peer_uid_check: Option<bool>,
    pub shutdown_timeout_secs: Option<u64>,
    pub connection_read_timeout_secs: Option<u64>,
    pub sink_type: Option<String>,
    pub sink_url: Option<String>,
    pub sink_timeout_secs: Option<u64>,
    pub sink_max_batch_size: Option<usize>,
    pub sink_send_idempotency_key: Option<bool>,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_table: Option<String>,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_column: Option<String>,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_insert_mode: Option<String>,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_table: Option<String>,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_column: Option<String>,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_insert_mode: Option<String>,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_database: Option<String>,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_table: Option<String>,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_column: Option<String>,
    pub dead_letter_max_bytes: Option<u64>,
    pub dead_letter_check_interval_secs: Option<u64>,
    pub log_level: Option<String>,
    pub tcp_bind: Option<String>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_client_ca_path: Option<PathBuf>,
    pub tls_handshake_timeout_secs: Option<u64>,
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
    /// Number of worker threads draining the work queue. The queue is
    /// partitioned by `shard_id`, so each shard is owned by exactly one
    /// worker (`worker_idx = shard_id % worker_count`). For best throughput
    /// set `worker_count == shard_count` — values above `shard_count` leave
    /// the excess workers idle (no shard ever routes to them), values below
    /// share a worker across multiple shards (still correct, lower
    /// parallelism per shard).
    pub worker_count: usize,
    pub batch_size: usize,
    pub batch_deadline_ms: u64,
    pub wab_segment_max_bytes: u64,
    pub max_connections: usize,
    pub max_payload_bytes: usize,
    pub metrics_port: u16,
    /// IP address the metrics HTTP endpoint binds to. Default `127.0.0.1`
    /// — localhost-only so an operator who runs weir on a multi-tenant host
    /// doesn't accidentally expose internal counters to the LAN. Override to
    /// `0.0.0.0` (or a specific interface) only after deciding that the
    /// metrics surface is safe to publish: the `weir_records_nack` family by
    /// reason exposes a decode-error oracle, and there is no authentication.
    pub metrics_bind: IpAddr,
    /// Cap on concurrent metrics-endpoint connections. Default 8 — a scrape
    /// is a single request/response, and a real Prometheus deployment never
    /// runs more than a handful of scrapers in parallel. Bounds the
    /// fork-bomb surface so a misconfigured (or hostile) client can't spawn
    /// unbounded tokio tasks against the endpoint.
    pub metrics_max_connections: usize,
    /// When true (default), the accept loop reads the peer's effective uid
    /// via SO_PEERCRED (Linux) / getpeereid (macOS) and refuses connections
    /// whose uid does not match the daemon's effective uid. Defense-in-depth
    /// on top of the socket file's `0o600` mode: if those bits are ever
    /// loosened by operator error or a producer reaches the socket inode
    /// some other way, mismatched-uid connections are still refused.
    ///
    /// Set to false only in environments where producers legitimately run
    /// under a different uid than the daemon AND the socket directory's
    /// permissions are the chosen trust boundary.
    pub peer_uid_check: bool,
    pub shutdown_timeout_secs: u64,
    pub connection_read_timeout_secs: u64,
    pub sink_type: SinkType,
    // These four fields are only read by the non-noop sink arms in main.rs.
    // When no non-noop sink feature is enabled they are dead code; suppress
    // the lint so `cargo clippy -- -D warnings` stays clean on noop-only builds.
    #[cfg_attr(
        not(any(
            feature = "http-sink",
            feature = "mysql-sink",
            feature = "postgres-sink"
        )),
        allow(dead_code)
    )]
    pub sink_url: Option<String>,
    #[cfg_attr(
        not(any(
            feature = "http-sink",
            feature = "mysql-sink",
            feature = "postgres-sink"
        )),
        allow(dead_code)
    )]
    pub sink_timeout_secs: u64,
    #[cfg_attr(
        not(any(
            feature = "http-sink",
            feature = "mysql-sink",
            feature = "postgres-sink"
        )),
        allow(dead_code)
    )]
    pub sink_max_batch_size: usize,
    // sink_send_idempotency_key is only consumed by the http-sink arm.
    #[cfg_attr(not(feature = "http-sink"), allow(dead_code))]
    pub sink_send_idempotency_key: bool,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_table: String,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_column: String,
    #[cfg(feature = "mysql-sink")]
    pub sink_mysql_insert_mode: crate::sink::mysql::InsertMode,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_table: String,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_column: String,
    #[cfg(feature = "postgres-sink")]
    pub sink_postgres_insert_mode: crate::sink::postgres::InsertMode,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_database: String,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_table: String,
    #[cfg(feature = "clickhouse-sink")]
    pub sink_clickhouse_column: String,
    pub dead_letter_max_bytes: u64,
    pub dead_letter_check_interval_secs: u64,
    pub log_level: String,
    /// TCP listen address for the mTLS listener (e.g. `0.0.0.0:7100`).
    /// `None` ⇒ no TCP listener; Unix-only. When `Some`, the three `tls_*`
    /// paths are required and the binary must be built with `--features tls`.
    // These fields are read by the `#[cfg(feature = "tls")]` TCP block in
    // main.rs. On the default (no-tls) build that block is compiled out, so
    // the fields are unused and the lint must be suppressed for that build
    // only. On tls builds they ARE read, so no suppression is needed there.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tcp_bind: Option<SocketAddr>,
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_cert_path: Option<PathBuf>,
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_key_path: Option<PathBuf>,
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_client_ca_path: Option<PathBuf>,
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_handshake_timeout_secs: u64,
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

        // Default 256 MiB matches the historical hard-coded behaviour.
        // Lower bound 4 KiB so the segment header (one page) fits;
        // upper bound 4 GiB so a single sealed segment can't exceed 32-bit
        // file-offset assumptions downstream.
        let wab_segment_max_bytes = merge!(wab_segment_max_bytes).unwrap_or(256 * 1024 * 1024);
        if !(4096..=4 * 1024 * 1024 * 1024).contains(&wab_segment_max_bytes) {
            return Err(ConfigError::InvalidValue {
                field: "wab_segment_max_bytes",
                reason: format!(
                    "{wab_segment_max_bytes} is outside the supported range [4096, 4294967296]"
                ),
            });
        }

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

        // Default 127.0.0.1: localhost-only. See Config::metrics_bind docs for
        // the security reasoning. Operators who want LAN-visible metrics must
        // explicitly set "0.0.0.0" (or a specific interface address).
        let metrics_bind_str = merge!(metrics_bind).unwrap_or_else(|| "127.0.0.1".to_string());
        let metrics_bind =
            metrics_bind_str
                .parse::<IpAddr>()
                .map_err(|_| ConfigError::InvalidValue {
                    field: "metrics_bind",
                    reason: format!(
                        "'{metrics_bind_str}' is not a valid IP address (expected e.g. \
                     '127.0.0.1', '0.0.0.0', '::1', or a specific interface address)"
                    ),
                })?;

        let metrics_max_connections = merge!(metrics_max_connections).unwrap_or(8);
        check_range("metrics_max_connections", metrics_max_connections, 1, 1024)?;

        let peer_uid_check = merge!(peer_uid_check).unwrap_or(true);

        let shutdown_timeout_secs = merge!(shutdown_timeout_secs).unwrap_or(30);
        if shutdown_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "shutdown_timeout_secs",
                reason: "must be at least 1".into(),
            });
        }

        // Per-connection read timeout. Caps how long a connection handler can
        // sit in read_exact waiting for the next byte. Without this a slow or
        // silent client can hold a semaphore permit indefinitely (slowloris).
        // Default 30 s is generous for legitimate clients doing batched
        // sends but cuts off a stalled connection promptly.
        let connection_read_timeout_secs = merge!(connection_read_timeout_secs).unwrap_or(30);
        check_range(
            "connection_read_timeout_secs",
            connection_read_timeout_secs as usize,
            1,
            600,
        )?;

        // ── Sink ──────────────────────────────────────────────────────────────

        let sink_type_str = merge!(sink_type).unwrap_or_else(|| "noop".to_string());
        let sink_type = SinkType::parse(&sink_type_str)?;

        let sink_url = merge!(sink_url);
        // Validate that sink_url is set whenever a sink type that requires it
        // is selected. Each check is guarded by the matching feature so the
        // variant only exists when the feature is compiled in.
        #[cfg(feature = "http-sink")]
        if matches!(sink_type, SinkType::Http) && sink_url.as_deref().unwrap_or("").is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "sink_url",
                reason: "sink_url must be set when sink_type = \"http\"".to_string(),
            });
        }
        #[cfg(feature = "mysql-sink")]
        if matches!(sink_type, SinkType::Mysql) && sink_url.as_deref().unwrap_or("").is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "sink_url",
                reason: "sink_url must be set when sink_type = \"mysql\"".to_string(),
            });
        }
        #[cfg(feature = "postgres-sink")]
        if matches!(sink_type, SinkType::Postgres) && sink_url.as_deref().unwrap_or("").is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "sink_url",
                reason: "sink_url must be set when sink_type = \"postgres\"".to_string(),
            });
        }
        #[cfg(feature = "clickhouse-sink")]
        if matches!(sink_type, SinkType::ClickHouse) && sink_url.as_deref().unwrap_or("").is_empty()
        {
            return Err(ConfigError::InvalidValue {
                field: "sink_url",
                reason: "sink_url must be set when sink_type = \"clickhouse\"".to_string(),
            });
        }

        let sink_timeout_secs = merge!(sink_timeout_secs).unwrap_or(10);
        check_range("sink_timeout_secs", sink_timeout_secs as usize, 1, 300)?;

        let sink_max_batch_size = merge!(sink_max_batch_size).unwrap_or(100);
        check_range("sink_max_batch_size", sink_max_batch_size, 1, 10_000)?;

        // Default true: at-least-once delivery means retries re-POST records
        // that may have been accepted; the Idempotency-Key header lets the
        // endpoint dedupe without computing the hash itself.
        let sink_send_idempotency_key = merge!(sink_send_idempotency_key).unwrap_or(true);

        // MySQL sink: identifier validation happens inside MySqlSink::new at
        // startup (strict [A-Za-z_][A-Za-z0-9_]{0,63} rule, single source of
        // truth so future identifier-policy changes only touch one file).
        #[cfg(feature = "mysql-sink")]
        let sink_mysql_table =
            merge!(sink_mysql_table).unwrap_or_else(|| "weir_records".to_string());
        #[cfg(feature = "mysql-sink")]
        let sink_mysql_column = merge!(sink_mysql_column).unwrap_or_else(|| "payload".to_string());
        #[cfg(feature = "mysql-sink")]
        let sink_mysql_insert_mode_str =
            merge!(sink_mysql_insert_mode).unwrap_or_else(|| "ignore".to_string());
        #[cfg(feature = "mysql-sink")]
        let sink_mysql_insert_mode = parse_mysql_insert_mode(&sink_mysql_insert_mode_str)?;

        // Postgres sink: same identifier-validation story as MySQL; happens
        // inside PostgresSink::new at startup. Default insert mode is
        // `on_conflict_do_nothing` (idempotent under crash-recovery retries
        // when paired with a UNIQUE constraint).
        #[cfg(feature = "postgres-sink")]
        let sink_postgres_table =
            merge!(sink_postgres_table).unwrap_or_else(|| "weir_records".to_string());
        #[cfg(feature = "postgres-sink")]
        let sink_postgres_column =
            merge!(sink_postgres_column).unwrap_or_else(|| "payload".to_string());
        #[cfg(feature = "postgres-sink")]
        let sink_postgres_insert_mode_str = merge!(sink_postgres_insert_mode)
            .unwrap_or_else(|| "on_conflict_do_nothing".to_string());
        #[cfg(feature = "postgres-sink")]
        let sink_postgres_insert_mode = parse_postgres_insert_mode(&sink_postgres_insert_mode_str)?;

        // ClickHouse sink: HTTP RowBinary inserts; identifier validation happens
        // inside ClickHouseSink::new at startup. Defaults mirror the SQL sinks.
        #[cfg(feature = "clickhouse-sink")]
        let sink_clickhouse_database =
            merge!(sink_clickhouse_database).unwrap_or_else(|| "default".to_string());
        #[cfg(feature = "clickhouse-sink")]
        let sink_clickhouse_table =
            merge!(sink_clickhouse_table).unwrap_or_else(|| "weir_records".to_string());
        #[cfg(feature = "clickhouse-sink")]
        let sink_clickhouse_column =
            merge!(sink_clickhouse_column).unwrap_or_else(|| "payload".to_string());

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

        // ── TCP + TLS ────────────────────────────────────────────────────────────
        let tcp_bind_str = merge!(tcp_bind);
        let tls_cert_path = merge!(tls_cert_path);
        let tls_key_path = merge!(tls_key_path);
        let tls_client_ca_path = merge!(tls_client_ca_path);
        let tls_handshake_timeout_secs = merge!(tls_handshake_timeout_secs).unwrap_or(10);

        let tcp_bind = match tcp_bind_str {
            None => None,
            Some(s) => {
                let addr = s.parse::<SocketAddr>().map_err(|_| ConfigError::InvalidValue {
                    field: "tcp_bind",
                    reason: format!(
                        "'{s}' is not a valid socket address (expected e.g. '0.0.0.0:7100' or '[::]:7100')"
                    ),
                })?;
                if !cfg!(feature = "tls") {
                    return Err(ConfigError::InvalidValue {
                        field: "tcp_bind",
                        reason: "tcp_bind is set but this binary was built without the 'tls' \
                                 feature; rebuild with --features tls \
                                 (plaintext TCP is never exposed)"
                            .to_string(),
                    });
                }
                // Validate each required TLS path field individually so we
                // can produce `&'static str` field names for ConfigError.
                macro_rules! require_tls_path {
                    ($fname:literal, $opt:expr) => {{
                        let path = $opt.as_ref().ok_or(ConfigError::InvalidValue {
                            field: $fname,
                            reason: concat!($fname, " is required when tcp_bind is set")
                                .to_string(),
                        })?;
                        if !path.exists() {
                            return Err(ConfigError::InvalidValue {
                                field: $fname,
                                reason: format!("{} '{}' does not exist", $fname, path.display()),
                            });
                        }
                    }};
                }
                require_tls_path!("tls_cert_path", tls_cert_path);
                require_tls_path!("tls_key_path", tls_key_path);
                require_tls_path!("tls_client_ca_path", tls_client_ca_path);
                Some(addr)
            }
        };

        Ok(Config {
            socket_path,
            wab_dir,
            shard_count,
            worker_count,
            batch_size,
            batch_deadline_ms,
            wab_segment_max_bytes,
            max_connections,
            max_payload_bytes,
            metrics_port,
            metrics_bind,
            metrics_max_connections,
            peer_uid_check,
            shutdown_timeout_secs,
            connection_read_timeout_secs,
            sink_type,
            sink_url,
            sink_timeout_secs,
            sink_max_batch_size,
            sink_send_idempotency_key,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_table,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_column,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_insert_mode,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_table,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_column,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_insert_mode,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_database,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_table,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_column,
            dead_letter_max_bytes,
            dead_letter_check_interval_secs,
            log_level,
            tcp_bind,
            tls_cert_path,
            tls_key_path,
            tls_client_ca_path,
            tls_handshake_timeout_secs,
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
        assert_eq!(c.wab_segment_max_bytes, 256 * 1024 * 1024);
        assert_eq!(c.max_connections, 256);
        assert_eq!(c.max_payload_bytes, MAX_PAYLOAD_HARD_CAP);
        assert_eq!(c.metrics_port, 9185);
        assert_eq!(c.shutdown_timeout_secs, 30);
        assert_eq!(c.connection_read_timeout_secs, 30);
        assert_eq!(c.sink_type, SinkType::Noop);
        assert_eq!(c.sink_url, None);
        assert_eq!(c.sink_timeout_secs, 10);
        assert_eq!(c.sink_max_batch_size, 100);
        assert!(c.sink_send_idempotency_key);
        #[cfg(feature = "mysql-sink")]
        assert_eq!(c.sink_mysql_table, "weir_records");
        #[cfg(feature = "mysql-sink")]
        assert_eq!(c.sink_mysql_column, "payload");
        #[cfg(feature = "mysql-sink")]
        assert_eq!(
            c.sink_mysql_insert_mode,
            crate::sink::mysql::InsertMode::Ignore
        );
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

    // ── wab_segment_max_bytes ─────────────────────────────────────────────────

    #[test]
    fn wab_segment_max_bytes_below_minimum_rejected() {
        let dir = tmp_dir("seg_too_small");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                wab_segment_max_bytes: Some(4095),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("wab_segment_max_bytes"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_segment_max_bytes_above_maximum_rejected() {
        let dir = tmp_dir("seg_too_big");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                wab_segment_max_bytes: Some(4 * 1024 * 1024 * 1024 + 1),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("wab_segment_max_bytes"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_segment_max_bytes_accepts_in_range_values() {
        let dir = tmp_dir("seg_ok");
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                wab_segment_max_bytes: Some(64 * 1024),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap();
        assert_eq!(c.wab_segment_max_bytes, 64 * 1024);
        fs::remove_dir_all(dir).ok();
    }

    // ── Sink: MySQL ───────────────────────────────────────────────────────────

    #[cfg(feature = "mysql-sink")]
    #[test]
    fn sink_type_mysql_requires_url() {
        let dir = tmp_dir("mysql_no_url");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                sink_type: Some("mysql".into()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("sink_url"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[cfg(feature = "mysql-sink")]
    #[test]
    fn sink_type_mysql_accepts_url() {
        let dir = tmp_dir("mysql_url");
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                sink_type: Some("mysql".into()),
                sink_url: Some("mysql://u:p@host/db".into()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap();
        assert_eq!(c.sink_type, SinkType::Mysql);
        assert_eq!(
            c.sink_mysql_insert_mode,
            crate::sink::mysql::InsertMode::Ignore
        );
        fs::remove_dir_all(dir).ok();
    }

    #[cfg(feature = "mysql-sink")]
    #[test]
    fn sink_mysql_insert_mode_parses() {
        for (input, expected) in [
            ("ignore", crate::sink::mysql::InsertMode::Ignore),
            ("plain", crate::sink::mysql::InsertMode::Plain),
        ] {
            let dir = tmp_dir(&format!("mysql_mode_{input}"));
            let c = Config::from_layers(
                PartialConfig {
                    wab_dir: Some(dir.clone()),
                    sink_type: Some("mysql".into()),
                    sink_url: Some("mysql://u:p@h/d".into()),
                    sink_mysql_insert_mode: Some(input.into()),
                    ..PartialConfig::empty()
                },
                PartialConfig::empty(),
                PartialConfig::empty(),
            )
            .unwrap();
            assert_eq!(c.sink_mysql_insert_mode, expected);
            fs::remove_dir_all(dir).ok();
        }
    }

    #[cfg(feature = "mysql-sink")]
    #[test]
    fn sink_mysql_insert_mode_rejects_garbage() {
        let dir = tmp_dir("mysql_bad_mode");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                sink_type: Some("mysql".into()),
                sink_url: Some("mysql://u:p@h/d".into()),
                sink_mysql_insert_mode: Some("upsert".into()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("sink_mysql_insert_mode"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn unknown_sink_type_rejected_with_helpful_message() {
        let dir = tmp_dir("bad_sink_type");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                // Was "postgres" — now a valid sink type. Pick something
                // that won't ever be a real backend name to keep the test
                // forward-stable.
                sink_type: Some("definitely-not-a-real-sink".into()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("sink_type"), "{msg}");
        // The error message for a completely-unknown sink type always names
        // all four backends regardless of which features are compiled in,
        // because that arm fires only when the string doesn't match any of
        // the known names (known names return feature-dependent errors).
        assert!(msg.contains("noop"), "{msg}");
        assert!(msg.contains("http"), "{msg}");
        assert!(msg.contains("mysql"), "{msg}");
        assert!(msg.contains("postgres"), "{msg}");
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

    // ── TCP + TLS config ──────────────────────────────────────────────────────

    #[test]
    fn tcp_bind_invalid_addr_is_rejected() {
        let dir = tmp_dir("tcp_bad_addr");
        let mut cli = PartialConfig::empty();
        cli.tcp_bind = Some("not-an-addr".to_string());
        let err = Config::from_layers(
            cli,
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                ..PartialConfig::empty()
            },
        )
        .expect_err("bad addr must fail");
        assert!(err.to_string().contains("tcp_bind"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn no_tcp_bind_defaults_to_unix_only() {
        let dir = tmp_dir("tcp_none");
        let cfg = Config::from_layers(
            PartialConfig::empty(),
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                ..PartialConfig::empty()
            },
        )
        .unwrap();
        assert!(cfg.tcp_bind.is_none());
        assert_eq!(cfg.tls_handshake_timeout_secs, 10);
        fs::remove_dir_all(dir).ok();
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tcp_bind_without_tls_paths_is_rejected() {
        let dir = tmp_dir("tcp_no_certs");
        let mut cli = PartialConfig::empty();
        cli.tcp_bind = Some("127.0.0.1:7100".to_string());
        let err = Config::from_layers(
            cli,
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                ..PartialConfig::empty()
            },
        )
        .expect_err("tcp_bind without certs must fail");
        assert!(err.to_string().contains("tls_cert_path"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tcp_bind_without_tls_feature_is_rejected() {
        let dir = tmp_dir("tcp_no_feature");
        let mut cli = PartialConfig::empty();
        cli.tcp_bind = Some("127.0.0.1:7100".to_string());
        let err = Config::from_layers(
            cli,
            PartialConfig::empty(),
            PartialConfig {
                wab_dir: Some(dir.clone()),
                ..PartialConfig::empty()
            },
        )
        .expect_err("tcp_bind without tls feature must fail");
        // Assert the error names the missing FEATURE specifically, so this test
        // isolates the compile-time feature guard from the "path missing" error
        // (which would also contain the substring "tls").
        assert!(err.to_string().contains("feature"), "{err}");
        fs::remove_dir_all(dir).ok();
    }
}
