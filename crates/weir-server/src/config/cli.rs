use std::path::PathBuf;

use super::{ConfigError, PartialConfig};

pub(super) const HELP: &str = "\
weir-server — write-ahead buffer daemon

USAGE:
    weir-server [OPTIONS]

OPTIONS:
    --config <path>                          Config file path [default: /etc/weir/weir.toml]
    --socket-path <path>                     Unix socket path [env: WEIR_SOCKET_PATH]
    --wab-dir <path>                         WAB directory [env: WEIR_WAB_DIR]
    --shard-count <n>                        Number of WAB shards (1-256) [default: 1]
    --worker-count <n>                       Worker thread count (1-64)
                                               [default: min(shard-count, 64)]
    --batch-size <n>                         Records per flush batch (1-100000) [default: 256]
    --batch-deadline-ms <n>                  Batch accumulation time ms (1-60000) [default: 1]
    --wab-segment-max-bytes <n>              WAB segment rotation threshold (4096-4294967296)
                                               [default: 268435456 (256 MiB)]
    --wab-segment-max-age-secs <n>           Idle-seal threshold secs, 0=disabled (0-86400)
                                               [default: 0]
    --max-connections <n>                    Connection cap (1-512) [default: 256]
    --max-payload-bytes <n>                  Payload cap in bytes [default: 16777216]
    --connection-read-timeout-secs <n>       Slowloris guard (1-600) [default: 30]
    --metrics-port <port>                    Prometheus metrics port [default: 9185]
    --metrics-bind <addr>                    Metrics bind IP (use 0.0.0.0 to expose
                                               on the network) [default: 127.0.0.1]
    --metrics-max-connections <n>            Concurrent metrics scrapes cap
                                               (1-1024) [default: 8]
    --peer-uid-check <bool>                  Refuse connections from uids other
                                               than the daemon's [default: true]
    --shutdown-timeout-secs <secs>           Graceful shutdown timeout (1+) [default: 30]
    --sink-type <type>                       Sink: noop | http | mysql | postgres |
                                               clickhouse [default: noop]
    --sink-url <url>                         Sink URL (required for http/mysql/postgres/
                                               clickhouse)
    --sink-timeout-secs <secs>               Per-request sink timeout (1-300) [default: 10]
    --sink-max-batch-size <n>                Sink commit batch cap (1-10000) [default: 100]
    --sink-send-idempotency-key <bool>       Send Idempotency-Key header (http) [default: true]
    --sink-http-concurrency <n>              Max concurrent POSTs per batch (http, 1-1024)
                                               [default: 8]
    --sink-http-batch <mode>                 HTTP framing: none (per-record POSTs) or
                                               ndjson (one POST/batch) [default: none]
    --sink-max-retries <n>                   Transient-retry attempts before a segment is
                                               stranded (0-100) [default: 3]
    --sink-retry-base-delay-ms <ms>          First retry backoff, doubles each retry
                                               (1-60000) [default: 100]
    --sink-mysql-table <name>                MySQL target table [default: weir_records]
    --sink-mysql-column <name>               MySQL target column [default: payload]
    --sink-mysql-insert-mode <mode>          MySQL: ignore | plain [default: ignore]
    --sink-postgres-table <name>             Postgres target table [default: weir_records]
    --sink-postgres-column <name>            Postgres target column [default: payload]
    --sink-postgres-insert-mode <mode>       Postgres: on_conflict_do_nothing | plain
                                               [default: on_conflict_do_nothing]
    --sink-clickhouse-database <name>        ClickHouse database (requires build with
                                               --features clickhouse-sink) [default: default]
    --sink-clickhouse-table <name>           ClickHouse target table [default: weir_records]
    --sink-clickhouse-column <name>          ClickHouse target column [default: payload]
    --dead-letter-max-bytes <n>              Dead-letter dir size cap [default: 1073741824]
    --dead-letter-check-interval-secs <n>    Blocked-state wake interval (1-3600) [default: 30]
    --health-poll-interval-secs <n>          Sink health + stranded-segment rescan cadence
                                               secs (1-3600) [default: 30]
    --log-level <level>                      Log level (trace/debug/info/warn/error) [default: info]
    --tcp-bind <addr>                        TCP listen address for the mTLS listener
                                               (e.g. '0.0.0.0:7100'). Requires --features tls
                                               and the three --tls-* path flags. [default: none]
    --tls-cert <path>                        Path to the server TLS certificate (PEM)
    --tls-key <path>                         Path to the server TLS private key (PEM)
    --tls-client-ca <path>                   Path to the client CA certificate for mTLS (PEM)
    --tls-handshake-timeout-secs <n>         TLS handshake timeout in seconds [default: 10]
    -h, --help                               Print this help and exit

ENVIRONMENT:
    Every option above can be set via WEIR_<UPPER_SNAKE_NAME>.
    WEIR_SINK_BEARER_TOKEN is env-only (never sourced from --sink-* flags
    or the config file). When the sink URL carries credentials
    (mysql/postgres/clickhouse), prefer setting it via WEIR_SINK_URL rather
    than the TOML file (sink_url) so the password is not written to disk.

NOTES:
    The --sink-mysql-*, --sink-postgres-*, and --sink-clickhouse-* flags
    require the matching build feature (mysql-sink / postgres-sink /
    clickhouse-sink). On a binary built without a feature, passing its flag
    is rejected with a message naming the feature to rebuild with.
    Boolean flags accept true/false, 1/0, yes/no, or on/off.
";

pub(super) fn parse() -> Result<(PartialConfig, PathBuf), ConfigError> {
    parse_from(pico_args::Arguments::from_env())
}

pub(super) fn parse_from(
    mut pargs: pico_args::Arguments,
) -> Result<(PartialConfig, PathBuf), ConfigError> {
    if pargs.contains(["-h", "--help"]) {
        print!("{HELP}");
        std::process::exit(0);
    }

    let config_path: PathBuf = pargs
        .opt_value_from_str("--config")
        .map_err(pico_err)?
        .unwrap_or_else(|| PathBuf::from("/etc/weir/weir.toml"));

    let partial = PartialConfig {
        socket_path: pargs
            .opt_value_from_str("--socket-path")
            .map_err(pico_err)?,
        wab_dir: pargs.opt_value_from_str("--wab-dir").map_err(pico_err)?,
        shard_count: pargs
            .opt_value_from_str("--shard-count")
            .map_err(pico_err)?,
        worker_count: pargs
            .opt_value_from_str("--worker-count")
            .map_err(pico_err)?,
        batch_size: pargs.opt_value_from_str("--batch-size").map_err(pico_err)?,
        batch_deadline_ms: pargs
            .opt_value_from_str("--batch-deadline-ms")
            .map_err(pico_err)?,
        wab_segment_max_bytes: pargs
            .opt_value_from_str("--wab-segment-max-bytes")
            .map_err(pico_err)?,
        wab_segment_max_age_secs: pargs
            .opt_value_from_str("--wab-segment-max-age-secs")
            .map_err(pico_err)?,
        max_connections: pargs
            .opt_value_from_str("--max-connections")
            .map_err(pico_err)?,
        max_payload_bytes: pargs
            .opt_value_from_str("--max-payload-bytes")
            .map_err(pico_err)?,
        metrics_port: pargs
            .opt_value_from_str("--metrics-port")
            .map_err(pico_err)?,
        metrics_bind: pargs
            .opt_value_from_str("--metrics-bind")
            .map_err(pico_err)?,
        metrics_max_connections: pargs
            .opt_value_from_str("--metrics-max-connections")
            .map_err(pico_err)?,
        peer_uid_check: opt_bool(&mut pargs, "--peer-uid-check")?,
        shutdown_timeout_secs: pargs
            .opt_value_from_str("--shutdown-timeout-secs")
            .map_err(pico_err)?,
        connection_read_timeout_secs: pargs
            .opt_value_from_str("--connection-read-timeout-secs")
            .map_err(pico_err)?,
        sink_type: pargs.opt_value_from_str("--sink-type").map_err(pico_err)?,
        sink_url: pargs.opt_value_from_str("--sink-url").map_err(pico_err)?,
        sink_timeout_secs: pargs
            .opt_value_from_str("--sink-timeout-secs")
            .map_err(pico_err)?,
        sink_max_batch_size: pargs
            .opt_value_from_str("--sink-max-batch-size")
            .map_err(pico_err)?,
        sink_send_idempotency_key: opt_bool(&mut pargs, "--sink-send-idempotency-key")?,
        sink_http_concurrency: pargs
            .opt_value_from_str("--sink-http-concurrency")
            .map_err(pico_err)?,
        sink_http_batch: pargs
            .opt_value_from_str("--sink-http-batch")
            .map_err(pico_err)?,
        sink_max_retries: pargs
            .opt_value_from_str("--sink-max-retries")
            .map_err(pico_err)?,
        sink_retry_base_delay_ms: pargs
            .opt_value_from_str("--sink-retry-base-delay-ms")
            .map_err(pico_err)?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_table: pargs
            .opt_value_from_str("--sink-mysql-table")
            .map_err(pico_err)?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_column: pargs
            .opt_value_from_str("--sink-mysql-column")
            .map_err(pico_err)?,
        #[cfg(feature = "mysql-sink")]
        sink_mysql_insert_mode: pargs
            .opt_value_from_str("--sink-mysql-insert-mode")
            .map_err(pico_err)?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_table: pargs
            .opt_value_from_str("--sink-postgres-table")
            .map_err(pico_err)?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_column: pargs
            .opt_value_from_str("--sink-postgres-column")
            .map_err(pico_err)?,
        #[cfg(feature = "postgres-sink")]
        sink_postgres_insert_mode: pargs
            .opt_value_from_str("--sink-postgres-insert-mode")
            .map_err(pico_err)?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_database: pargs
            .opt_value_from_str("--sink-clickhouse-database")
            .map_err(pico_err)?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_table: pargs
            .opt_value_from_str("--sink-clickhouse-table")
            .map_err(pico_err)?,
        #[cfg(feature = "clickhouse-sink")]
        sink_clickhouse_column: pargs
            .opt_value_from_str("--sink-clickhouse-column")
            .map_err(pico_err)?,
        dead_letter_max_bytes: pargs
            .opt_value_from_str("--dead-letter-max-bytes")
            .map_err(pico_err)?,
        dead_letter_check_interval_secs: pargs
            .opt_value_from_str("--dead-letter-check-interval-secs")
            .map_err(pico_err)?,
        health_poll_interval_secs: pargs
            .opt_value_from_str("--health-poll-interval-secs")
            .map_err(pico_err)?,
        log_level: pargs.opt_value_from_str("--log-level").map_err(pico_err)?,
        tcp_bind: pargs.opt_value_from_str("--tcp-bind").map_err(pico_err)?,
        tls_cert_path: pargs.opt_value_from_str("--tls-cert").map_err(pico_err)?,
        tls_key_path: pargs.opt_value_from_str("--tls-key").map_err(pico_err)?,
        tls_client_ca_path: pargs
            .opt_value_from_str("--tls-client-ca")
            .map_err(pico_err)?,
        tls_handshake_timeout_secs: pargs
            .opt_value_from_str("--tls-handshake-timeout-secs")
            .map_err(pico_err)?,
    };

    let remaining = pargs.finish();
    if !remaining.is_empty() {
        // A flag that's only wired up under a Cargo feature lands in `remaining`
        // exactly when that feature wasn't compiled in (the matching
        // `opt_value_from_str` call is `#[cfg]`-ed out). Turn the generic
        // "unknown arguments" into a message that names the required feature so
        // an operator who copied a flag straight from --help isn't left guessing
        // (F56).
        for arg in &remaining {
            if let Some(s) = arg.to_str() {
                let name = s.split('=').next().unwrap_or(s);
                if let Some((flag, feature)) = FEATURE_GATED_FLAGS.iter().find(|(f, _)| *f == name)
                {
                    return Err(ConfigError::InvalidValue {
                        field: "<args>",
                        reason: format!(
                            "flag '{flag}' requires the '{feature}' feature, which this binary \
                             was built without (rebuild with --features {feature})"
                        ),
                    });
                }
            }
        }
        return Err(ConfigError::InvalidValue {
            field: "<args>",
            reason: format!("unknown arguments: {remaining:?}"),
        });
    }

    Ok((partial, config_path))
}

/// CLI flags that exist only when their sink feature is compiled in, mapped to
/// the feature that provides them. Used to give a precise "requires feature X"
/// error (F56) instead of the generic "unknown arguments". A flag reaches the
/// `remaining` set only on a build without its feature, so no `cfg!` guard is
/// needed here — a flag that IS compiled in is consumed by pico-args before
/// `finish()` and never matched against this table.
const FEATURE_GATED_FLAGS: &[(&str, &str)] = &[
    ("--sink-mysql-table", "mysql-sink"),
    ("--sink-mysql-column", "mysql-sink"),
    ("--sink-mysql-insert-mode", "mysql-sink"),
    ("--sink-postgres-table", "postgres-sink"),
    ("--sink-postgres-column", "postgres-sink"),
    ("--sink-postgres-insert-mode", "postgres-sink"),
    ("--sink-clickhouse-database", "clickhouse-sink"),
    ("--sink-clickhouse-table", "clickhouse-sink"),
    ("--sink-clickhouse-column", "clickhouse-sink"),
];

/// Parses a boolean flag leniently (`true/false`, `1/0`, `yes/no`, `on/off`),
/// via [`super::parse_bool`]. `opt_value_from_str::<bool>` accepted only exact
/// `true`/`false` (F57).
fn opt_bool(
    pargs: &mut pico_args::Arguments,
    flag: &'static str,
) -> Result<Option<bool>, ConfigError> {
    // Read the raw value as a String, then parse it ourselves so the flag name
    // lands in the error (opt_value_from_fn takes a non-capturing fn pointer).
    match pargs
        .opt_value_from_str::<_, String>(flag)
        .map_err(pico_err)?
    {
        None => Ok(None),
        Some(raw) => super::parse_bool(flag, &raw).map(Some),
    }
}

fn pico_err(e: pico_args::Error) -> ConfigError {
    ConfigError::ParseError {
        field: "command-line argument",
        source: Box::new(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    fn args(parts: &[&str]) -> pico_args::Arguments {
        // from_vec excludes the executable path, matching how we'd parse real argv.
        pico_args::Arguments::from_vec(parts.iter().map(OsString::from).collect())
    }

    // ── F57: lenient boolean flags ────────────────────────────────────────────

    #[test]
    fn bool_flag_accepts_zero_one_and_mixed_case() {
        for (raw, expected) in [
            ("0", false),
            ("1", true),
            ("TRUE", true),
            ("False", false),
            ("yes", true),
            ("off", false),
        ] {
            let (partial, _) = parse_from(args(&["--peer-uid-check", raw])).unwrap();
            assert_eq!(partial.peer_uid_check, Some(expected), "input {raw:?}");
        }
    }

    #[test]
    fn bool_flag_rejects_garbage_naming_the_flag() {
        // map Ok payload to () — PartialConfig isn't Debug (and shouldn't be:
        // it holds the raw sink_url), so unwrap_err can't format the Ok variant.
        let err = parse_from(args(&["--peer-uid-check", "maybe"]))
            .map(|_| ())
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("--peer-uid-check"), "{msg}");
        assert!(msg.contains("not a valid boolean"), "{msg}");
    }

    // ── F56: feature-gated flags name the required feature ─────────────────────

    /// On a build WITHOUT clickhouse-sink, `--sink-clickhouse-table` is not wired
    /// into the parser, so it lands in `remaining`. The error must name the
    /// feature to rebuild with, not the generic "unknown arguments".
    #[cfg(not(feature = "clickhouse-sink"))]
    #[test]
    fn gated_flag_without_its_feature_names_the_feature() {
        let err = parse_from(args(&["--sink-clickhouse-table", "events"]))
            .map(|_| ())
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("clickhouse-sink"), "{msg}");
        assert!(msg.contains("--sink-clickhouse-table"), "{msg}");
        assert!(!msg.contains("unknown arguments"), "{msg}");
    }

    /// A truly unknown flag still produces the generic error.
    #[test]
    fn unknown_flag_still_generic_error() {
        let err = parse_from(args(&["--definitely-not-a-flag", "x"]))
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("unknown arguments"), "{err}");
    }

    #[test]
    fn every_gated_flag_maps_to_a_real_feature_name() {
        // Drift guard: the feature names must match the ones the build actually
        // gates on, mirroring file.rs's KNOWN feature set.
        for (flag, feature) in FEATURE_GATED_FLAGS {
            assert!(
                matches!(*feature, "mysql-sink" | "postgres-sink" | "clickhouse-sink"),
                "{flag} names an unknown feature {feature}"
            );
        }
    }
}
