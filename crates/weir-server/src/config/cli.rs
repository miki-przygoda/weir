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
    --worker-count <n>                       Worker thread count (1-64) [default: 2]
    --batch-size <n>                         Records per flush batch (1-100000) [default: 256]
    --batch-deadline-ms <n>                  Batch accumulation time ms (1-60000) [default: 1]
    --max-connections <n>                    Connection cap (1-512) [default: 256]
    --max-payload-bytes <n>                  Payload cap in bytes [default: 16777216]
    --connection-read-timeout-secs <n>       Slowloris guard (1-600) [default: 30]
    --metrics-port <port>                    Prometheus metrics port [default: 9185]
    --shutdown-timeout-secs <secs>           Graceful shutdown timeout (1+) [default: 30]
    --sink-type <type>                       Sink: noop | http [default: noop]
    --sink-url <url>                         Sink URL (required if type=http)
    --sink-timeout-secs <secs>               Per-request sink timeout (1-300) [default: 10]
    --sink-max-batch-size <n>                Sink commit batch cap (1-10000) [default: 100]
    --sink-send-idempotency-key <bool>       Send Idempotency-Key header (http) [default: true]
    --dead-letter-max-bytes <n>              Dead-letter dir size cap [default: 1073741824]
    --dead-letter-check-interval-secs <n>    Blocked-state wake interval (1-3600) [default: 30]
    --log-level <level>                      Log level (trace/debug/info/warn/error) [default: info]
    -h, --help                               Print this help and exit

ENVIRONMENT:
    Every option above can be set via WEIR_<UPPER_SNAKE_NAME>.
    WEIR_SINK_BEARER_TOKEN is env-only (never sourced from --sink-* flags
    or the config file).
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
        max_connections: pargs
            .opt_value_from_str("--max-connections")
            .map_err(pico_err)?,
        max_payload_bytes: pargs
            .opt_value_from_str("--max-payload-bytes")
            .map_err(pico_err)?,
        metrics_port: pargs
            .opt_value_from_str("--metrics-port")
            .map_err(pico_err)?,
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
        sink_send_idempotency_key: pargs
            .opt_value_from_str("--sink-send-idempotency-key")
            .map_err(pico_err)?,
        dead_letter_max_bytes: pargs
            .opt_value_from_str("--dead-letter-max-bytes")
            .map_err(pico_err)?,
        dead_letter_check_interval_secs: pargs
            .opt_value_from_str("--dead-letter-check-interval-secs")
            .map_err(pico_err)?,
        log_level: pargs.opt_value_from_str("--log-level").map_err(pico_err)?,
    };

    let remaining = pargs.finish();
    if !remaining.is_empty() {
        return Err(ConfigError::InvalidValue {
            field: "<args>",
            reason: format!("unknown arguments: {remaining:?}"),
        });
    }

    Ok((partial, config_path))
}

fn pico_err(e: pico_args::Error) -> ConfigError {
    ConfigError::ParseError {
        field: "command-line argument",
        source: Box::new(e),
    }
}
