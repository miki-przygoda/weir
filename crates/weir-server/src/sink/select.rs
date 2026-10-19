//! Sink selection (spec §5.12): config → a built sink → a [`DrainSpawner`].
//!
//! A mechanical move out of `main`. The sink is still built after recovery.
//! This is the SP-M (sink hot-swap) seam: a spawner is a value, so a later
//! sub-project can build a second one, stop the old drain and hand it the same
//! Receiver. Invariant (spec §18): the drain Receiver is never cloned; it moves
//! through `DrainSpawnArgs` into exactly one drain.

use std::{path::PathBuf, sync::Arc};

// Only the feature-gated sink arms use these; a noop-only build
// (`--no-default-features`) would otherwise fail clippy on unused imports.
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink",
    feature = "s3-sink"
))]
use std::time::Duration;
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink",
    feature = "s3-sink"
))]
use tracing::info;
use tracing::warn;

use crate::config::{Config, SinkType};
use crate::drain::{self, DrainConfig, DrainHeartbeat};
use crate::metrics::Metrics;
use crate::sink::Sink;
#[cfg(feature = "clickhouse-sink")]
use crate::sink::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
#[cfg(feature = "http-sink")]
use crate::sink::http::{HttpBatchMode, HttpSink, HttpSinkConfig};
#[cfg(feature = "mysql-sink")]
use crate::sink::mysql::{MySqlSink, MySqlSinkConfig};
use crate::sink::noop::NoopSink;
#[cfg(feature = "postgres-sink")]
use crate::sink::postgres::{PostgresSink, PostgresSinkConfig};

/// Everything a drain needs except the sink.
pub(crate) struct DrainSpawnArgs {
    pub(crate) rx: crossbeam_channel::Receiver<PathBuf>,
    pub(crate) config: DrainConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) heartbeat: DrainHeartbeat,
    /// PR 2's control, board and startup backlog. Production passes
    /// `DrainControl::attached()` (PR 4).
    pub(crate) runtime: drain::DrainRuntime,
}

type SpawnFn = Box<dyn FnOnce(DrainSpawnArgs) -> std::thread::JoinHandle<drain::DrainExit> + Send>;

/// A built sink, type-erased: `Sink` has an associated error type and async
/// methods, so it is not object-safe; the closure captures the concrete type.
/// `drain::spawn_with` is generic over the sink but returns the same
/// `JoinHandle<drain::DrainExit>` for every sink, so the join sequence in
/// `main` sees one handle type.
pub(crate) struct DrainSpawner {
    sink_type: SinkType,
    spawn: SpawnFn,
}

impl DrainSpawner {
    pub(crate) fn from_sink<S: Sink + 'static>(sink_type: SinkType, sink: S) -> Self {
        let sink = Arc::new(sink);
        DrainSpawner {
            sink_type,
            spawn: Box::new(move |a: DrainSpawnArgs| {
                drain::spawn_with(a.rx, sink, a.config, a.metrics, a.heartbeat, a.runtime)
            }),
        }
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "read by SP-M's hot-swap; pinned by the unit test")
    )]
    pub(crate) fn sink_type(&self) -> SinkType {
        self.sink_type
    }

    /// Spawns the drain thread. Consumes the spawner: one sink, one drain.
    pub(crate) fn spawn(self, args: DrainSpawnArgs) -> std::thread::JoinHandle<drain::DrainExit> {
        (self.spawn)(args)
    }
}

/// Returns the configured sink URL, or a clear startup error if it's absent.
/// Config validation already guarantees it's set for URL-requiring sinks, so
/// this is belt-and-suspenders — but a `Result` beats a panic on the startup
/// path, and one message replaces five near-identical `.expect` strings.
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink"
))]
fn require_sink_url(
    config: &Config,
    sink_label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    config.sink_url.0.clone().ok_or_else(|| {
        format!("sink_type = {sink_label} requires a sink URL (set --sink-url or WEIR_SINK_URL)")
            .into()
    })
}

/// Builds the configured sink. Every arm's log lines, warnings and error
/// texts are the ones `main` had; only the tail changes, from spawning the
/// drain to returning a [`DrainSpawner`] that `main` spawns.
pub(crate) fn build_drain_spawner(
    config: &Config,
    _metrics: &Arc<Metrics>,
) -> Result<DrainSpawner, Box<dyn std::error::Error>> {
    match config.sink_type {
        SinkType::Noop => {
            warn!(
                "sink: noop — records are acked and DISCARDED, not forwarded anywhere. \
                 This is for soak-testing the pipeline; set --sink-type to deliver downstream."
            );
            Ok(DrainSpawner::from_sink(SinkType::Noop, NoopSink))
        }
        #[cfg(feature = "http-sink")]
        SinkType::Http => {
            let url = require_sink_url(config, "http")?;
            // Bearer token read from env at startup (never from config file).
            // Logged only as a presence boolean — the token itself never reaches
            // a log line.
            let bearer_token = std::env::var("WEIR_SINK_BEARER_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(Arc::from);
            // One-shot startup hint: with no token set the sink POSTs
            // unauthenticated, and the first symptom is otherwise a flood of 401
            // dead-letters at drain time. Gated to the http arm, so it only fires
            // when the http sink is actually selected.
            if bearer_token.is_none() {
                warn!(
                    "sink: http — WEIR_SINK_BEARER_TOKEN is unset; requests will be sent \
                     UNAUTHENTICATED (no Authorization header). If the endpoint requires auth, \
                     set WEIR_SINK_BEARER_TOKEN or expect 401s to dead-letter at drain time."
                );
            }
            // Validated to "none"|"ndjson" at config load; "ndjson" sends the
            // whole batch as one newline-delimited POST, anything else (i.e.
            // "none") keeps the per-record default.
            let batch_mode = if config.sink_http_batch == "ndjson" {
                HttpBatchMode::Ndjson
            } else {
                HttpBatchMode::PerRecord
            };
            // sink_http_concurrency only applies to per-record POSTs; in ndjson
            // mode the whole batch is one POST, so report it as inert rather than
            // logging a misleading "concurrency = 8".
            let concurrency_display = match batch_mode {
                HttpBatchMode::Ndjson => "n/a (ndjson)".to_string(),
                HttpBatchMode::PerRecord => config.sink_http_concurrency.to_string(),
            };
            info!(
                // The URL may carry userinfo (user:password@host); redact the
                // password so it never reaches a log line (S25).
                url = %crate::sink::redact_url_password(&url),
                bearer = bearer_token.is_some(),
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                concurrency = %concurrency_display,
                batch = %config.sink_http_batch,
                "sink: http"
            );
            let http_cfg = HttpSinkConfig {
                url,
                timeout: Duration::from_secs(config.sink_timeout_secs),
                max_batch_size: config.sink_max_batch_size,
                batch_mode,
                bearer_token,
                send_idempotency_key: config.sink_send_idempotency_key,
                concurrency: config.sink_http_concurrency,
            };
            let sink = HttpSink::new(http_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build HTTP sink: {e}"))
            })?;
            Ok(DrainSpawner::from_sink(SinkType::Http, sink))
        }
        #[cfg(feature = "mysql-sink")]
        SinkType::Mysql => {
            let url = require_sink_url(config, "mysql")?;
            let insert_mode = config.sink_mysql_insert_mode;
            info!(
                // URL omitted from the log line — it contains credentials.
                // The MySqlSink Debug impl redacts the password, but logging
                // even the user component here gives operators no information
                // they can't get from `weir-server --help` plus their config.
                table = %config.sink_mysql_table,
                column = %config.sink_mysql_column,
                // Logged because its absence is the interesting state: without
                // it the operator's UNIQUE constraint is on payload bytes, and
                // that discards distinct records that share bytes.
                id_column = ?config.sink_mysql_id_column,
                insert_mode = ?config.sink_mysql_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: mysql"
            );
            let mysql_cfg = MySqlSinkConfig {
                url,
                table: config.sink_mysql_table.clone(),
                column: config.sink_mysql_column.clone(),
                id_column: config.sink_mysql_id_column.clone(),
                insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = MySqlSink::new(mysql_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build MySQL sink: {e}"))
            })?;
            Ok(DrainSpawner::from_sink(SinkType::Mysql, sink))
        }
        #[cfg(feature = "postgres-sink")]
        SinkType::Postgres => {
            let url = require_sink_url(config, "postgres")?;
            info!(
                // URL omitted from the log line for the same reason as MySQL.
                table = %config.sink_postgres_table,
                column = %config.sink_postgres_column,
                // See the MySQL arm: absence is the interesting state.
                id_column = ?config.sink_postgres_id_column,
                insert_mode = ?config.sink_postgres_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: postgres"
            );
            let pg_cfg = PostgresSinkConfig {
                url,
                table: config.sink_postgres_table.clone(),
                column: config.sink_postgres_column.clone(),
                id_column: config.sink_postgres_id_column.clone(),
                insert_mode: config.sink_postgres_insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = PostgresSink::new(pg_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build Postgres sink: {e}"))
            })?;
            Ok(DrainSpawner::from_sink(SinkType::Postgres, sink))
        }
        #[cfg(feature = "clickhouse-sink")]
        SinkType::ClickHouse => {
            let url = require_sink_url(config, "clickhouse")?;
            info!(
                // URL omitted from the log line — it may carry credentials.
                database = %config.sink_clickhouse_database,
                table = %config.sink_clickhouse_table,
                column = %config.sink_clickhouse_column,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: clickhouse"
            );
            let ch_cfg = ClickHouseSinkConfig {
                url,
                database: config.sink_clickhouse_database.clone(),
                table: config.sink_clickhouse_table.clone(),
                column: config.sink_clickhouse_column.clone(),
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = ClickHouseSink::new(ch_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build ClickHouse sink: {e}"))
            })?;
            Ok(DrainSpawner::from_sink(SinkType::ClickHouse, sink))
        }
        #[cfg(feature = "s3-sink")]
        SinkType::S3 => {
            if config.sink_s3_bucket.is_empty() {
                return Err(Box::<dyn std::error::Error>::from(
                    "sink_type = \"s3\" requires sink_s3_bucket",
                ));
            }
            // One object per commit batch. At the default sink_max_batch_size of
            // 100 that is roughly a 50 KB object for a 500-byte record --
            // hundreds of thousands per hour for a busy producer, which is
            // expensive to PUT and pathological for every query engine.
            if config.sink_max_batch_size < 1000 {
                warn!(
                    sink_max_batch_size = config.sink_max_batch_size,
                    "s3 sink: small batches produce many small objects; consider \
                     sink_max_batch_size = 10000. NOTE: this value determines batch \
                     boundaries, and batch boundaries determine object keys -- changing it \
                     after records have been written makes replayed batches land under new \
                     keys and duplicate. Choose it once."
                );
            }
            info!(
                bucket = %config.sink_s3_bucket,
                region = %config.sink_s3_region,
                // Endpoint omitted: it is operator-supplied and may carry a host
                // an operator would rather not see echoed. Bucket + region are
                // enough to identify the target.
                path_style = config.sink_s3_force_path_style,
                prefix = %config.sink_s3_prefix,
                partition = %config.sink_s3_partition,
                framing = %config.sink_s3_framing,
                compression = %config.sink_s3_compression,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: s3"
            );
            let opt = |v: &str| (!v.is_empty()).then(|| v.to_string());
            let s3_cfg = weir_sink_s3::S3SinkConfig {
                endpoint: opt(&config.sink_s3_endpoint),
                bucket: config.sink_s3_bucket.clone(),
                region: config.sink_s3_region.clone(),
                force_path_style: config.sink_s3_force_path_style,
                prefix: config.sink_s3_prefix.clone(),
                partition: config.sink_s3_partition.clone(),
                framing: weir_sink_s3::Framing::parse(&config.sink_s3_framing)
                    .expect("sink_s3_framing validated at config parse"),
                compression: weir_sink_s3::Compression::parse(&config.sink_s3_compression)
                    .expect("sink_s3_compression validated at config parse"),
                access_key_id: opt(&config.sink_s3_access_key_id),
                secret_access_key: opt(&config.sink_s3_secret_access_key),
                storage_class: opt(&config.sink_s3_storage_class),
                sse: opt(&config.sink_s3_sse),
                sse_kms_key_id: opt(&config.sink_s3_sse_kms_key_id),
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = weir_sink_s3::S3Sink::new(s3_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build S3 sink: {e}"))
            })?;
            Ok(DrainSpawner::from_sink(SinkType::S3, sink))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PartialConfig;
    use std::time::{Duration, Instant};

    fn noop_config(label: &str) -> Config {
        let dir = crate::testutil::scratch_dir(&format!("select_{label}"));
        crate::testutil::mkdir_p(&dir);
        Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir),
                sink_type: Some("noop".to_string()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .map_err(|e| e.to_string())
        .unwrap()
    }

    fn drain_config(wab_dir: PathBuf) -> DrainConfig {
        DrainConfig {
            wab_dir,
            dead_letter_max_bytes: 1 << 20,
            dead_letter_check_interval: Duration::from_secs(1),
            base_retry_delay: Duration::from_millis(10),
            max_retries: 1,
            max_respawns: 1,
            commit_timeout: Duration::from_secs(60),
            health_poll_interval: Duration::from_secs(1),
            health_probe_timeout: Duration::from_secs(1),
            orphan_cleanup: false,
            local_read_escalation_min_age: drain::LOCAL_READ_ESCALATION_MIN_AGE,
        }
    }

    #[test]
    fn the_noop_spawner_names_its_sink_and_runs_a_drain_that_exits_on_disconnect() {
        let config = noop_config("noop");
        let metrics = Arc::new(Metrics::new().0);
        let spawner = build_drain_spawner(&config, &metrics).unwrap();
        assert_eq!(spawner.sink_type(), SinkType::Noop);

        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(crate::wab::DRAIN_CHANNEL_CAPACITY);
        let handle = spawner.spawn(DrainSpawnArgs {
            rx,
            config: drain_config(config.wab_dir.clone()),
            metrics,
            heartbeat: DrainHeartbeat::new(),
            runtime: drain::DrainRuntime::detached(),
        });
        drop(tx); // detached drain: Disconnected with nothing pending = exit
        let deadline = Instant::now() + Duration::from_secs(5);
        while !handle.is_finished() {
            assert!(
                Instant::now() < deadline,
                "the drain did not exit after its channel closed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let exit = handle.join().unwrap();
        assert_eq!(
            exit.reason,
            crate::status::DrainExitReason::Stopped,
            "{exit:?}"
        );
    }
}
