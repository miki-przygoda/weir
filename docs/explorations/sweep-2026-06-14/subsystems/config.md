# Config subsystem — audit verification (sweep 2026-06-14)

All eight findings were verified against the code on `v1/phase-4-cleanup`. Seven are real defects (one downgraded from high to medium); one is real but correctly info-level (32-bit-only). None were refuted.

## Confirmed (real)

### shard_count in 65..=256 with defaulted worker_count rejects an in-range config with a misleading error
- **file:** crates/weir-server/src/config/mod.rs:397
- **severity:** high
- **argument:** `shard_count` is range-checked to `[1, 256]` (mod.rs:395), `worker_count` to `[1, 64]` (mod.rs:398), and `worker_count` defaults to `shard_count` (mod.rs:397, `merge!(worker_count).unwrap_or(shard_count)`). Setting `shard_count` in `65..=256` without an explicit `worker_count` makes `worker_count` inherit that value, then `check_range("worker_count", 128, 1, 64)` fails with `worker_count 128 is out of range [1, 64]` — naming a field the operator never set, hiding the real cause (shard_count exceeds the worker cap). The `worker_count` doc (mod.rs:245-249) explicitly recommends `worker_count == shard_count` for best throughput, which is unreachable for any `shard_count` in `65..=256`.
- **verdict:** real — confirmed: mod.rs:397 `unwrap_or(shard_count)` then mod.rs:398 `check_range("worker_count", worker_count, 1, 64)`; the field named in the error is one the operator never set, and no diagnostic ties it back to shard_count.

### Config-time warn! calls are silently dropped (tracing subscriber initialized after Config::load)
- **file:** crates/weir-server/src/main.rs:185
- **severity:** medium (downgraded from medium — kept)
- **argument:** `Config::load()` runs at main.rs:185; the only subscriber init in the binary is at main.rs:187-192, *after*. Any `warn!` emitted during config loading has no subscriber and is discarded. This disables the unknown-key safety net (file.rs:191 "unknown top-level config key", file.rs:199 "unknown [server] config key") and the dead_letter_max_bytes advisory (mod.rs:596). An operator who fat-fingers a key (`shard_counts =`) silently gets the default with zero feedback.
- **verdict:** real — confirmed: `Config::load()` (main.rs:185) precedes the sole `tracing_subscriber::fmt()...init()` (main.rs:187-192); the warns in file.rs:191/199 and mod.rs:596 fire during that earlier window with no installed subscriber.

### Feature-gated [server] keys are silently dropped: in KNOWN_SERVER_KEYS but absent from RawConfig on builds without the feature
- **file:** crates/weir-server/src/config/file.rs:92
- **severity:** low
- **argument:** `KNOWN_SERVER_KEYS` (file.rs:70-109) lists `sink_mysql_*`, `sink_postgres_*`, `sink_clickhouse_*` unconditionally, but the matching `RawServer` fields are `#[cfg(feature = ...)]`-gated (file.rs:37-54), and `RawConfig`/`RawServer` carry no `#[serde(deny_unknown_fields)]` (derive is bare `#[derive(Deserialize)]`, file.rs:8/14). `clickhouse-sink` is NOT in default features (`default = ["http-sink", "mysql-sink", "postgres-sink"]`, Cargo.toml). On a default build a TOML key like `sink_clickhouse_table = "x"` is (a) not flagged by `warn_unknown_keys` because it IS in `KNOWN_SERVER_KEYS` (file.rs:198), and (b) silently dropped at deserialization because the field does not exist. The env layer is analogous: env.rs:40-45 gates the same fields under cfg, so `WEIR_SINK_CLICKHOUSE_TABLE` is silently unread on a default build.
- **verdict:** real — confirmed: `sink_clickhouse_table` is in KNOWN_SERVER_KEYS (file.rs:99) yet the RawServer field is `#[cfg(feature = "clickhouse-sink")]` (file.rs:51-52), no `deny_unknown_fields`, and clickhouse-sink is non-default per Cargo.toml.

### HELP advertises feature-gated CLI flags that fail as generic 'unknown arguments' when the feature is off
- **file:** crates/weir-server/src/config/cli.rs:51
- **severity:** low
- **argument:** The static HELP lists `--sink-mysql-*`, `--sink-postgres-*`, `--sink-clickhouse-table`/`-column` unconditionally (cli.rs:42-52), but the parsing of these flags is `#[cfg(feature = ...)]`-gated (cli.rs:146-181). With the feature off, the `opt_value_from_str` call is compiled out, the flag is never consumed, and it falls into `pargs.finish()` remaining args, producing the generic `unknown arguments: ...` error (cli.rs:200-206) rather than a "requires --features X" message. Only `--sink-clickhouse-database` (cli.rs:49-50) carries the feature caveat; `--sink-clickhouse-table`/`-column` (cli.rs:51-52) do not, despite being equally gated. `SinkType::parse` gives a precise feature-missing error for `--sink-type` (mod.rs:93-126), but these per-sink option flags do not.
- **verdict:** real — confirmed: cli.rs:51-52 list the flags with no caveat while cli.rs:170-181 gate their parsing under `clickhouse-sink`; unconsumed flags hit the generic error at cli.rs:200-206.

### Boolean env/CLI values accept only exactly 'true'/'false'; '1', '0', 'TRUE' abort startup
- **file:** crates/weir-server/src/config/env.rs:19
- **severity:** low
- **argument:** `peer_uid_check` and `sink_send_idempotency_key` are parsed via `env_parse` (env.rs:19, 26) and `opt_value_from_str` (cli.rs:123-125, 140-142). `env_parse` uses `T::from_str` (env.rs:79-80) and pico_args `opt_value_from_str` uses `FromStr` too; `bool::from_str` accepts ONLY `true`/`false` (`"1"`, `"0"`, `"TRUE"`, `"yes"`, `""` all yield `ParseBoolError`). So `WEIR_PEER_UID_CHECK=1` or `--peer-uid-check 0` aborts startup with a parse error instead of toggling. `peer_uid_check` is a security-relevant default-on toggle (mod.rs:467 `unwrap_or(true)`); the help text shows only `<bool>` (cli.rs:30) without naming accepted literals. Fails safe (hard error, not silent), but is a real footgun.
- **verdict:** real — confirmed: both paths funnel through `FromStr for bool` (env.rs:79-80 `v.parse::<T>()`, cli.rs:123 `opt_value_from_str`), whose only accepted inputs are the literals `true`/`false`.

### log_level is never validated; invalid or empty values silently degrade or disable logging
- **file:** crates/weir-server/src/config/mod.rs:611
- **severity:** low
- **argument:** `log_level` is passed through unvalidated (mod.rs:611 `merge!(log_level).unwrap_or_else(|| "info".into())`). main.rs:188-191 does `EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info"))`. A typo'd non-empty level (`WEIR_LOG_LEVEL=inof`) silently falls back to info. More dangerously, an empty value (`WEIR_LOG_LEVEL=""` → `env_string` returns `Some("")` at env.rs:48/57-60, which wins via merge over the default) makes `EnvFilter::try_new("")` succeed and produce a directive-less filter — logging effectively OFF — again with no diagnostic. The config layer could reject obviously-invalid levels at load instead of deferring to a silent runtime fallback.
- **verdict:** real — confirmed: mod.rs:611 applies no validation; the empty-string env path (env.rs:57-60 returns `Some("")`) wins the merge and reaches `EnvFilter::try_new` at main.rs:188-191 with the documented silent-fallback / off behavior.

### Config derives Debug, exposing sink_url credentials, inconsistent with deliberate bearer_token redaction
- **file:** crates/weir-server/src/config/mod.rs:236
- **severity:** low
- **argument:** `Config` derives `Debug` (mod.rs:236) and includes `sink_url: Option<String>` (mod.rs:295), which by design holds DSNs like `mysql://user:password@host/db` (help cli.rs:67-70 notes the URL "contains credentials"). The sink layer wrote manual redacting `Debug` impls specifically because a `?config` log line could leak the password (http.rs:78-92 redacts `bearer_token`; sql_common.rs:90-91, mysql.rs:86, postgres.rs:102 redact the password in `sink_url`). `Config` gets a naive derive that would print the whole `sink_url`. It is not currently logged whole — grep shows only field-level logging (`?config.sink_mysql_insert_mode` at main.rs:346, `?config.sink_postgres_insert_mode` at main.rs:371) — so this is latent, but any future `debug!(?config)` or panic-on-Config would leak DB credentials.
- **verdict:** real (latent) — confirmed: mod.rs:236 `#[derive(Debug)]` over `sink_url` (mod.rs:295) contradicts the deliberate redaction the sink layer adopted for the exact same DSN (sql_common.rs:90-91, mysql.rs:86, postgres.rs:102); no current whole-Config log exists, so the risk is latent not active.

### u64 durations range-checked via `as usize` truncate on 32-bit targets, letting out-of-range values pass
- **file:** crates/weir-server/src/config/mod.rs:406
- **severity:** info
- **argument:** `batch_deadline_ms` (mod.rs:406), `connection_read_timeout_secs` (mod.rs:485), `sink_timeout_secs` (mod.rs:530), and `dead_letter_check_interval_secs` (mod.rs:606) are `u64` but are range-checked via `value as usize`. On a 32-bit target (`usize = 32-bit`), a `u64` above `u32::MAX` wraps before the bound check: e.g. `batch_deadline_ms = 2^32 + 1` truncates to 1, passes `check_range(.., 1, 60_000)`, and is then stored/used as the full `u64`. The validated range and the runtime value disagree. Harmless on the 64-bit servers weir targets (lossless cast), hence info-level; the clean fix is to range-check the `u64` directly.
- **verdict:** real (info) — confirmed: all four sites cast `u64 as usize` before `check_range` (mod.rs:406, 485, 530, 606); truncation is reachable only on 32-bit `usize`, and weir targets 64-bit servers, so severity info is correct.

## Refuted / dismissed

None. All eight findings hold as described.
