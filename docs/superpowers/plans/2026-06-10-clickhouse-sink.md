# ClickHouse Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a ClickHouse drain sink (HTTP/RowBinary, sha256 dedup token) behind a default-off `clickhouse-sink` feature, reusing `sql_common`.

**Architecture:** `sink/clickhouse.rs` implements the existing `Sink` trait. Each `commit(batch)` POSTs one `INSERT … FORMAT RowBinary` request to ClickHouse's HTTP interface via `reqwest`, with the batch encoded as length-prefixed bytes into a single `String` column and a content-derived `insert_deduplication_token`. It reuses `sql_common`'s identifier validation, password redaction, and `SqlSinkError`. It mirrors `sink/postgres.rs` structurally.

**Tech Stack:** Rust 2024, tokio, reqwest (HTTP), sha2 (dedup token), the existing `sql_common` infra.

**Reference spec:** `docs/superpowers/specs/2026-06-10-clickhouse-sink-design.md`
**Branch:** `v1/phase-2-clickhouse` (local; commit per task, **do not push, do not open PRs**).
**Template to read first:** `crates/weir-server/src/sink/postgres.rs` (config + Debug redaction + build-error + `new` + `Sink` impl + tests) and `crates/weir-server/src/sink/sql_common.rs` (`validate_identifier`, `redact_password`, `SqlSinkError::{transient,permanent,timeout}`, `InvalidIdentifier`).

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `crates/weir-server/Cargo.toml` | Modify | Add `clickhouse-sink` feature |
| `crates/weir-server/src/sink/mod.rs` | Modify | `#[cfg(feature="clickhouse-sink")] pub mod clickhouse;` + doc-list entry |
| `crates/weir-server/src/sink/clickhouse.rs` | Create | The sink: RowBinary encoder, dedup token, config, build-error, `new`, `Sink` impl, unit tests |
| `crates/weir-server/src/config/mod.rs` | Modify | `SinkType::ClickHouse` + parse + validation + `sink_clickhouse_*` fields |
| `crates/weir-server/src/config/{cli,env,file}.rs` | Modify | Parse the 3 new keys |
| `crates/weir-server/src/main.rs` | Modify | `SinkType::ClickHouse` dispatch arm |
| `crates/weir-server/tests/system.rs` | Modify | `#[ignore]` `clickhouse_sink_end_to_end` |
| `deploy/docker/test/docker-compose.yml` | Modify | ClickHouse service |
| `deploy/docker/test/init-clickhouse.sql` | Create | Seed table (dedup-capable engine) |
| `deploy/run-sink-integration-tests.sh` | Modify | Export `WEIR_TEST_CLICKHOUSE_URL` |
| `docs/operations/configuration.md`, `docs/testing/sink-integration.md`, `README.md`, `CHANGELOG.md`, root `Cargo.toml` | Modify | Docs + version 0.7.0 |

---

## Task 1: Feature flag + module skeleton

**Files:** Modify `crates/weir-server/Cargo.toml`, `crates/weir-server/src/sink/mod.rs`; Create `crates/weir-server/src/sink/clickhouse.rs`.

- [ ] **Step 1: Add the feature** — in `[features]`:
```toml
# ClickHouse sink: HTTP RowBinary inserts with a content-derived dedup token.
clickhouse-sink = ["dep:reqwest", "dep:sha2", "_sql-sink"]
```
(`reqwest`/`sha2` are already optional from Phase 1; this just also enables them. `_sql-sink` pulls in `sql_common`.) Leave `default` unchanged (opt-in).

- [ ] **Step 2: Register the module** — in `sink/mod.rs`, beside the other gated sink decls:
```rust
#[cfg(feature = "clickhouse-sink")]
pub mod clickhouse;
```
Add a bullet to the module-doc sink list: `//! - [\`clickhouse::ClickHouseSink\`] — HTTP RowBinary batch inserts; sha256 insert_deduplication_token for replay safety. Use when \`sink_type = "clickhouse"\`.`

- [ ] **Step 3: Create the stub** `crates/weir-server/src/sink/clickhouse.rs`:
```rust
//! ClickHouse sink — HTTP `INSERT … FORMAT RowBinary` with a content-derived
//! `insert_deduplication_token`. Reuses `sql_common` (identifier validation,
//! password redaction, `SqlSinkError`). Structurally mirrors `postgres.rs`.

use std::time::Duration;

use weir_core::Payload;

use super::sql_common;
use super::{CommitResult, Sink, SinkHealth};
```

- [ ] **Step 4: Verify it compiles** — `cargo build -p weir-server --no-default-features --features clickhouse-sink` and `cargo build -p weir-server` (default unchanged). Expected: both Finished. (Unused-import warnings on the stub are fine for now; the next tasks use them. If clippy `-D warnings` is run, expect it to fail until Task 5 — don't add `#[allow]`.)

- [ ] **Step 5: Commit** `git add -A && git commit -m "build(server): clickhouse-sink feature + module skeleton"`

---

## Task 2: RowBinary encoder (TDD)

**Files:** `crates/weir-server/src/sink/clickhouse.rs` (impl + `#[cfg(test)] mod tests`).

- [ ] **Step 1: Failing test** — add the test module + tests:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rowbinary_empty_batch_is_empty() {
        assert!(encode_rowbinary(&[]).is_empty());
    }

    #[test]
    fn rowbinary_single_short_payload() {
        // RowBinary String = unsigned LEB128 length, then the bytes.
        let out = encode_rowbinary(&[b"ab".to_vec()]);
        assert_eq!(out, vec![0x02, b'a', b'b']);
    }

    #[test]
    fn rowbinary_multi_payload_concatenates_rows() {
        let out = encode_rowbinary(&[b"a".to_vec(), b"bc".to_vec()]);
        assert_eq!(out, vec![0x01, b'a', 0x02, b'b', b'c']);
    }

    #[test]
    fn rowbinary_len_uses_multibyte_leb128_past_127() {
        // 200 = 0xC8 → LEB128 [0xC8, 0x01].
        let payload = vec![0u8; 200];
        let out = encode_rowbinary(&[payload]);
        assert_eq!(&out[..2], &[0xC8, 0x01]);
        assert_eq!(out.len(), 2 + 200);
    }

    #[test]
    fn rowbinary_handles_empty_payload() {
        // A zero-length payload is a valid String row: just the 0 length byte.
        assert_eq!(encode_rowbinary(&[Vec::new()]), vec![0x00]);
    }
}
```

- [ ] **Step 2: Run → fail** — `cargo test -p weir-server --no-default-features --features clickhouse-sink --lib sink::clickhouse` → FAIL (`encode_rowbinary` not found).

- [ ] **Step 3: Implement**:
```rust
/// Append an unsigned LEB128 varint (ClickHouse RowBinary string-length prefix).
fn write_leb128(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// Encode a batch as ClickHouse RowBinary for a single `String` column:
/// each payload is `leb128(len) ++ bytes`. Binary-safe (no escaping).
fn encode_rowbinary(batch: &[Payload]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in batch {
        write_leb128(&mut out, p.len() as u64);
        out.extend_from_slice(p);
    }
    out
}
```

- [ ] **Step 4: Run → pass** — same command → 5 tests PASS.

- [ ] **Step 5: Commit** `git commit -am "feat(clickhouse): RowBinary single-String-column encoder"`

---

## Task 3: Dedup token (TDD)

**Files:** `crates/weir-server/src/sink/clickhouse.rs`.

- [ ] **Step 1: Failing tests** — add to the test module:
```rust
    #[test]
    fn dedup_token_is_deterministic() {
        let b = vec![b"x".to_vec(), b"yy".to_vec()];
        assert_eq!(dedup_token(&b), dedup_token(&b));
    }

    #[test]
    fn dedup_token_changes_on_reorder() {
        let a = vec![b"x".to_vec(), b"yy".to_vec()];
        let b = vec![b"yy".to_vec(), b"x".to_vec()];
        assert_ne!(dedup_token(&a), dedup_token(&b));
    }

    #[test]
    fn dedup_token_is_64_hex_chars() {
        let t = dedup_token(&[b"hello".to_vec()]);
        assert_eq!(t.len(), 64);
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
    }
```

- [ ] **Step 2: Run → fail** — `dedup_token` not found.

- [ ] **Step 3: Implement** (sha256 of the concatenated payloads, lower-hex):
```rust
/// Content-derived dedup token: `sha256(payload₀ ++ payload₁ ++ …)`, lower-hex.
/// A crash-replayed byte-identical batch produces the same token, so a
/// Replicated*MergeTree engine deduplicates the re-inserted block.
fn dedup_token(batch: &[Payload]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for p in batch {
        hasher.update(p);
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
```

- [ ] **Step 4: Run → pass** — 3 tests PASS.

- [ ] **Step 5: Commit** `git commit -am "feat(clickhouse): sha256 content-derived dedup token"`

---

## Task 4: Config struct, build-error, and `new` (TDD)

**Files:** `crates/weir-server/src/sink/clickhouse.rs`. **Read `postgres.rs` lines ~104-167 for the exact mirror.**

- [ ] **Step 1: Failing tests**:
```rust
    fn cfg(url: &str, table: &str, column: &str) -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            url: url.to_string(),
            database: "default".to_string(),
            table: table.to_string(),
            column: column.to_string(),
            max_batch_size: 1000,
            timeout: std::time::Duration::from_secs(30),
        }
    }

    #[test]
    fn new_rejects_empty_url() {
        let e = ClickHouseSink::new(cfg("", "t", "payload")).unwrap_err();
        assert!(matches!(e, ClickHouseSinkBuildError::EmptyUrl));
    }

    #[test]
    fn new_rejects_bad_table_identifier() {
        let e = ClickHouseSink::new(cfg("http://h:8123", "bad-table; DROP", "payload")).unwrap_err();
        assert!(matches!(e, ClickHouseSinkBuildError::InvalidIdentifier { .. }));
    }

    #[test]
    fn new_accepts_valid_config() {
        assert!(ClickHouseSink::new(cfg("http://h:8123", "weir_records", "payload")).is_ok());
    }

    #[test]
    fn debug_redacts_url_password() {
        let s = ClickHouseSink::new(cfg("http://user:secret@h:8123", "t", "payload")).unwrap();
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("secret"), "password leaked: {dbg}");
        assert!(dbg.contains("<redacted>"));
    }
```

- [ ] **Step 2: Run → fail** — types not defined.

- [ ] **Step 3: Implement** the config, build-error, sink struct, Debug redaction, and `new` (mirroring postgres). Use `IDENTIFIER_MAX_LEN = 63` for ClickHouse (its identifier limit is generous; 63 is a safe conservative bound shared with PG). `new` validates database/table/column and builds a `reqwest::Client` with the timeout:
```rust
/// ClickHouse identifier length cap (conservative; matches the PG bound).
const IDENTIFIER_MAX_LEN: usize = 63;

pub struct ClickHouseSinkConfig {
    /// `http://[user:password@]host:8123`. Required.
    pub url: String,
    pub database: String,
    pub table: String,
    pub column: String,
    pub max_batch_size: usize,
    pub timeout: Duration,
}

impl std::fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSinkConfig")
            .field("url", &sql_common::redact_password(&self.url))
            .field("database", &self.database)
            .field("table", &self.table)
            .field("column", &self.column)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseSinkBuildError {
    #[error("clickhouse sink: url is empty")]
    EmptyUrl,
    #[error("clickhouse sink: invalid {field} identifier (must be [A-Za-z_][A-Za-z0-9_]{{0,62}})")]
    InvalidIdentifier { field: String },
    #[error("clickhouse sink: building HTTP client: {0}")]
    Client(String),
}

impl From<sql_common::InvalidIdentifier> for ClickHouseSinkBuildError {
    fn from(e: sql_common::InvalidIdentifier) -> Self {
        ClickHouseSinkBuildError::InvalidIdentifier { field: e.field }
    }
}

pub struct ClickHouseSink {
    config: ClickHouseSinkConfig,
    client: reqwest::Client,
    /// Base URL with any userinfo stripped; credentials applied per-request.
    base_url: String,
    credentials: Option<(String, String)>,
}

impl std::fmt::Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink").field("config", &self.config).finish()
    }
}

impl ClickHouseSink {
    pub fn new(config: ClickHouseSinkConfig) -> Result<Self, ClickHouseSinkBuildError> {
        if config.url.is_empty() {
            return Err(ClickHouseSinkBuildError::EmptyUrl);
        }
        sql_common::validate_identifier("database", &config.database, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("table", &config.table, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("column", &config.column, IDENTIFIER_MAX_LEN)?;

        // Split optional `user:password@` out of the URL so it never appears
        // in the request URL (sent as HTTP basic auth instead).
        let (base_url, credentials) = split_credentials(&config.url);

        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| ClickHouseSinkBuildError::Client(e.to_string()))?;
        Ok(Self { config, client, base_url, credentials })
    }
}

/// Returns `(base_url_without_userinfo, Some((user, password)))` if the URL has
/// a `user:password@` component, else `(url, None)`.
fn split_credentials(url: &str) -> (String, Option<(String, String)>) {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            let creds = &rest[..at];
            if let Some(colon) = creds.find(':') {
                let user = creds[..colon].to_string();
                let pass = creds[colon + 1..].to_string();
                let base = format!("{}://{}", &url[..scheme_end], &rest[at + 1..]);
                return (base, Some((user, pass)));
            }
        }
    }
    (url.to_string(), None)
}
```
> NOTE: confirm `super::sql_common::validate_identifier`/`redact_password`/`InvalidIdentifier` are reachable — they're `pub(super)` in `sql_common`, and `clickhouse.rs` is a sibling module under `sink`, so `super::sql_common::…` works (same as `postgres.rs` uses `sql_common::…`). Use whichever path matches postgres.rs's imports.

- [ ] **Step 4: Run → pass** — 4 tests PASS.

- [ ] **Step 5: Commit** `git commit -am "feat(clickhouse): config, build-error, new() with identifier validation"`

---

## Task 5: `Sink` impl — commit / max_batch_size / health (TDD where unit-testable)

**Files:** `crates/weir-server/src/sink/clickhouse.rs`.

- [ ] **Step 1: Failing test for the URL/query builder** — extract the query-string build into a testable helper and test it (the HTTP round-trip itself is covered by the integration test):
```rust
    #[test]
    fn insert_query_is_well_formed() {
        let s = ClickHouseSink::new(cfg("http://h:8123", "weir_records", "payload")).unwrap();
        let q = s.insert_query();
        assert_eq!(q, "INSERT INTO default.weir_records (payload) FORMAT RowBinary");
    }
```

- [ ] **Step 2: Run → fail** — `insert_query` not found.

- [ ] **Step 3: Implement** the helper + the `Sink` impl. The error mapping: timeout→`timeout`, network/connect→`transient`, 5xx→`transient`, 4xx→`permanent`:
```rust
impl ClickHouseSink {
    fn insert_query(&self) -> String {
        format!(
            "INSERT INTO {}.{} ({}) FORMAT RowBinary",
            self.config.database, self.config.table, self.config.column
        )
    }
}

impl Sink for ClickHouseSink {
    type Record = Payload;
    type Error = sql_common::SqlSinkError;

    async fn commit(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, sql_common::SqlSinkError> {
        if batch.is_empty() {
            return Ok(CommitResult { committed: Vec::new(), dead_lettered: Vec::new() });
        }
        let body = encode_rowbinary(&batch);
        let token = dedup_token(&batch);
        // query + dedup token go in the URL query string; reqwest percent-encodes.
        let mut req = self
            .client
            .post(&self.base_url)
            .query(&[
                ("query", self.insert_query().as_str()),
                ("insert_deduplication_token", token.as_str()),
            ])
            .body(body);
        if let Some((user, pass)) = &self.credentials {
            req = req.basic_auth(user, Some(pass));
        }

        let send = tokio::time::timeout(self.config.timeout, req.send());
        let resp = match send.await {
            Err(_elapsed) => return Err(sql_common::SqlSinkError::timeout("clickhouse")),
            Ok(Err(e)) if e.is_timeout() => {
                return Err(sql_common::SqlSinkError::timeout("clickhouse"));
            }
            Ok(Err(e)) => {
                // connect / DNS / network reset → transient.
                return Err(sql_common::SqlSinkError::transient("clickhouse", format!("request: {e}")));
            }
            Ok(Ok(r)) => r,
        };

        let status = resp.status();
        if status.is_success() {
            Ok(CommitResult { committed: batch, dead_lettered: Vec::new() })
        } else if status.is_server_error() {
            Err(sql_common::SqlSinkError::transient("clickhouse", format!("http {status}")))
        } else {
            let detail = resp.text().await.unwrap_or_default();
            let detail: String = detail.chars().take(500).collect();
            Err(sql_common::SqlSinkError::permanent("clickhouse", format!("http {status}: {detail}")))
        }
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        let mut req = self.client.get(&self.base_url).query(&[("query", "SELECT 1")]);
        if let Some((user, pass)) = &self.credentials {
            req = req.basic_auth(user, Some(pass));
        }
        match tokio::time::timeout(self.config.timeout, req.send()).await {
            Ok(Ok(r)) if r.status().is_success() => SinkHealth::Healthy,
            Ok(Ok(r)) => SinkHealth::Down(format!("http {}", r.status())),
            Ok(Err(e)) => SinkHealth::Down(format!("request: {e}")),
            Err(_) => SinkHealth::Down("health probe timed out".to_string()),
        }
    }
}
```
> NOTE: confirm `reqwest::RequestBuilder::query(&[(&str,&str)])` + `.basic_auth` + `.body(Vec<u8>)` signatures against the resolved reqwest version (the http-sink already uses reqwest — mirror its calls). The `Sink` trait uses `async fn` in trait (AFIT) — match how postgres.rs writes `impl Sink`.

- [ ] **Step 4: Run + clippy** — `cargo test -p weir-server --no-default-features --features clickhouse-sink --lib sink::clickhouse` (all pass) and `cargo clippy -p weir-server --no-default-features --features clickhouse-sink --all-targets -- -D warnings` (clean — the stub's unused imports are now used).

- [ ] **Step 5: Commit** `git commit -am "feat(clickhouse): Sink impl — commit/max_batch_size/health"`

---

## Task 6: Config surface (SinkType + keys)

**Files:** `config/mod.rs`, `config/cli.rs`, `config/env.rs`, `config/file.rs`. **Mirror the existing `Postgres`/`sink_postgres_*` wiring exactly** (it's already feature-gated from Phase 1 — copy its `#[cfg]` shape).

- [ ] **Step 1:** `config/mod.rs` — add the gated `SinkType` variant, parse arms (both `cfg` and `not(cfg)` like Phase 1 did for the others), the `sink_clickhouse_{database,table,column}` fields on `PartialConfig` and final `Config` (gated `#[cfg(feature="clickhouse-sink")]`), the merge, and the validation (when `sink_type = ClickHouse`: require `sink_url` + `sink_clickhouse_table`). Read the `Postgres` arm + `sink_postgres_*` field handling and mirror it; defaults: database `"default"`, column `"payload"`.

- [ ] **Step 2:** `config/cli.rs` — `--sink-clickhouse-database`/`--sink-clickhouse-table`/`--sink-clickhouse-column` (gated, mirror `--sink-postgres-*`); add to `--help`.

- [ ] **Step 3:** `config/env.rs` — `WEIR_SINK_CLICKHOUSE_DATABASE`/`_TABLE`/`_COLUMN` (gated, mirror the postgres env reads).

- [ ] **Step 4:** `config/file.rs` — the 3 TOML keys in `RawServer` + `KNOWN_SERVER_KEYS` + the PartialConfig ctor (gated, mirror postgres).

- [ ] **Step 5: Tests** — add a config test (in `config/mod.rs` test module), gated `#[cfg(feature="clickhouse-sink")]`:
```rust
#[cfg(feature = "clickhouse-sink")]
#[test]
fn clickhouse_sink_requires_table() {
    let mut cli = PartialConfig::empty();
    cli.sink_type = Some("clickhouse".to_string());
    cli.sink_url = Some("http://h:8123".to_string());
    // no sink_clickhouse_table
    let err = Config::from_layers(cli, PartialConfig::empty(), minimal_valid_partial())
        .expect_err("clickhouse without table must fail");
    assert!(err.to_string().contains("table"), "{err}");
}
```
(Use the real minimal-config test helper — find it as in Phase 1.)

- [ ] **Step 6: Verify** — `cargo test -p weir-server --no-default-features --features clickhouse-sink config` and `cargo build -p weir-server` (default still builds; ClickHouse variant absent there). clippy clean for the clickhouse feature set.

- [ ] **Step 7: Commit** `git commit -am "feat(config): sink_type=clickhouse + sink_clickhouse_* keys"`

---

## Task 7: main.rs dispatch arm

**Files:** `crates/weir-server/src/main.rs`. **Mirror the `SinkType::Postgres` arm (lines ~307-330).**

- [ ] **Step 1:** Add the gated import: `#[cfg(feature = "clickhouse-sink")] use sink::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};`

- [ ] **Step 2:** Add the gated match arm building `ClickHouseSinkConfig` from the merged config (url = `config.sink_url`, database/table/column from `sink_clickhouse_*`, `max_batch_size = config.sink_max_batch_size`, `timeout = Duration::from_secs(config.sink_timeout_secs)`), constructing `ClickHouseSink::new(...)`, and spawning the drain — exactly like the Postgres arm. Log the redacted config (its `Debug` redacts).

- [ ] **Step 3: Verify** — `cargo build -p weir-server --no-default-features --features clickhouse-sink` (Finished) and `cargo build -p weir-server` (default, Finished — match exhaustiveness holds because the ClickHouse variant only exists with the feature). `cargo build -p weir-server --all-features`.

- [ ] **Step 4: Commit** `git commit -am "feat(server): wire clickhouse sink into the drain dispatch"`

---

## Task 8: Integration test (docker-compose + #[ignore] E2E)

**Files:** `deploy/docker/test/docker-compose.yml`, `deploy/docker/test/init-clickhouse.sql` (create), `deploy/run-sink-integration-tests.sh`, `crates/weir-server/tests/system.rs`, `docs/testing/sink-integration.md`. **Mirror `postgres_sink_end_to_end` + the postgres service entry.**

- [ ] **Step 1:** Add a ClickHouse service to `deploy/docker/test/docker-compose.yml`:
```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:24-alpine
    ports: ["127.0.0.1:18123:8123"]
    volumes:
      - ./init-clickhouse.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:8123/ping"]
      interval: 2s
      timeout: 5s
      retries: 30
```

- [ ] **Step 2:** Create `deploy/docker/test/init-clickhouse.sql` — a dedup-capable table with one String column:
```sql
CREATE TABLE IF NOT EXISTS default.weir_records
(
    payload String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;
```
(`non_replicated_deduplication_window` makes a plain `MergeTree` dedup by `insert_deduplication_token` — the simplest dedup-capable setup for the test; the docs note Replicated engines for production.)

- [ ] **Step 3:** `deploy/run-sink-integration-tests.sh` — export `WEIR_TEST_CLICKHOUSE_URL="http://127.0.0.1:18123"` and gate the clickhouse healthcheck wait, mirroring the mysql/postgres handling.

- [ ] **Step 4:** Add `clickhouse_sink_end_to_end` to `crates/weir-server/tests/system.rs`, `#[ignore]`-marked + `#[cfg(feature = "clickhouse-sink")]`, reading `WEIR_TEST_CLICKHOUSE_URL`. Mirror `postgres_sink_end_to_end`: push 100 Sync records → assert all committed with ≥10:1 records-per-insert IOPS compression (query `SELECT count() FROM default.weir_records`). Then **push the SAME 100 records again** (simulating replay) and assert the row count is **still 100** (dedup token worked). Document required env in the test doc comment.

- [ ] **Step 5:** `docs/testing/sink-integration.md` — add ClickHouse to the service list + the `WEIR_TEST_CLICKHOUSE_URL` env.

- [ ] **Step 6: Verify** the test compiles (it won't run without the live service): `cargo test -p weir-server --no-default-features --features clickhouse-sink --test system -- --list 2>/dev/null | grep clickhouse_sink_end_to_end`. If docker is available locally, optionally run `deploy/run-sink-integration-tests.sh` (else rely on it in the final CI push). Note in the commit if the live run was skipped.

- [ ] **Step 7: Commit** `git commit -am "test(clickhouse): docker-compose E2E + replay-dedup assertion"`

---

## Task 9: Docs + version bump

**Files:** `docs/operations/configuration.md`, `README.md`, `crates/weir-server/src/sink/mod.rs` (doc), `CHANGELOG.md`, root `Cargo.toml`.

- [ ] **Step 1:** `docs/operations/configuration.md` — add a "ClickHouse sink" subsection: the 3 keys (database/table/column, types/defaults/CLI/env/TOML), that it needs `--features clickhouse-sink`, the RowBinary/single-column model, and the **dedup contract** (sha256 token + `Replicated*MergeTree` / `insert_deduplicate` + the dedup-window caveat).
- [ ] **Step 2:** `README.md` sink table + the `sink/mod.rs` module-doc: add ClickHouse (done partly in Task 1 — ensure consistent).
- [ ] **Step 3:** `CHANGELOG.md` — under the working section, an `### Added`: ClickHouse sink (HTTP RowBinary, dedup token), opt-in `clickhouse-sink` feature.
- [ ] **Step 4:** Bump root `Cargo.toml` `[workspace.package] version` **0.6.0 → 0.7.0**; run `cargo build` so `Cargo.lock` updates.
- [ ] **Step 5: Final verify** — all feature combos build (default, `--no-default-features`, `--features clickhouse-sink`, `clickhouse-sink + postgres-sink`, `--all-features`); `cargo test -p weir-server` default + `--features clickhouse-sink` (serial: `-- --test-threads=1`) pass; clippy clean default + clickhouse + all-features; `cargo fmt --all --check`.
- [ ] **Step 6: Commit** `git commit -am "docs+release: ClickHouse sink docs; bump 0.7.0"`

---

## Self-Review (author)

- **Spec coverage:** §3 arch → T1,T4,T5; §4 transport/RowBinary → T2,T5; §5 dedup → T3,T5; §6 config → T6; §7 errors → T5; §8 health → T5; §9 main wiring → T7; §10 feature gating → T1; §11 testing → T2,T3,T4,T6(config),T8; §12 docs → T9; §13 version → T9; §14 out-of-scope → not built (correct); §15 acceptance → final verify (T9 step 5) + T8.
- **Placeholder scan:** none — every code step has complete code; the config/main/test wiring tasks (T6, T7, T8) say "mirror the Postgres arm" and point at exact line ranges + give the new identifiers/keys/defaults, which is concrete given the Phase-1-gated postgres code is the literal template.
- **Type consistency:** `ClickHouseSinkConfig`/`ClickHouseSink`/`ClickHouseSinkBuildError`/`encode_rowbinary`/`write_leb128`/`dedup_token`/`insert_query`/`split_credentials` are defined once and referenced consistently; `Sink::Error = sql_common::SqlSinkError` matches the postgres sink; the config keys `sink_clickhouse_{database,table,column}` are consistent across T6 + T7 + T9.
- **Executor judgement points (flagged inline):** `sql_common` path (`super::sql_common` vs `sql_common`), reqwest call signatures (mirror http-sink), the minimal-config test helper name, and the docker-compose/runner mirror — each has a NOTE.
