# ClickHouse Sink — Design Spec

- **Status:** Approved (design), pre-implementation
- **Date:** 2026-06-10
- **Phase:** v1 roadmap Phase 2 (→ 0.7.0). Branch `v1/phase-2-clickhouse` off `v1/phase-1-publish`.
- **Author:** Mikolaj (with Claude Code)

## 1. Goal

Add a **ClickHouse sink** as the fourth real drain target, behind a default-off
`clickhouse-sink` cargo feature. It proves weir's "extensible sink line": a new
sink is ~150 LOC of driver glue on the shared `sql_common` base, not a from-scratch
implementation. Same `Sink` trait, same batch-insert IOPS-compression story as the
SQL sinks (N records → 1 HTTP insert → 1 ClickHouse block).

## 2. Decisions (locked in brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Transport | **Raw HTTP via `reqwest`** (`:8123` interface) | reqwest already in the tree (http-sink); opaque `Vec<u8>` payloads gain nothing from a typed client; leanest. |
| Insert format | **`RowBinary` into one `String` column** | ClickHouse `String` = arbitrary bytes → binary-safe (no escaping, unlike `Values`); compact; trivial to build. |
| Idempotency | **Content-derived `insert_deduplication_token`** (sha256 of the batch) | Replay re-emits the byte-identical batch → identical token → ClickHouse dedups the block. No `Sink` trait change. Same sha256 pattern the http-sink already uses. |
| Shared infra | **Reuse `sql_common`** (`validate_identifier`, `redact_password`, `SqlSinkError`) | ClickHouse speaks URL + database/table/column, so it fits the SQL-sink shape; net new code stays small. |
| Default feature | **Opt-in** (not in `default`) | New/heavier sink; consistent with the Phase 1 sink-gating policy. |

## 3. Architecture & placement

- New file `crates/weir-server/src/sink/clickhouse.rs`, declared
  `#[cfg(feature = "clickhouse-sink")] pub mod clickhouse;` in `sink/mod.rs`.
- `clickhouse-sink` enables `_sql-sink` (so `sql_common` compiles) plus
  `dep:reqwest` and `dep:sha2`.
- Implements `Sink` (`commit` + `max_batch_size` + `health`) with
  `type Record = Payload`, `type Error = SqlSinkError`.
- `ClickHouseSink { client: reqwest::Client, config }` and
  `ClickHouseSinkConfig { url, database, table, column, max_batch_size,
  timeout }`. A `Debug` impl that redacts the URL password via
  `sql_common::redact_password`.

## 4. Transport & insert format

Per `commit(batch)`:
```
POST {base_url}/?query=INSERT+INTO+{db}.{table}+({col})+FORMAT+RowBinary
                 &insert_deduplication_token={token}
Headers: Authorization: Basic … (if url carries user:password)
Body (RowBinary, one String column):  for each payload → leb128(len) ++ payload_bytes
```
- `{db}`, `{table}`, `{col}` are validated at construction via
  `sql_common::validate_identifier` (ClickHouse identifier rules ≈ the SQL ones;
  `IDENTIFIER_MAX_LEN` for ClickHouse — use the Postgres value 63 unless docs say
  otherwise) so the query string is injection-safe.
- RowBinary String encoding = unsigned LEB128 varint length prefix, then the raw
  bytes. A helper `encode_rowbinary(batch: &[Payload]) -> Vec<u8>` builds the body.
- HTTP user:password (if present in `sink_url`) → HTTP Basic auth header; never in
  logs (redacted in `Debug`).
- One request per batch. `max_batch_size` default 1000 (override via
  `sink_max_batch_size`).

## 5. Idempotency

- `token = hex(sha256(payload₀ ++ payload₁ ++ … ++ payloadₙ))`, computed per batch,
  sent as `insert_deduplication_token`.
- On crash-replay the drain re-reads the same segment and re-emits the **byte-
  identical batch in the same order** → identical token → ClickHouse deduplicates
  the inserted block (no duplicate rows).
- **Documented operator requirement:** a `Replicated*MergeTree` engine with
  `insert_deduplicate = 1` (default on for replicated tables); note the dedup
  *window* (default the last 100 block hashes, `replicated_deduplication_window`).
  If the table engine is not dedup-capable, the token is harmless and dedup falls
  back to the user's own config — weir stays honest about the boundary (consistent
  with the non-goal "not opinionated about dedup").
- **Determinism dependency (explicit):** this relies on the drain producing the
  same batch composition on replay (same records, same order, same `max_batch_size`).
  The drain replays a segment's records deterministically, so this holds. If a
  future change made replay batching non-deterministic, dedup would degrade to the
  engine's own block dedup — documented, not silent.

## 6. Config surface

Reuses the shared sink keys (`sink_url`, `sink_timeout_secs`, `sink_max_batch_size`,
`sink_type`). Adds, gated `#[cfg(feature = "clickhouse-sink")]` across
`config/{mod,cli,env,file}.rs`:

| TOML key | CLI flag | Env | Default | Notes |
|---|---|---|---|---|
| `sink_clickhouse_database` | `--sink-clickhouse-database` | `WEIR_SINK_CLICKHOUSE_DATABASE` | `default` | |
| `sink_clickhouse_table` | `--sink-clickhouse-table` | `WEIR_SINK_CLICKHOUSE_TABLE` | — | required when `sink_type = clickhouse` |
| `sink_clickhouse_column` | `--sink-clickhouse-column` | `WEIR_SINK_CLICKHOUSE_COLUMN` | `payload` | |

- `sink_type = "clickhouse"` selects it; requesting it in a build without the
  feature yields the Phase 1 "requires the 'clickhouse-sink' feature" error.
- No `insert_mode` (ClickHouse has no `ON CONFLICT`; dedup = token + engine).
- `sink_url` carries scheme/host/port (e.g. `http://user:pass@ch-host:8123`).
- Validation mirrors the SQL sinks: when `sink_type = clickhouse`, `sink_url` and
  `sink_clickhouse_table` are required; the table/column/database identifiers are
  validated at construction.

## 7. Error handling

`commit` maps reqwest/HTTP outcomes onto `SqlSinkError` (`driver = "clickhouse"`):
- connection refused / DNS / network reset / request timeout / HTTP **5xx** →
  `Transient` (drain retries with backoff).
- HTTP **4xx** (bad query, auth failure, unknown table/column) → `Permanent`
  (drain dead-letters the batch).
- reqwest timeout elapsed → `Timeout` (treated as transient by `SqlSinkError`).

A `ClickHouseSinkBuildError` (mirrors `MySqlSinkBuildError`/`PostgresSinkBuildError`)
for construction failures: invalid identifier (via the shared
`From<InvalidIdentifier>` impl + a `clickhouse` `IDENTIFIER_MAX_LEN`), malformed
URL, missing required config.

## 8. Health

`async fn health()` issues `GET {base_url}/?query=SELECT+1` → `SinkHealth::Healthy`
on 200, `SinkHealth::Down { reason }` otherwise (or on network error). Surfaced via
the existing `weir_sink_health{state}` metric — same shape as the SQL sinks.

## 9. main.rs wiring

Add `#[cfg(feature = "clickhouse-sink")]` to: the `use sink::clickhouse::{...}`
import and a new `SinkType::ClickHouse` match arm that builds
`ClickHouseSinkConfig` from the merged config and constructs the sink, then spawns
the drain (mirrors the Postgres arm).

## 10. Feature gating & deps

```toml
clickhouse-sink = ["dep:reqwest", "dep:sha2", "_sql-sink"]
```
`reqwest`/`sha2` are already optional (Phase 1, under `http-sink`); `clickhouse-sink`
just also enables them. Builds must stay green for: `clickhouse-sink` alone,
`clickhouse-sink` + the SQL sinks (shared `_sql-sink`/`sql_common`), `--all-features`,
default, and `--no-default-features`.

## 11. Testing

**Unit (in `clickhouse.rs`):**
- `encode_rowbinary`: empty payload, single payload, multi-payload, a payload ≥128
  bytes (LEB128 multi-byte varint boundary), and the exact byte layout for a known
  small batch.
- dedup token: same batch → same token; reordered batch → different token; empty
  batch handled.
- identifier validation rejects injection attempts in database/table/column.
- URL password redaction in `Debug`.
- error classification: 5xx → transient, 4xx → permanent, network error → transient,
  timeout → timeout.

**Integration (real ClickHouse via docker-compose):**
- Add a `clickhouse/clickhouse-server` service to
  `deploy/docker/test/docker-compose.yml` (HTTP on a mapped port, healthcheck).
- `deploy/docker/test/init-clickhouse.sql`: create a `ReplicatedMergeTree` (or
  `MergeTree` with `non_replicated_deduplication_window` set) table with one
  `String` column so dedup is exercisable.
- `clickhouse_sink_end_to_end` in `crates/weir-server/tests/system.rs`,
  `#[ignore]`-marked, reads `WEIR_TEST_CLICKHOUSE_URL`: push N Sync records, assert
  all committed and ≥10:1 records-per-insert IOPS compression — mirrors
  `postgres_sink_end_to_end`. A second assertion replays the same batch and confirms
  no duplicate rows (dedup token works).
- Wire it into `deploy/run-sink-integration-tests.sh` (export
  `WEIR_TEST_CLICKHOUSE_URL`) and document in `docs/testing/sink-integration.md`.

## 12. Docs

- `docs/operations/configuration.md`: the 3 new keys + the dedup/engine note.
- Sink list in `sink/mod.rs` module doc and the README sink table: add ClickHouse.
- CHANGELOG `[Unreleased]` (or the 0.7.0 section on this branch): the new sink.

## 13. Version

Workspace bump **0.6.0 → 0.7.0** on this branch (final commit of the phase).

## 14. Out of scope

- Native TCP protocol (`:9000`) — HTTP is sufficient and lean.
- Typed/columnar multi-field inserts — weir's payload is opaque bytes → one column.
- Async/streaming inserts, ClickHouse-side async_insert — the batch-per-segment
  model already gives the IOPS compression.
- Logical (identity-based) dedup tokens that would require threading batch identity
  through the `Sink` trait — content-hash is sufficient given deterministic replay.

## 15. Acceptance criteria

- `weir-server --features clickhouse-sink` builds; default build unchanged.
- All feature combos compile (clickhouse alone, + SQL sinks, all-features, default,
  no-default); clippy + fmt clean.
- Unit tests pass (RowBinary encoding, token determinism, validation, error mapping).
- `sink_type = "clickhouse"` end-to-end against a real ClickHouse commits a batch in
  one insert; a replayed identical batch produces no duplicate rows.
- `sink_type = "clickhouse"` without the feature → clear "requires the
  'clickhouse-sink' feature" error.
- Config keys wired through CLI/env/TOML + validation; docs updated; version 0.7.0.
