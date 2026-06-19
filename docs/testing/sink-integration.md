# Sink integration tests

The MySQL, Postgres, and ClickHouse sinks each ship a unit-test suite
that covers the parts that don't need a real backend — identifier
validation, SQL generation, error classification, password redaction.
What unit tests can't verify is the part that actually matters in
production: does the sink correctly commit a multi-row batch to a real
database, does SQLSTATE / error-code mapping work end-to-end, do
crash-recovery re-commits actually de-duplicate via the schema's UNIQUE
constraint (or, for ClickHouse, the per-batch
`insert_deduplication_token`)?

For that, we ship three `#[ignore]`-marked tests in
`crates/weir-server/tests/system.rs`:

- `mysql_sink_end_to_end` — pushes 100 Sync records, verifies the sink
  committed them all and did so as ≥10 records per `commit()` call
  (the IOPS-compression claim).
- `postgres_sink_end_to_end` — same assertion shape for the Postgres
  sink.
- `clickhouse_sink_end_to_end` — same assertion shape for the
  ClickHouse sink, verifying the end-to-end RowBinary HTTP insert path
  and the IOPS-compression ratio. Gated behind the `clickhouse-sink`
  cargo feature (`#[cfg(feature = "clickhouse-sink")]`), so it only
  compiles when that feature is enabled.

All three tests are `#[ignore]`-marked because they require a real
backend reachable at `WEIR_TEST_MYSQL_URL` / `WEIR_TEST_POSTGRES_URL` /
`WEIR_TEST_CLICKHOUSE_URL`. The runner script below brings up all three
backends in containers and runs all three tests in one shot.

## Quick start

```sh
# Pre-requisite: Docker installed, `docker compose` plugin available.
# Brings up the stack, waits for healthchecks, runs all three tests, tears down.
bash deploy/run-sink-integration-tests.sh

# Release build (matches CI):
RELEASE=1 bash deploy/run-sink-integration-tests.sh
```

The script exits 0 iff all three tests pass. The ClickHouse test is run
with `--features clickhouse-sink` so its sink is compiled in. The
container stack is torn down on exit (including failures), so the runner
is safe to re-invoke without leftover state.

## What it does

1. Runs `docker compose -f deploy/docker/test/docker-compose.yml up -d`.
   The compose file spins up:
   - `mysql:8.0` on `127.0.0.1:33306`, with the schema from
     `deploy/docker/test/init-mysql.sql` pre-seeded.
   - `postgres:16` on `127.0.0.1:55432`, with the schema from
     `deploy/docker/test/init-postgres.sql` pre-seeded.
   - `clickhouse/clickhouse-server:24-alpine` on `127.0.0.1:18123`
     (HTTP), with the schema from
     `deploy/docker/test/init-clickhouse.sql` pre-seeded.
2. Waits up to 120 s per service for the compose healthchecks
   (`mysqladmin ping`, `pg_isready`, and ClickHouse's HTTP `/ping`) to
   report `healthy`. Each container entrypoint gates the external
   listener on init-script completion, so a healthy status implies the
   `weir_records` table exists.
3. Exports the three `WEIR_TEST_*_URL` env vars pointing at the
   containers' published ports.
4. Runs the three ignored tests via `cargo test -- --ignored --exact`,
   adding `--features clickhouse-sink` for the ClickHouse test.
5. Tears down with `docker compose down -v` on exit.

## Schemas

All three schemas live in `deploy/docker/test/` and mirror the
reference schemas documented in `docs/operations/configuration.md`. The
SQL schemas include the `UNIQUE` constraint the default insert mode
(`INSERT IGNORE` for MySQL, `ON CONFLICT DO NOTHING` for Postgres) keys
against; the ClickHouse schema is a `MergeTree` with a non-zero
`non_replicated_deduplication_window`, which dedups inserts by the
per-batch `insert_deduplication_token` the sink sends. Either way the
tests exercise the idempotent re-commit path the drain relies on under
crash-recovery.

## Manual setup

If `docker compose` isn't available, the MySQL and Postgres test
docstrings include single-`docker run` setup recipes (the ClickHouse
docstring uses the compose recipe). Adjust the URL accordingly and run
one test at a time:

```sh
WEIR_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:3306/weir_test \
  cargo test -p weir-server --test system -- --ignored --exact \
  mysql_sink_end_to_end
```

## Out of scope

- **CI.** The runner script ships locally-runnable. Wiring this into
  GitHub Actions needs a service-container setup and is enough
  complexity that it deserves its own PR — `.github/workflows/sink-integration.yml`
  is a clean follow-up when someone wants it.
- **TLS-enabled containers.** The Postgres sink supports TLS via
  `?sslmode=require` (see
  [Configuration → Postgres](../operations/configuration.md), but
  the test compose stack runs without TLS so the runner doesn't
  need a CA-trusted cert. A TLS-enabled `postgres-tls` service
  alongside the existing `postgres` one is a clean follow-up
  that would exercise the handshake path end-to-end.
