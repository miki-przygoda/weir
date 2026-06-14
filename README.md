# weir

A durable, high-throughput write buffer for Rust.

Producers write records to the `weir` daemon over a Unix domain socket.
The daemon validates each record, writes it to a CRC32-checksummed
Write-Ahead Buffer (WAB), fsyncs according to the configured durability
tier, ACKs the producer, and asynchronously drains batches to a
user-implemented `Sink`. WAB segments are not reclaimed until the sink
confirms the batch. On restart, any unconfirmed segments are replayed
automatically.

> **Status — pre-v1.** Pipeline, security hardening, and the SQL
> sink line are complete. Five built-in sinks (feature-gated; `noop`
> always compiled): `noop` for soak-testing,
> `http` (POST per record, transient/permanent classification),
> `mysql` (multi-row `INSERT` per batch — the IOPS-compression sink:
> N records → 1 statement → 1 server-side commit), `postgres`
> (the same shape with `ON CONFLICT DO NOTHING`, TLS opt-in via
> `?sslmode=require`), and `clickhouse` (HTTP `RowBinary` inserts with a
> sha256 `insert_deduplication_token` for replay safety). WAB flusher panics are supervised and respawned
> (capped at 10 attempts before the shard goes offline). Wire protocol
> and WAB on-disk format are versioned and stable.
> **Not yet on crates.io.**

## Quickstart

```bash
cargo build --release -p weir-server
mkdir -p /tmp/weir/wab /tmp/weir/run && chmod 0700 /tmp/weir/run
./target/release/weir-server \
    --wab-dir /tmp/weir/wab \
    --socket-path /tmp/weir/run/weir.sock
```

Full walk-through, including pushing your first record: [docs/getting-started/quickstart.md](docs/getting-started/quickstart.md).

## Documentation

Start at [`docs/`](docs/) — the docs landing page is the recommended
entry point. The structure:

| Section            | What's there                                                              |
|--------------------|---------------------------------------------------------------------------|
| **Getting started**| [install](docs/getting-started/install.md), [quickstart](docs/getting-started/quickstart.md) |
| **Operations**     | [configuration reference](docs/operations/configuration.md) (every option, default, range, tuning notes) |
| **Protocol**       | [wire format](docs/wire_protocol.md), [WAB format](docs/wab_format.md)    |
| **Architecture**   | [internals](docs/architecture.md), [benchmarks](docs/benchmarks.md)       |
| **Security**       | [threat model](docs/security/threat-model.md), [socket-bind hardening](docs/security/socket-bind.md), [container hardening](docs/security/container.md) — reporting policy at [SECURITY.md](SECURITY.md) |
| **Testing**        | [system-test audit](docs/testing/test-audit.md) (verdicts on all 41 system tests), [sink integration suite](docs/testing/sink-integration.md) (docker-compose runner for the SQL sinks), [fuzzing](docs/testing/fuzzing.md) (`cargo-fuzz` targets for the trust-boundary parsers) |

The full version history is in [CHANGELOG.md](CHANGELOG.md).

## Why

The path between "producer wants to write a record" and "record is
durably committed to a downstream database" is usually slow
(synchronous insert per record, fsync per row, network round-trip) or
unsafe (in-memory queue, lost on crash). `weir` compresses
producer-facing latency to one local socket round-trip and one local
fsync, while preserving end-to-end durability via the WAB, and
amortises downstream cost via bulk drain.

## Crates

| Crate           | Type       | Description                                                                                          |
|-----------------|------------|------------------------------------------------------------------------------------------------------|
| `weir-core`     | lib        | Wire protocol types — `Envelope`, `Header`, `Durability`, `NackReason`, `Payload`. Cross-platform.   |
| `weir-server`   | bin + lib  | Daemon: socket layer, WAB, queue, worker pool, drain, metrics, config. **Unix only.**                |
| `weir-client`   | lib        | Client library. Connects over a Unix socket (or TCP + mutual TLS), sends Push/HealthCheck frames, returns typed errors. Ships two examples (`push_simple`, `health_check`). Benchmark coverage lives in `weir-server/tests/load.rs`. |
| `weir-sink-sdk` | lib        | The `Sink` trait plus its `SinkError` / `CommitResult` contract, published standalone so downstream authors can write custom sinks without depending on the daemon internals. |
| `weir-ctl`      | bin        | Admin CLI for a running daemon: `health`, `push`, `metrics`, `segments` (per-shard WAB inspect), and `dl` (dead-letter list/drop). |
| `weir-testkit`  | lib (dev)  | Internal test harness (the `weir_server!` integration-test macro). Not published.                    |

## Non-goals (v1)

- **Not embedded** — weir is a daemon; producers talk to it over a socket.
- **Not a message broker** — no pub/sub, no fan-out. One producer stream, one sink.
- **Not a database** — the WAB is not queryable; replay on crash is the only read path.
- **Not opinionated about serialisation** — the wire envelope carries opaque bytes.

The default transport is the Unix domain socket; an optional TCP + mutual-TLS
listener is available behind the `tls` feature for cross-host producers.

## License

MIT License. See [LICENSE](LICENSE).
