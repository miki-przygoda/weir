# weir

A durable, high-throughput write buffer for Rust.

Producers write records to the `weir` daemon over a Unix domain socket.
The daemon validates each record, writes it to a CRC32-checksummed
Write-Ahead Buffer (WAB), fsyncs according to the configured durability
tier, ACKs the producer, and asynchronously drains batches to a
user-implemented `Sink`. WAB segments are not reclaimed until the sink
confirms the batch. On restart, any unconfirmed segments are replayed
automatically.

> **Status — 1.0.** The v1 wire protocol and the public Rust API
> (`weir-core`, `weir-client`, `weir-sink-sdk`) are **frozen under
> [Semantic Versioning](https://semver.org/)**, with a
> [language-neutral conformance suite](docs/conformance.md) pinning the
> wire format for non-Rust implementers. The WAB on-disk format is stable
> and unconfirmed segments replay on restart.
>
> Five built-in sinks (feature-gated; `noop` always compiled): `noop` for
> soak-testing, `http` (concurrent per-record POSTs, transient/permanent
> classification), `mysql` (multi-row `INSERT` per batch — the
> IOPS-compression sink: N records → 1 statement → 1 server-side commit),
> `postgres` (the same shape with `ON CONFLICT DO NOTHING`, TLS opt-in via
> `?sslmode=require`), and `clickhouse` (HTTP `RowBinary` inserts with a
> sha256 `insert_deduplication_token` for replay safety). WAB flusher and
> drain threads are panic-supervised. Publishing to crates.io with the 1.0
> release.

## Quickstart

```bash
cargo build --release -p weir-server
mkdir -p /tmp/weir/wab /tmp/weir/run && chmod 0700 /tmp/weir/run
./target/release/weir-server \
    --wab-dir /tmp/weir/wab \
    --socket-path /tmp/weir/run/weir.sock
```

Full walk-through, including pushing your first record: [docs/getting-started/quickstart.md](docs/getting-started/quickstart.md).

## Demo

[`demo/index.html`](demo/index.html) is a self-contained, browser-only
simulation — push records, toggle the durability tier, crash the daemon, and
watch unconfirmed segments replay, side by side with a naive
insert-per-record baseline. No build step; open the file directly.

## Documentation

Start at [`docs/`](docs/) — the docs landing page is the recommended
entry point. The structure:

| Section            | What's there                                                              |
|--------------------|---------------------------------------------------------------------------|
| **Getting started**| [install](docs/getting-started/install.md), [quickstart](docs/getting-started/quickstart.md) |
| **Operations**     | [configuration reference](docs/operations/configuration.md) (every option, default, range, tuning notes), [TCP + mutual TLS](docs/operations/tcp-mtls.md), [monitoring](docs/monitoring.md) (Prometheus metrics, alerts, Grafana) |
| **Protocol**       | [wire format](docs/wire_protocol.md), [WAB format](docs/wab_format.md), [conformance vectors](docs/conformance.md) (language-neutral test vectors for non-Rust clients) |
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

**Use weir when** a high-rate producer must not block on a slow or
flaky downstream, and must not lose records it has already been told are
safe — even across a crash. The classic fit is "fire a record at a local
socket, get a durable ack in microseconds, let the database catch up in
bulk."

**Don't reach for weir when** you need fan-out or pub/sub (use a message
broker), when you can already tolerate writing straight to the database
synchronously, or when losing un-acked in-flight data on crash is
acceptable (an in-process channel is simpler). See [Non-goals](#non-goals-v1).

## Crates

| Crate           | Type       | Description                                                                                          |
|-----------------|------------|------------------------------------------------------------------------------------------------------|
| `weir-core`     | lib        | Wire protocol types — `Envelope`, `Header`, `Durability`, `NackReason`, `Payload`. Cross-platform.   |
| `weir-server`   | bin + lib  | Daemon: socket layer, WAB, queue, worker pool, drain, metrics, config. **Unix only.**                |
| `weir-client`   | lib        | Client library. Connects over a Unix socket (or TCP + mutual TLS), sends Push/HealthCheck frames, returns typed errors. Ships three examples (`push_simple`, `health_check`, `push_tls`). Benchmark coverage lives in `weir-server/tests/load.rs`. |
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, the exact pre-PR
gate (the same checks CI runs), and the heavier test suites. Security issues
go through [SECURITY.md](SECURITY.md), not public issues.

## License

MIT License. See [LICENSE](LICENSE).
