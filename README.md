# weir

[![CI](https://github.com/miki-przygoda/weir/actions/workflows/ci.yml/badge.svg)](https://github.com/miki-przygoda/weir/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](Cargo.toml)
<!-- crates.io + docs.rs badges to be added in the same commit as the 1.0 crates.io publish -->

A durable, high-throughput write buffer for Rust.

**Fire a record at a local socket, get a durable ack in microseconds, and let
your database catch up in bulk.** weir writes each record to a CRC32-checksummed
write-ahead buffer, fsyncs it according to the durability tier you ask for, acks
the producer, then drains records to your sink in batches — turning N per-record
commits into 1. **An ack is never a false ack:** an acked record is on disk and
replays after a crash.

`~69 µs` Buffered (non-durable) ack p50 · `~364 µs` durable Sync ack p50 ·
`~2,550` Sync RPS · `~58,600` RPS at saturation (Buffered, ~64 threads)
*(indicative — single box, sandboxed CI runners; reproduce on your own hardware, see [benchmarks](docs/benchmarks.md))* ·
5 built-in sinks (4 in a default build; `clickhouse` opt-in) · v1 wire + Rust API frozen under SemVer

**▶ [Try the demo](demo/index.html)** — a self-contained, browser-only
simulation of the pipeline: push records, flip the durability tier, crash the
daemon mid-flight, and watch unconfirmed segments replay, side by side with a
naive insert-per-record baseline. No build step — open `demo/index.html` in any
browser. *(Hosted version coming with the public launch.)*

> **Status — 1.x (stable).** The v1 wire protocol and public Rust API (`weir-core`,
> `weir-client`, `weir-sink-sdk`, `weir-wab`) are frozen under
> [Semantic Versioning](https://semver.org/), with a
> [language-neutral conformance suite](docs/conformance.md) pinning the wire
> format for non-Rust implementers. The WAB on-disk format is stable and
> unconfirmed segments replay on restart. Built-in sinks: `noop`, `http`,
> `mysql`, `postgres` in the default build, plus `clickhouse` behind the opt-in
> `clickhouse-sink` Cargo feature (see [Crates](#crates) and the
> [configuration reference](docs/operations/configuration.md)). WAB flusher and
> drain threads are panic-supervised. Publishing to crates.io with the public release.

## How it works

Producers write records to the `weir` daemon over a Unix domain socket (or
TCP + mutual TLS). The daemon validates each record, writes it to the
CRC32-checksummed Write-Ahead Buffer, fsyncs per the durability tier, acks the
producer, and asynchronously drains batches to a `Sink`. WAB segments are not
reclaimed until the sink confirms the batch; on restart, unconfirmed segments
are replayed automatically.

## Quickstart

```bash
cargo build --release -p weir-server
mkdir -p /tmp/weir/wab /tmp/weir/run && chmod 0700 /tmp/weir/run
./target/release/weir-server \
    --wab-dir /tmp/weir/wab \
    --socket-path /tmp/weir/run/weir.sock \
    --sink-type http --sink-url https://your-endpoint.example/ingest
```

> **Ack ≠ delivered — and the default sink discards everything.** A successful
> push means the record is durably *buffered* on disk, not that it reached a
> downstream. With **no `--sink-type`, the sink defaults to `noop`, which acks
> then DISCARDS every record** (it's for soak-testing). Set `--sink-type`
> (`http`/`mysql`/`postgres`/`clickhouse`) + `--sink-url` to actually deliver.
> Records also only drain once a WAB segment seals, so a few small records won't
> reach the sink until shutdown — see the quickstart.
>
> **Two more producer gotchas:** `push()` is **blocking** — never call it directly
> from an `async fn` (it starves the runtime; use the
> [async bridge](docs/getting-started/integrating.md#producing-from-an-async-runtime)).
> And `Buffered` acks *before* fsync, so it survives a process crash but **not**
> power loss — use `Sync`/`Batched` for data you can't lose. The
> [quickstart](docs/getting-started/quickstart.md#push-your-first-record) has the
> full rundown.

Full walk-through, including pushing your first record: [docs/getting-started/quickstart.md](docs/getting-started/quickstart.md).

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
| **Testing**        | [system-test audit](docs/testing/test-audit.md) (verdicts on every system test), [sink integration suite](docs/testing/sink-integration.md) (docker-compose runner for the SQL sinks), [fuzzing](docs/testing/fuzzing.md) (`cargo-fuzz` targets for the trust-boundary parsers) |

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

## How weir compares

weir fills a specific gap: a **fast, durable, local write-ahead point** in front
of a slow or flaky downstream — typically a SQL database — that acks your
producer in **microseconds** and then **compresses N buffered records into 1
downstream commit**, surviving crashes by replaying unconfirmed segments. It does
this **without running a message broker**. That niche sits between three
well-trodden categories, and weir is deliberately *none* of them: not a broker
(no pub/sub or fan-out — one producer stream → one sink), not embedded (a daemon,
not a linked library), and not a database (the buffer isn't queryable).

| Project | Category | Great at | Why you'd pick weir instead |
|---|---|---|---|
| **Kafka** / **Redpanda** | Streaming broker | Multi-consumer fan-out, replayable topics, horizontal scale | One stream → one DB without running a broker (or a Connect + JDBC sink) just to batch writes |
| **NATS JetStream** / **NSQ** | Durable messaging | Subject/topic pub/sub, edge & realtime fan-out | A fsync-before-ack durability contract + N→1 SQL-commit delivery, not a subscribe/pull broker |
| **Apache Pulsar** | Streaming + tiered storage | Multi-tenant, geo-replicated, millions of topics | One binary on one host, not brokers + BookKeeper |
| **Chronicle Queue** | Embedded durable queue (JVM) | Ultra-low-latency persisted IPC inside the JVM | A language-agnostic *daemon* that also owns the drain into your database, not an in-process library you read back yourself |
| **RocksDB** / **sled** / **bbolt** | Embedded KV store + WAL | Queryable embedded persistence inside your app | A standalone buffer that drains to a sink and reclaims itself, not a queryable store you link in |
| **Vector** | Observability pipeline | Telemetry collection, transforms, routing with disk buffers | An application-facing durable ack + transactional N→1 commits into one SQL DB, not a general telemetry router |
| **Fluentd** / **Fluent Bit** / **Beats** | Log shippers | Log/metric forwarding with file buffering and retries | Your producer is an application needing a durable write-ahead point in front of its DB, not a logging agent |

**Reach for weir when** an application produces records faster than your database
wants to commit them, you need a **durable ack now** (not after a network
round-trip to a broker), and you want **many records to become one transactional
`INSERT`/commit** against MySQL, Postgres, ClickHouse, or an HTTP endpoint — from
a single local daemon with crash-safe replay. **Reach for something else when**
you need multiple consumers / pub/sub / fan-out (Kafka, Redpanda, NATS, Pulsar,
NSQ), an embedded queryable store (RocksDB, sled, bbolt) or in-JVM low-latency
queue (Chronicle Queue), or a telemetry pipeline with transforms and many
destinations (Vector, Fluentd, Fluent Bit, Beats).

**Delivery contract (read before migrating from Kafka/NATS):** delivery is
**at-least-once** — a crash or retry can redeliver a record, so **your sink must
dedupe** (e.g. on an `Idempotency-Key` / `ON CONFLICT`). **Ordering holds only
within a single connection's** sequential pushes — there is **no ordering across
connections, and no per-key or global order**. And **routing/keys live in your
payload**: the wire envelope is opaque bytes, with no topic/subject/key field —
weir does not inspect or route on content. Details in
[architecture](docs/architecture.md) and [integrating](docs/getting-started/integrating.md).

*No popular project combines all three of weir's defining properties — a local
microsecond fsync'd ack, N→1 transactional SQL-commit compression, and a
non-broker single binary. Comparisons reflect public positioning as of June 2026,
not benchmarks.*

## Crates

| Crate           | Type       | Description                                                                                          |
|-----------------|------------|------------------------------------------------------------------------------------------------------|
| `weir-core`     | lib        | Wire protocol types — `Envelope`, `Header`, `Durability`, `NackReason`, `Payload`. Cross-platform.   |
| `weir-wab`      | lib        | On-disk WAB segment format + `SegmentReader`. Shared by the daemon and `weir-ctl` (one parser) so `dl requeue` can read dead-letter segments without the daemon's dep tree. Cross-platform. |
| `weir-server`   | bin + lib  | Daemon: socket layer, WAB, queue, worker pool, drain, metrics, config. **Unix only.**                |
| `weir-client`   | lib        | Client library. Connects over a Unix socket (or TCP + mutual TLS), sends Push/HealthCheck frames, returns typed errors. Ships three examples (`push_simple`, `health_check`, `push_tls`). Benchmark coverage lives in `weir-server/tests/load.rs`. **Client type is Unix-only** — it compiles everywhere but `WeirClient` is `#[cfg(unix)]`; on Windows only the `Durability`/`NackReason` re-exports are usable (produce from non-Unix via the [wire protocol](docs/wire_protocol.md) — which in practice means the daemon's [TCP + mutual-TLS listener](docs/operations/tcp-mtls.md), since the Unix socket is unreachable cross-host and the Windows server binary is a non-functional stub). |
| `weir-sink-sdk` | lib        | The `Sink` trait plus its `SinkError` / `CommitResult` contract — published standalone so you can **implement and unit-test** a custom sink against a stable API, independent of the daemon internals. *Running* a custom sink in the shipped daemon currently means building `weir-server` with your sink wired into the sink-selection path (no dynamic plugin yet — see the crate docs). |
| `weir-ctl`      | bin        | Admin CLI for a running daemon: `health`, `push`, `metrics`, `segments` (per-shard WAB inspect), and `dl` (dead-letter list/drop/requeue). |
| `weir-testkit`  | lib (dev)  | Internal test harness (the `weir_server!` integration-test macro). Not published.                    |

These are deliberately separate so you can compose the pieces you need without
the daemon — produce with `weir-client`, forward with a custom `weir-sink-sdk`
sink, or read the buffer with `weir-wab`, and only depend on `weir-server` if you
run it. See [Integrating & extending](docs/getting-started/integrating.md) for
how, and [Architecture → Workspace & crate boundaries](docs/architecture.md#workspace--crate-boundaries)
for why.

## Non-goals (v1)

- **Not embedded** — weir is a daemon; producers talk to it over a socket.
- **Not a message broker** — no pub/sub, no fan-out. One producer stream, one sink.
- **Not a database** — the WAB is not queryable; replay on crash is the only read path.
- **Not opinionated about serialisation** — the wire envelope carries opaque bytes.

The default transport is the Unix domain socket; an optional TCP + mutual-TLS
listener is available behind the `tls` feature for cross-host producers.

## How it earns the "durable" claim

- **The invariant:** an ack is never a false ack — an acked record is on disk and
  replays after a crash. Every design choice serves this. (On Linux, durability
  is `fdatasync`. On macOS, weir uses `F_BARRIERFSYNC`, which orders writes and
  survives process crashes but is **not** guaranteed to survive sudden power loss
  on consumer SSDs with a volatile write cache — macOS is best treated as a dev
  platform; run production on Linux.)
- **Deterministic simulation (DST):** the WAB durability invariants are checked
  under injected crash/fault schedules with **replayable seeds**, on every CI run
  (a 300-seed sweep) — see [`docs/architecture.md`](docs/architecture.md).
- **Language-neutral conformance vectors:** canonical hex frames plus the result
  a conformant decoder must produce for every message type, all nine Nack
  reasons, and each rejection case — run your own codec against them
  ([docs/conformance.md](docs/conformance.md)).
- **Cross-platform CI:** `fmt` + `clippy` across the feature matrix + `cargo-deny`
  + the full test suites + a monitoring-stack end-to-end smoke test, on every
  push. The build matrix compiles `weir-server` on all five release targets,
  including `x86_64-pc-windows-msvc` — though the Windows binary is a
  non-functional stub (no Unix-socket listener); the daemon runs only on
  Linux and macOS.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, the exact pre-PR
gate (the same checks CI runs), and the heavier test suites. Security issues
go through [SECURITY.md](SECURITY.md), not public issues.

## License

MIT License. See [LICENSE](LICENSE).
