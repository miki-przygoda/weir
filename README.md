# weir

A durable, high-throughput write buffer for Rust.

Producers write records to the `weir` daemon over a Unix domain socket. The daemon validates each record, writes it to a CRC32-checksummed Write-Ahead Buffer (WAB), fsyncs according to the configured durability tier, ACKs the producer, and asynchronously drains batches to a user-implemented `Sink`. WAB segments are not reclaimed until the sink confirms the batch. On restart, any unconfirmed segments are replayed automatically.

---

## Status

**Pre-release. Not yet ready for production use.**

Core pipeline (wire protocol, WAB, queue, worker pool, socket layer, sink/drain, and Prometheus metrics) is implemented and tested. Config wiring and main-loop assembly are in progress (step 08). The wire protocol and WAB on-disk format are versioned and will not change without a version bump.

---

## Why

The path between "producer wants to write a record" and "record is durably committed to a downstream database" is usually either slow (synchronous insert per record, fsync per row, network round-trip) or unsafe (in-memory queue, lost on crash). `weir` compresses producer-facing latency to one local socket round-trip and one local fsync, while preserving end-to-end durability via the WAB, and amortises downstream cost via bulk drain.

---

## Platform

The `weir` daemon (`weir-server`) requires **Unix** (Linux, macOS). It uses Unix domain sockets and Unix file permissions throughout. The `weir-core` crate (wire protocol types) is cross-platform and can be used in clients on any platform.

Windows is not supported for the daemon. CI builds `weir-core` on Windows to keep the library crate clean; `weir-server` is excluded from the Windows build.

---

## Crate layout

| Crate         | Type      | Description                                                                                                      |
|---------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `weir-core`   | lib       | Wire protocol types: `Envelope`, `Header`, `Durability`, `NackReason`, `DecodeError`, `Payload`. Cross-platform. |
| `weir-server` | bin + lib | Daemon: socket layer, WAB, queue, worker pool, drain, metrics. Unix only.                                        |
| `weir-client` | lib       | Client library. Thin wrapper around the socket protocol.                                                         |
| `weir-bench`  | bin       | Benchmark harness. Three-tier throughput numbers per platform.                                                   |

---

## Durability tiers

| Tier       | ACK condition                                      | Throughput     |
|------------|----------------------------------------------------|----------------|
| `Sync`     | After `fdatasync` of the WAB segment               | Lowest         |
| `Batched`  | After group `fdatasync` at the next batch boundary | ~10–50× `Sync` |
| `Buffered` | After WAB memory write — no fsync                  | Highest        |

The tier is set per-record in the wire frame header. `weir` does not hide the fsync cost behind a global setting — every record's durability contract is explicit at the call site.

---

## Documentation

- [Architecture](docs/architecture.md) — data flow, component responsibilities, runtime boundary, security design
- [Wire protocol](docs/wire_protocol.md) — frame layout, message types, decode order, Nack payload format
- [WAB format](docs/wab_format.md) — segment binary format, `.confirmed` sidecar, crash recovery algorithm

---

## Non-goals (v1)

- **Not embedded.** `weir` is a daemon; producers talk to it over a socket.
- **Not a message broker.** No pub/sub, no fan-out. One producer stream, one sink.
- **Not a database.** The WAB is not queryable; replay on crash is the only read path.
- **Not opinionated about serialisation.** The wire envelope carries opaque bytes.
- **No TCP in v1.** Unix domain socket only. TCP + TLS is a later addition.

---

## License

MIT License. See [LICENSE](LICENSE).
