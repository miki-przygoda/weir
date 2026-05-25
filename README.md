# weir

A durable, high-throughput write buffer for Rust.

Producers write records to the `weir` daemon over a Unix domain socket. The daemon validates each record, writes it to a CRC32-checksummed Write-Ahead Buffer (WAB), fsyncs according to the configured durability tier, ACKs the producer, and asynchronously drains batches to a user-implemented `Sink`. WAB segments are not reclaimed until the sink confirms the batch. On restart, any unconfirmed segments are replayed automatically.

---

## Status

**Pre-release. Not yet ready for production use.**

The full pipeline is implemented, assembled, and tested: wire protocol → socket layer → queue → worker pool → WAB → drain → Prometheus metrics, with three-layer config (CLI > env > TOML file > defaults) and graceful shutdown. The `NoopSink` placeholder accepts all records; replace it with a real `Sink` implementation to commit records to a downstream store. The wire protocol and WAB on-disk format are versioned and will not change without a version bump.

The system test suite covers 41 scenarios including graceful shutdown under load, stalled-client isolation, partial frame injection, disk-full nacks, WAB byte-level integrity, fd-limit exhaustion, socket takeover, record ordering, and crash-restart metric consistency. A load benchmark baseline is published at [`docs/benchmarks.md`](docs/benchmarks.md).

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
| `weir-server` | bin + lib | Daemon: socket layer, WAB, queue, worker pool, drain, metrics, config. Unix only.                                |
| `weir-client` | lib       | Client library. Connects over a Unix socket, sends Push/HealthCheck frames, returns typed errors.                |
| `weir-bench`  | bin       | Standalone benchmark binary (placeholder; the CI load suite lives in `weir-server/tests/load.rs`).               |

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
- [Benchmarks](docs/benchmarks.md) — throughput baseline by deadline, latency percentiles, saturation ramp

---

## CI and deployment

The CI pipeline (`.github/workflows/ci.yml`) runs:

1. **lint** — `cargo fmt --check` + `cargo clippy -D warnings`
2. **test** — `cargo test` (unit + integration, 41 system tests)
3. **load** — 5 × 1ms + 5 × 2ms deadline runs; results averaged by `deploy/avg_benchmarks.py` and committed to `docs/benchmarks.md` on pushes to `main`
4. **build** — cross-compiled release binaries for `x86_64-linux`, `aarch64-linux`, `x86_64-macos`, `aarch64-macos`, `x86_64-windows`

A Docker image is available via `deploy/Dockerfile` (multi-stage, `gcr.io/distroless/cc-debian12` runtime). The annotated example config is at `deploy/docker/weir.toml.example`.

```
docker compose -f deploy/docker-compose.yml up
```

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
