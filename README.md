# weir

A durable, high-throughput write buffer for Rust.

Producers write records to the `weir` daemon over a Unix domain socket. The daemon validates each record, writes it to a CRC32-checksummed Write-Ahead Buffer (WAB), fsyncs according to the configured durability tier, ACKs the producer, and asynchronously drains batches to a user-implemented `Sink`. WAB segments are not reclaimed until the sink confirms the batch. On restart, any unconfirmed segments are replayed automatically.

## Why

The path between "producer wants to write a record" and "record is durably committed to a downstream database" is usually either slow (synchronous insert per record, fsync per row, network round-trip) or unsafe (in-memory queue, lost on crash). `weir` compresses producer-facing latency to one local socket round-trip and one local fsync, while preserving end-to-end durability via the WAB, and amortises downstream cost via bulk drain.

## Durability tiers

| Tier       | ACK condition                  | Throughput     |
|------------|--------------------------------|----------------|
| `Sync`     | After fsync of the WAB segment | Lowest         |
| `Batched`  | After fsync of the next batch  | ~10–50× `Sync` |
| `Buffered` | After WAB memory write         | Highest        |

The tier is set per-record (or as a per-connection default). The benchmark harness publishes one number per tier, per platform — including the honest `F_FULLFSYNC` vs `fdatasync` distinction.

## License

MIT License. See [LICENSE](LICENSE).
