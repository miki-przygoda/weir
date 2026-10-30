# weir and Vector's disk buffer, and Fluent Bit's filesystem storage

**Conclusion first.** Vector and Fluent Bit are telemetry pipelines that can
buffer on disk. They collect from systems that do not know about them, and
Vector transforms and fans out on the way. weir does none of that.
What it does offer is a producer-facing ack with a stated relation to fsync: at
`Durable` the ack follows an `fdatasync`, and that has been tested against
simulated power cuts. Neither tool's documentation, as read on 2026-10-29,
states what its disk buffer guarantees across a power loss. If you are shipping
logs or metrics, use one of them. If an application needs to know that a record
it was told is safe is on disk, weir is built around that question.

Claims about Vector and Fluent Bit below link to their documentation, read on
2026-10-29. Claims about weir link to this repository.

## The core question: when is the sender acked, relative to fsync?

### Vector

- **Buffers are per sink, and default to memory.** On the ClickHouse sink, for
  example, `buffer.type` is `memory` or `disk`, default `memory`, and a disk
  buffer must be at least ~256 MB
  ([ClickHouse sink](https://vector.dev/docs/reference/configuration/sinks/clickhouse/)).
  In-memory buffers are "not durable"; their events "would be lost if Vector, or
  the host running Vector, crashed"
  ([buffering model](https://vector.dev/docs/architecture/buffering-model/)).
- **Disk buffers sync on an interval.** "By default, we do not synchronize data
  to disk for every write, but instead synchronize on an interval (500
  milliseconds)." The page describes this as "high throughput with a reduced
  risk of data loss" (same page).
- **An ack can come from the buffer.** With end-to-end acknowledgements, a
  source waits for every connected sink to mark an event delivered, and "sinks
  which have a durable buffer configured will mark events as delivered once
  they are persisted to that buffer"
  ([guarantees](https://vector.dev/docs/architecture/guarantees/)).
- **Not every source can ack.** The acknowledgements page names `kafka` and
  `aws_sqs` as supporting it, says HTTP clients get a status code, and says the
  `socket` source "cannot acknowledge events"
  ([end-to-end acknowledgements](https://vector.dev/docs/architecture/end-to-end-acknowledgements/)).

Read together, the two documented facts mean an acknowledged event can be in the
disk buffer before the next 500 ms sync has run. That is a reading of the
documentation, not something it states. The buffering page says
buffered events survive a Vector crash; it does not address power loss.

### Fluent Bit

- **Buffering defaults to memory.** `storage.type` is `memory` by default, and
  "by default, all chunks are created only in memory." `filesystem` stores each
  chunk through `mmap(2)`
  ([buffering and storage](https://docs.fluentbit.io/manual/administration/buffering-and-storage)).
- **`storage.sync` is `normal` or `full`, default `normal`.** The documentation
  describes `full` only: it "increases the reliability of the filesystem buffer
  and ensures that data is guaranteed to be synced to the filesystem even if
  Fluent Bit crashes. On Linux, `full` corresponds with the `MAP_SYNC` option
  for memory mapped files." It does not describe what `normal` does
  ([service section](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/yaml/service-section)).
- **`MAP_SYNC` is a DAX flag.** The Linux `mmap(2)` page says it "is supported
  only for files supporting DAX (direct mapping of persistent memory)," is
  silently ignored on a plain `MAP_SHARED` mapping, and fails with `EOPNOTSUPP`
  on other files
  ([mmap(2)](https://man7.org/linux/man-pages/man2/mmap.2.html)). How Fluent Bit
  behaves on an ordinary ext4 or XFS file with `full` set is not in its
  documentation, and it is not tested here.
- **Checksums are off by default.** `storage.checksum` enables a CRC32 check on
  write and read; default `off`
  ([service section](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/yaml/service-section)).
- **The documentation does not describe a sender ack** tied to `storage.sync`.
  Its buffering page does not address power loss.

### weir

- **`Durable`** group-fdatasyncs at the batch boundary, then acks every record
  in the batch. **`Buffered`** acks after the in-memory write, before any fsync:
  it survives a process crash but not power loss
  ([WAB](../architecture.md#wab-srcwab)). The tier is chosen per record by the
  producer.
- **The ack is the contract.** "At the `Durable` tier an ack is never a false
  ack: an acked record is on disk and replays after a crash." On Linux that is
  `fdatasync`; macOS uses `F_BARRIERFSYNC`, which is not guaranteed across power
  loss, so production durability is claimed for Linux only
  ([platform support](../platform-support.md#durability-by-platform)).
- **Every record carries a CRC32** in its on-disk header
  ([WAB format](../wab_format.md)).

## What each loses on power failure

| | Documented or measured |
|---|---|
| Vector, memory buffer | Lost if Vector or its host crashes (documented). |
| Vector, disk buffer | Not stated. Sync runs every 500 ms by default. |
| Fluent Bit, `memory` | "More prone to data loss" (documented); no figure. |
| Fluent Bit, `filesystem`, `normal` | Not stated. |
| Fluent Bit, `filesystem`, `full` | Documented for a Fluent Bit crash, not for power loss. |
| weir `Durable` | **None lost** in 5,311 simulated power cuts covering 42,814,591 acked records ([measurement](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md)). |
| weir `Buffered` | Between zero and a ceiling of **~30 MB of acked records** per cut (27.7–31.7 MB), measured at `shard_count = 4` and `wab_segment_max_bytes = 8 MiB`. What sets the ceiling is not established ([ceiling](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-09-27-buffered-ceiling-is-bytes.md)). |

The weir rows come from a fault-injection harness on Linux (`dm-flakey` on
beast), not from pulling the plug on a machine. The harness design and its
limits are in the measurement write-up linked above. Vector and Fluent Bit have not
been run under the same harness, so this table compares weir's measurements
with the other two tools' documentation, not like with like.

**What the guarantee costs.** On beast (Intel i9-9900K, ext4 on a Samsung 850
EVO SATA SSD, weir 4.0.0, one connection, `batch_deadline_ms=1`), `Durable` p50
is 1.1 ms and `Buffered` p50 is 19 µs
([bare-metal capture](../benchmarks/bare-metal.md)). A single-connection
`Durable` ack is one `fdatasync`, so the 1.1 ms is the disk's
([environments](../benchmarks/environments.md#what-to-compare)). Many producers
share each fsync, and so do the records in one `PushBatch`
([wire batching](../benchmarks/wire-batching.md)).

## What Vector and Fluent Bit do that weir does not

- **Sources.** Both collect from systems that do not know about them. Vector's
  sources include `kafka`, `aws_sqs`, HTTP and `socket`
  ([end-to-end acknowledgements](https://vector.dev/docs/architecture/end-to-end-acknowledgements/));
  Fluent Bit's inputs include `tail` and TCP
  ([buffering and storage](https://docs.fluentbit.io/manual/administration/buffering-and-storage)).
  weir accepts records only over its own wire protocol, on
  a Unix socket or TCP with mutual TLS. A producer uses `weir-client` (Rust) or
  implements the [wire protocol](../wire_protocol.md) itself.
- **Transforms.** Vector's `remap` transform runs Vector Remap Language, "an
  expression-oriented language designed for processing observability data"
  ([remap](https://vector.dev/docs/reference/configuration/transforms/remap/)).
  weir treats payloads as opaque bytes and does not inspect or change them
  ([README, non-goals](https://github.com/miki-przygoda/weir#non-goals-v1)).
- **Fan-out.** A Vector source can feed several sinks, and with
  acknowledgements it waits for all of them
  ([guarantees](https://vector.dev/docs/architecture/guarantees/)). weir has one
  sink per daemon.
- **Logs, metrics and traces as data types.** Fluent Bit describes itself as
  "an open source telemetry agent that processes logs, metrics, traces, and
  profiles," and is "a CNCF graduated project under the umbrella of Fluentd"
  ([what is Fluent Bit](https://docs.fluentbit.io/manual/about/what-is-fluent-bit)).
  weir knows records, not telemetry.
- **Destinations.** weir has six sinks: `noop`, `http`, `mysql`, `postgres`,
  and opt-in `clickhouse` and `s3`
  ([sink selection](../operations/configuration.md#sink-selection)). Check your
  destination against theirs before assuming weir covers it.

## When Vector or Fluent Bit is the better choice

- **You are shipping telemetry.** Logs, metrics and traces are what they are
  built for; weir has no notion of them.
- **You need to parse, enrich, sample or redact on the way.** weir cannot.
- **You need more than one destination.** weir delivers to one sink.
- **The sender cannot change.** If the data comes from a file, a syslog stream
  or an existing agent, weir has no way to read it.
- **Your data can tolerate what your buffer mode can lose.** Then the
  strongest argument for weir, the `Durable` ack, does not apply to you.

## When weir is the better choice

- **The sender is your application, and it needs a durable ack.** At `Durable`,
  an ack means the record has been fdatasynced on Linux, and replays after a
  crash. That is a statement about each ack, backed by the power-loss record
  above.
- **You want to choose loss per record.** One connection can mix `Durable` and
  `Buffered` records ([README](https://github.com/miki-przygoda/weir#weir)).
- **The destination is a database and the point is fewer commits.** The SQL
  sinks write a batch with one multi-row `INSERT`, and the Postgres and MySQL
  sinks dedupe replays on a per-record id when their `id_column` is set
  ([MySQL sink](../operations/configuration.md#mysql-sink),
  [Postgres sink](../operations/configuration.md#sink-postgres-sink_type--postgres)).

## Using them together

The two do not exclude each other. weir has an `http` sink with an NDJSON batch
mode for "Loki / ES `_bulk`-style ingesters"
([sinks](../architecture.md#sink-srcsink),
[`sink_http_batch`](../operations/configuration.md#sink_http_batch)). Whether a
given Vector or Fluent Bit HTTP source accepts that framing is a matter for its
own documentation, and it is not tested here.
