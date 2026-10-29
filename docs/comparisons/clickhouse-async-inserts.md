# weir and ClickHouse async inserts, Buffer tables, and Kafka

**Conclusion first.** If your producers can reach ClickHouse and can wait for
it, use ClickHouse's own async inserts with `wait_for_async_insert=1`: nothing
extra to run, and the data lands in typed columns. weir earns its place when the
producer must get an ack from local disk without waiting on ClickHouse, and
records must survive a ClickHouse outage, a weir crash or (at the `Durable`
tier) a power cut on the producer's host. It costs a daemon per host, a raw
`String` column you parse on the ClickHouse side, and at-least-once delivery
that you must make idempotent.

Claims about ClickHouse below link to its documentation, read on 2026-10-29.
Claims about weir link to this repository.

## The problem all four solve

ClickHouse turns every synchronous `INSERT` into at least one data part on disk,
and background merges combine them. Too many small inserts outrun the merges:

- "Each synchronous `INSERT` creates at least one data part," and frequent small
  inserts are a common trigger of the error
  ([Too many parts](https://clickhouse.com/docs/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts)).
- Past `parts_to_delay_insert` (default 1000) active parts in one partition,
  ClickHouse slows inserts down. Past `parts_to_throw_insert` (default 3000), the
  insert fails with `Too many parts (N). Merges are processing significantly
  slower than inserts`
  ([parts_to_* settings](https://clickhouse.com/docs/reference/settings/merge-tree-settings/parts-to)).
- ClickHouse recommends batches "of at least 1,000 rows, and ideally between
  10,000–100,000 rows"
  ([selecting an insert strategy](https://clickhouse.com/docs/concepts/best-practices/selecting-an-insert-strategy)).

Every option on this page is a way to get there when the producer emits one
record at a time.

## The options

### Async inserts (`async_insert=1`)

Batching moves from the client to the server. ClickHouse writes incoming data
to an in-memory buffer and flushes it when the first of these is reached:
`async_insert_max_data_size` (default 100 MiB), `async_insert_busy_timeout_ms`
(default 200 ms; 1000 ms on Cloud) or `async_insert_max_query_number` (default
450). There are "multiple buffers per insert shape and settings combination,"
kept per node
([asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)).

The ack depends on `wait_for_async_insert`
([same page](https://clickhouse.com/docs/optimize/asynchronous-inserts)):

- **`1`, the default.** "ClickHouse only acknowledges the insert after the data
  is successfully flushed to disk." Errors come back to the client.
- **`0`.** "The server acknowledges the insert as soon as the data is buffered,
  without waiting for it to reach storage," with "no guarantee the data will be
  persisted," and errors surface only at flush time. ClickHouse calls this "very
  risky" and makes a "strong recommendation" to use
  `async_insert=1,wait_for_async_insert=1`.

"Flushed to disk" means the part was written. Whether that write is fsynced is a
separate MergeTree setting, `fsync_after_insert`, which defaults to `0`
([fsync settings](https://clickhouse.com/docs/reference/settings/merge-tree-settings/fsync)).
ClickHouse's transactions guide says "either insert_quorum into different AZ or
fsync should be enabled to ensure durable inserts in the typical setup"
([transactional support](https://clickhouse.com/docs/guides/developer/transactional)).

Since 26.2, ClickHouse deduplicates async inserts as well as synchronous ones,
per user query rather than per flushed batch
([asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)).

Async inserts do not remove the part-count problem in every case: each flush
creates at least one part per partition key value, so a high-cardinality
partition key can still produce "too many parts" (same page).

### Buffer table engine

A Buffer table "buffers the data to write in RAM, periodically flushing it to
another table." Flushes happen when all `min*` thresholds or any `max*`
threshold (time, rows, bytes) is met. "If the server is restarted abnormally,
the data in the buffer is lost." ClickHouse names async inserts as "a
recommended alternative," says it "can only recommend using a Buffer table in
rare cases," and notes that row reordering and part-size changes break
deduplication for replicated targets
([Buffer engine](https://clickhouse.com/docs/engines/table-engines/special/buffer)).
The async inserts page calls itself "the modern replacement for Buffer tables"
([asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)).

### Kafka in front of ClickHouse

Producers write to Kafka; ClickHouse consumes in blocks. ClickHouse documents
several routes: the Kafka table engine, the Kafka Connect Sink, ClickPipes,
Vector and others. Its comparison lists the Kafka table engine and ClickPipes as
at-least-once, and the Connect Sink as able to "be configured to support
exactly-once semantics"
([Kafka integrations](https://clickhouse.com/docs/integrations/kafka)).

With the Kafka table engine, a materialized view pulls messages into a MergeTree
table in blocks of up to `max_insert_block_size` rows, or whatever has arrived
after `stream_flush_interval_ms`. Its page documents one sharp edge: the engine
"can silently lose already-consumed rows if the OS page cache is discarded
before the inserted data is written to disk," because the offset may already be
committed, and inserted rows are "only durable once the target part is fsynced,
which does not happen synchronously by default"
([Kafka engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka)).

### weir with the ClickHouse sink

The producer pushes each record to a weir daemon on its own host over a Unix
socket (or TCP with mutual TLS). weir appends it to an on-disk write-ahead
buffer (WAB) and acks according to the record's tier, then a drain thread
delivers it to ClickHouse later
([architecture](../architecture.md#data-flow)).

- **`Durable`** acks after a group `fdatasync` at the batch boundary;
  **`Buffered`** acks after the in-memory write, before any fsync, and survives
  a process crash but not power loss
  ([WAB](../architecture.md#wab-srcwab)).
- The sink sends one `INSERT INTO db.table (col) FORMAT RowBinary` per batch,
  with `insert_deduplication_token = sha256(batch)`. It needs the opt-in
  `clickhouse-sink` build feature
  ([ClickHouse sink](../operations/configuration.md#sink-clickhouse-sink_type--clickhouse)).
- Records go into **one `String` column** as opaque bytes; the target column
  must be `String` (`crates/weir-server/src/sink/clickhouse.rs`). weir does not
  parse or type your data. To get typed columns you add a materialized view on
  the ClickHouse side, which "runs a query on blocks of data as they're inserted
  into a table" and inserts the result into a target table
  ([incremental materialized views](https://clickhouse.com/docs/materialized-view/incremental-materialized-view)).
- Delivery is at-least-once. A crash between a commit and its confirmation
  replays the segment ([drain](../architecture.md#drain-srcdrain)). The dedup
  token only works on a `Replicated*MergeTree` or a `MergeTree` with
  `non_replicated_deduplication_window` set
  ([idempotency](../operations/configuration.md#idempotency-dedup-token)), and
  ClickHouse's own default for that window is `0`, which deduplicates nothing
  ([deduplicating inserts on retries](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)).

## Side by side

| | Async inserts, `wait=1` | Async inserts, `wait=0` | Buffer table | Kafka in front | weir `Durable` | weir `Buffered` |
|---|---|---|---|---|---|---|
| Producer acked when | the flush has written the part | data is in ClickHouse's memory buffer | data is in ClickHouse's RAM | outside this page | after `fdatasync` of the local WAB | after the local in-memory write |
| Producer waits on ClickHouse | yes, up to the flush threshold | no | no | no | no | no |
| Documented loss of acked data | no per-part fsync unless `fsync_after_insert=1` | "no guarantee the data will be persisted" | lost "if the server is restarted abnormally" | Kafka engine: consumed rows lost if the page cache is discarded first | none in 5,311 simulated power cuts | 0 to ~30 MB per power cut, measured |
| New infrastructure | none | none | a table per node | Kafka and a consumer | a daemon per producer host | a daemon per producer host |
| Typed columns on arrival | yes | yes | yes | yes | no: one `String` column | no: one `String` column |
| Dedup on retry | per query, since 26.2 | per query, since 26.2 | breaks replicated dedup | depends on the route | batch token, needs a dedup window | batch token, needs a dedup window |

Each cell is cited in the sections above or the paragraphs below. Kafka's own
producer ack settings are outside the scope of this page.

**weir's measured numbers.** On beast (Intel i9-9900K, ext4 on a Samsung 850
EVO SATA SSD, weir 4.0.0, one connection, `batch_deadline_ms=1`), `Durable` p50
is 1.1 ms and p99 3.7 ms, and `Buffered` p50 is 19 µs
([bare-metal capture](../benchmarks/bare-metal.md)). On one connection a
`Durable` ack is one `fdatasync`, so that number is the disk's
([environments](../benchmarks/environments.md#what-to-compare)). Under hours of
sustained load on the same SSD one producer averages 352 rec/s at `Durable`, not
the 877 a short run shows, and the cause is the storage
([sustained load](../benchmarks/sustained-load.md)).

**weir's power-loss record.** 5,311 simulated power cuts on Linux, 42,814,591
acknowledged `Durable` records, none lost
([first power-loss measurement](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md)).
`Buffered` lost between zero and a ceiling of about 30 MB of acked records
(27.7–31.7 MB across 64, 256 and 1024-byte records), measured with
`shard_count = 4` and `wab_segment_max_bytes = 8 MiB`. What sets that ceiling is
not established; size your own exposure from a measurement on your own
configuration ([the ceiling is bytes](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-09-27-buffered-ceiling-is-bytes.md)).

## When async inserts are the better choice

- **Your producers can wait for ClickHouse.** With `wait_for_async_insert=1` the
  client learns of every error and ClickHouse does the batching. ClickHouse
  recommends it for exactly the case of clients that cannot batch.
- **You do not want another process.** Async inserts are a setting. weir is a
  daemon on every producer host, with its own disk, config and monitoring.
- **You want typed columns without extra work.** Async inserts write into your
  table's own columns. weir delivers opaque bytes into one `String` column.
- **You want ClickHouse's dedup semantics.** Since 26.2 async inserts deduplicate
  per user query. weir's token is per drain batch, so it only works if
  `sink_max_batch_size` never changes across a restart
  ([`sink_max_batch_size`](../operations/configuration.md#sink_max_batch_size)).

## When a Buffer table is the better choice

ClickHouse itself recommends async inserts over Buffer tables except in rare
cases, and this page has nothing to add to that.

## When Kafka is the better choice

- **More than one consumer reads the stream.** With Kafka, a second consumer
  is a second consumer group; the Kafka engine page says to "create a copy of
  the table with another group name" to read the data twice
  ([Kafka engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka)).
  weir has one sink per daemon and no fan-out
  ([README, non-goals](https://github.com/miki-przygoda/weir#non-goals-v1)).
- **You need to re-read delivered data.** weir deletes a segment once its sink
  confirms it ([drain](../architecture.md#drain-srcdrain)), so there is nothing
  to re-read.
- **You already run Kafka.** Adding a ClickHouse consumer is less new
  infrastructure than adding weir.
- **The buffer must survive the loss of the producer host.** weir's WAB is on
  that host's disk ([`wab_dir`](../operations/configuration.md#wab_dir)), so
  records not yet delivered go with it.

## When weir is the better choice

- **The producer must not wait for ClickHouse.** A `Durable` ack is one local
  fdatasync away, and a ClickHouse outage does not reach the producer: records
  wait in the WAB, the drain retries with backoff, and a stranded segment is
  re-drained when the sink recovers
  ([`sink_url`, retries](../operations/configuration.md#sink_url)). With
  `wait_for_async_insert=1` an outage is an error your producer must handle.
- **Acked records must survive a crash on the producer side.** `Durable` is the
  tier with the power-loss record above. Async inserts with `wait=0` and Buffer
  tables hold acked data in ClickHouse's memory.
- **You want batching without a broker.** weir is one binary per host. It is not
  a replacement for Kafka's fan-out or retention.

## If you use weir with ClickHouse

- **Raise [`sink_max_batch_size`](../operations/configuration.md#sink_max_batch_size)
  to at least 1,000.** Its default is 100, which is below ClickHouse's own
  minimum recommendation. Its range is 1–10,000. Each `Sink::commit` is one
  `INSERT` and so at least one part.
- **Then never change it.** The dedup token hashes the batch, so a different
  batch size re-splits a replayed segment into batches ClickHouse has not seen,
  and you get duplicates (same section).
- **Use a dedup-capable engine,** and keep the dedup window larger than the
  number of inserts that can happen during a retry: ClickHouse warns that
  deduplication can fail if "more than `*_deduplication_window` other insert
  operations occur during the retry sequence"
  ([deduplicating inserts on retries](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)).
- **Set a seal timer.** Records reach the sink only when a WAB segment seals,
  and by default that is at 256 MiB or shutdown. Set
  [`wab_segment_max_lifetime_secs`](../operations/configuration.md#wab_segment_max_lifetime_secs)
  to bound delivery latency.
- **Set [`wab_max_bytes`](../operations/configuration.md#wab_max_bytes).** It
  defaults to `0` (no cap), so a long ClickHouse outage fills the disk until you
  set one.
