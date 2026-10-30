# weir and a transactional outbox, a local SQLite queue, or an in-process disk queue

**Conclusion first.** weir cannot take part in your application's database
transaction. If a record must be written if and only if a business change
commits, use a transactional outbox: no external buffer, weir included, can give
you that. weir fits when there is no business transaction to share: the record
is itself the write you want to batch, several processes or languages produce
it, and you want the buffer, its drain and its retries outside your
application. A local SQLite queue or an in-process disk queue sits in between:
less to run than weir, but the drain, the retries and the process boundary are
yours to build.

Claims about the outbox pattern, SQLite and yaque below link to their
documentation, read on 2026-10-29. Claims about weir link to this repository.

## The four options

### Transactional outbox

The application writes each outgoing message to an outbox table "as part of the
same transaction that updates its business entities," and a separate relay
publishes from the table. The pattern's stated benefit is that "messages are
guaranteed to be sent if and only if the database transaction commits," without
two-phase commit. The relay "might publish a message more than once," so
consumers must be idempotent. Two relay designs are named: transaction log
tailing and polling publisher
([transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)).

Debezium is one log-tailing relay. Its outbox event router says "an outbox
table functions as a queue; updates to records in an outbox table are not
allowed," and filters out `DELETE`s on it
([outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)).

### A local SQLite queue

A table in a SQLite file on the producer's host, written by the application and
read by a drain loop you write.

- **Durability depends on `synchronous`.** In WAL mode, `FULL` is ACID. With
  `NORMAL`, "a transaction committed in WAL mode with synchronous=NORMAL might
  roll back following a power loss or system crash," though transactions are
  durable across application crashes in every mode
  ([PRAGMA synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous)).
  The WAL page says the same: with `NORMAL`, writers "omit this sync" on
  commit, and transactions "might rollback following a power failure or hard
  reset" ([WAL](https://www.sqlite.org/wal.html)).
- **One writer at a time.** "Since there is only one WAL file, there can only be
  one writer at a time" ([WAL](https://www.sqlite.org/wal.html)).
- **One host.** "All processes using a database must be on the same host
  computer; WAL does not work over a network filesystem" (same page).
- **Atomic with business state only if the business state is in the same
  file.** A SQLite transaction covers the SQLite database it runs in.

### An in-process disk queue (yaque, as an example)

yaque is "yet another disk-backed persistent queue (and mutex) for Rust" that
"implements an SPSC channel using your OS' filesystem." Its documentation says
"writing to the queue is an atomic operation," that data is safe if the program
panics, and that "all the queue state is written to the disk when the queue is
dropped." It does not mention fsync. After an unclean stop, recovery either
replays ("some elements may be replayed") or erases the smallest segment. Its
own status notes say "this is a brand new project" and "there are probably
unknown bugs hidden in some corner case" (version 0.6.6,
[docs.rs/yaque](https://docs.rs/yaque/latest/yaque/)).

Other crates make other choices. Read the durability section of whichever you
pick for the same three things: when it fsyncs, what an unclean stop does, and
how many producers it supports.

### weir

A daemon on the producer's host. The application pushes each record over a Unix
socket (or TCP with mutual TLS); weir appends it to an on-disk write-ahead
buffer, acks, and a drain thread delivers batches to one sink
([architecture](../architecture.md#data-flow)).

- **`Durable`** acks after a group `fdatasync`; **`Buffered`** acks before any
  fsync and survives a process crash but not power loss
  ([WAB](../architecture.md#wab-srcwab)).
- **Delivery is at-least-once.** A crash after a sink commit but before the
  segment is confirmed replays it, so the sink must dedupe
  ([drain](../architecture.md#drain-srcdrain)). The Postgres and MySQL sinks do
  this on a per-record id with a `UNIQUE` column
  ([Postgres reference schema](../operations/configuration.md#reference-schema)).
- **No transaction spans weir and anything else.** A push is one record; a
  `PushBatch` answers with a per-record bitmap, so even a batch is not
  all-or-nothing ([batch push](../wire_protocol.md#batch-push--pushbatch--ackbatch)).

## weir cannot be in your transaction

This is the deciding difference, so it is worth spelling out. Suppose an order
is written to Postgres and an event about it must reach the warehouse.

- **Commit, then push to weir.** If the process dies between the two, the order
  exists and the event does not.
- **Push to weir, then commit.** If the commit fails or the process dies before
  it, the event exists for an order that does not.
- **Push inside the transaction.** weir acks independently of your database.
  A rollback does not un-push a record.

There is no ordering of the two steps that removes the window. weir's
durability guarantee is about weir's ack, not about any other system's state.
The outbox removes the window by making the event a row in the same
transaction.

## Side by side

| | Outbox table | SQLite queue | In-process queue (yaque) | weir |
|---|---|---|---|---|
| Atomic with business state | **yes**, same DB transaction | only for state in the same SQLite file | no | **no** |
| Where the buffer lives | your business database | a file on the producer host | a directory on the producer host | a directory on the producer host |
| Durable ack | the database's commit | `synchronous=FULL` in WAL mode | fsync not documented | `Durable` tier: `fdatasync` |
| Who writes the drain | you, or a relay such as Debezium | you | you | weir, with retries, dead-lettering and six sinks |
| Producers | anything that writes the DB | one writer at a time, one host | one sender, one receiver | many connections, any language that speaks the [wire protocol](../wire_protocol.md) |
| Survives the producer process dying | yes | yes | data safe on panic; signals not handled | yes, the buffer is another process |
| Queryable | yes, it is a table | yes | no | no, replay is the only read path |

Sources: the sections above, plus the weir non-goals
([README](https://github.com/miki-przygoda/weir#non-goals-v1)) for the last row.

**What a weir ack costs.** On beast (Intel i9-9900K, ext4 on a Samsung 850 EVO
SATA SSD, weir 4.0.0, one connection, `batch_deadline_ms=1`), `Durable` p50 is
1.1 ms and `Buffered` p50 is 19 µs
([bare-metal capture](../benchmarks/bare-metal.md)). A single-connection
`Durable` ack is one `fdatasync`, so that figure is the disk's
([environments](../benchmarks/environments.md#what-to-compare)), and a local
SQLite commit with `synchronous=FULL` pays an fsync too. Measure both on your
own disk before choosing on latency. No weir benchmark compares the two.

**What it loses.** 5,311 simulated power cuts, 42,814,591 acked `Durable`
records, none lost
([measurement](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md)).
`Buffered` lost between zero and ~30 MB of acked records per cut, measured at
`shard_count = 4` and `wab_segment_max_bytes = 8 MiB`; what sets that ceiling is
not established
([ceiling](https://github.com/miki-przygoda/weir/blob/main/docs/benchmarks/chaos-phase2/2026-09-27-buffered-ceiling-is-bytes.md)).

## When the outbox is the better choice

- **The event must exist if and only if the business change commits.** This is
  the outbox's whole purpose, and weir cannot provide it.
- **You already have the database.** The outbox adds a table and a relay, not a
  new kind of process on every host.
- **You want to query or repair pending events with SQL.** An outbox is a table.
  weir's buffer is not queryable.

The cost is that every event is a write to the database you may be trying to
protect, inside its transactions.

## When a local SQLite queue is the better choice

- **One process produces, on one host,** and you are comfortable writing the
  drain, the retry loop and the dead-letter handling yourself.
- **Your state already lives in that SQLite file,** so the queue row can be
  atomic with it.
- **You want no extra daemon.** SQLite is a library in your process.

## When an in-process disk queue is the better choice

- **The producer and consumer are the same program,** and you want a durable
  channel between two parts of it with no socket and no daemon.
- **You can check its fsync behaviour** against your loss tolerance, since that
  is the guarantee you are relying on.

## When weir is the better choice

- **The record is the write.** There is no business transaction to share: the
  job is to take many small writes and land them as fewer, larger commits in
  Postgres, MySQL, ClickHouse, an HTTP endpoint or S3
  ([sink selection](../operations/configuration.md#sink-selection)).
- **Several processes or languages produce.** weir takes many connections, each
  pinned to a shard ([socket layer](../architecture.md#socket-layer-srcsocket--unix-only)).
- **You do not want to write the drain.** Retries with backoff, stranded-segment
  recovery, dead-lettering with a size cap, quarantine of corrupt segments and
  Prometheus metrics come with it ([drain](../architecture.md#drain-srcdrain),
  [monitoring](../monitoring.md)).
- **The buffer should outlive the producer process.** The WAB belongs to the
  daemon, so a producer that crashes after an ack has nothing to recover.

## Combining them

An outbox relay can be a weir producer. The outbox keeps the event atomic with
the business change; the relay pushes outbox rows to weir and deletes them once
acked; weir batches delivery to the destination. That adds a moving part, and
the destination must tolerate duplicates from both stages, because the outbox
relay and weir's drain are each at-least-once.
