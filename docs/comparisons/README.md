# Comparisons

These pages compare weir with tools people already use for the same job. Each
one cites the other tool's own documentation for every claim about it, and this
repository for every claim about weir. Each has a section on when the other
tool is the better choice, and those sections are meant to be used.

The documentation of the other tools was read on 2026-10-29. If something has
changed since, the linked page wins over this one.

## The pages

**[ClickHouse async inserts, Buffer tables and Kafka](clickhouse-async-inserts.md).**
All four exist because many small inserts make ClickHouse create too many
parts. Async inserts with `wait_for_async_insert=1` solve that with a setting,
and are the right answer when producers can wait for ClickHouse. weir fits when
the producer must get a durable ack from local disk without waiting, and records
must survive a ClickHouse outage. It costs a daemon per host and a raw `String`
column to parse on the ClickHouse side.

**[Vector's disk buffer and Fluent Bit's filesystem storage](vector-fluent-bit.md).**
The comparison is about one question: when the sender is acked relative to
fsync, and what is lost on power failure. weir's `Durable` ack follows an
`fdatasync` and has been tested against simulated power cuts; neither tool's
documentation states what its disk buffer guarantees across a power loss.
Vector and Fluent Bit collect, transform and route telemetry, which weir does
not do at all.

**[A transactional outbox, a SQLite queue or an in-process disk queue](outbox-sqlite.md).**
weir cannot be part of your database transaction. When a record must exist if
and only if a business change commits, the outbox is the answer. weir fits when
the record is itself the write to batch, several processes produce it, and you
want the drain and its retries run for you.

## When weir is the wrong choice

From the pages above and weir's own
[non-goals](https://github.com/miki-przygoda/weir#non-goals-v1):

- **The write must be atomic with other state.** weir acks independently of
  your database. Use an outbox.
- **More than one consumer reads the stream, or you need to replay history.**
  weir delivers to one sink and deletes a segment once it is confirmed. Use a
  broker.
- **You need to parse, transform or route the data.** weir treats payloads as
  opaque bytes. Use a telemetry pipeline.
- **Your producers can wait for the database.** Then batch in the client, or
  use the database's own batching, such as ClickHouse async inserts. weir is
  one more process to run.
- **Losing in-flight data on a crash is acceptable.** An in-process channel is
  simpler.
- **The producer runs on Windows only, or the guarantee must hold on macOS.**
  The daemon is Unix-only, and the power-loss guarantee is claimed for Linux
  only ([platform support](../platform-support.md)).
