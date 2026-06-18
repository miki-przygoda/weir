# weir-server

The [weir](https://github.com/miki-przygoda/weir) daemon: a durable
write-ahead-buffer for a flaky uplink.

Producers push records over a Unix socket (or TCP + mTLS); records are written to
CRC32-checked WAB segments, fsynced per the requested durability tier, acked, then
drained asynchronously to a sink (http / mysql / postgres / clickhouse / noop)
with N→1 commit compression and at-least-once delivery. Crash recovery replays
unconfirmed segments. Crown invariant: **an ack is never a false ack.** Unix only.

See the [workspace README](https://github.com/miki-przygoda/weir) for install,
configuration, and operations docs.
