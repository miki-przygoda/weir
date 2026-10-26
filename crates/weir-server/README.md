# weir-server

The [weir](https://github.com/miki-przygoda/weir) daemon: a durable
write-ahead-buffer for a flaky uplink.

Producers push records over a Unix socket (or TCP + mTLS); records are written to
CRC32-checked WAB segments, fsynced per the requested durability tier, acked, then
drained asynchronously to one of six sinks (http / mysql / postgres / noop by
default; clickhouse and s3 opt-in) with N→1 commit compression and at-least-once
delivery. Crash recovery replays unconfirmed segments. Crown invariant: **an ack
is never a false ack.** Unix only.

One daemon owns a `wab_dir`: it takes `<wab_dir>/.weir.lock` before recovery
reads the WAB, and a second instance on the same directory refuses to start.
The metrics listener also serves `/livez`, `/readyz`, `/startupz` and the
`weir.status/v1` document at `/status`, and an admin Unix socket lets
`weir-ctl` read status, reload TLS material and pause or resume delivery.
Shutdown is bounded: on SIGTERM the daemon stops accepting, gives the drain
`drain_shutdown_budget_secs` (default 10) to deliver, and leaves the rest
sealed on disk for the next start.

See the [workspace README](https://github.com/miki-przygoda/weir) for install,
configuration, and operations docs.
