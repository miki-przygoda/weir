# Quickstart

> **TL;DR** — Build, point `wab_dir` at a directory you own, run.
> The daemon listens on a Unix socket and serves Prometheus metrics
> on `:9185`. With the `NoopSink` placeholder, records are accepted
> and acked but not forwarded anywhere — that's intentional in v0.x.

This guide gets you to a running weir daemon in under five minutes,
then walks through pushing your first record and reading the
metrics. For installation options other than building from source,
see [install.md](install.md). For the full configuration surface,
see [configuration.md](../operations/configuration.md).

## Prerequisites

- **Rust 1.85+** (edition 2024) — `rustup toolchain install stable`
- **Unix host** — Linux or macOS. weir-server does not run on Windows;
  the `weir-core` and `weir-client` crates are cross-platform.
- **A writable directory** — for the WAB segments. Anywhere works;
  the examples below use `/tmp/weir-quickstart/wab`.

## 60-second run

```bash
# 1. Build the daemon (release mode; debug mode is ~20× slower).
cargo build --release -p weir-server

# 2. Create the directories the daemon needs.
mkdir -p /tmp/weir-quickstart/wab /tmp/weir-quickstart/run
chmod 0700 /tmp/weir-quickstart/run

# 3. Run.
./target/release/weir-server \
    --wab-dir /tmp/weir-quickstart/wab \
    --socket-path /tmp/weir-quickstart/run/weir.sock
```

You should see output like:

```
2026-05-26T15:23:01Z  INFO weir_server: starting weir-server  socket=/tmp/weir-quickstart/run/weir.sock wab_dir=/tmp/weir-quickstart/wab shards=1 workers=2
2026-05-26T15:23:01Z  INFO weir_server::wab: scanning for unsealed WAB segments
2026-05-26T15:23:01Z  INFO weir_server::socket: socket listening  path=/tmp/weir-quickstart/run/weir.sock
2026-05-26T15:23:01Z  INFO weir_server::metrics::server: metrics endpoint bound  addr=0.0.0.0:9185
2026-05-26T15:23:01Z  INFO weir_server: pipeline assembled; awaiting connections
```

That's a fully-functional weir daemon. Leave it running and move on
to pushing a record.

## Push your first record

From another terminal, write a small Rust program against the
`weir-client` crate:

```rust
// examples/hello.rs
use weir_client::{WeirClient, Durability};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = WeirClient::connect("/tmp/weir-quickstart/run/weir.sock").await?;

    // Push a 12-byte record with Sync durability (fsync before ack).
    let payload = b"hello, weir";
    client.push(payload, Durability::Sync).await?;
    println!("pushed {} bytes", payload.len());

    // Verify the daemon is healthy.
    client.health_check().await?;
    println!("health check ok");

    Ok(())
}
```

```bash
# Add weir-client to your Cargo.toml dev/example deps, then:
cargo run --release --example hello
```

You should see:

```
pushed 12 bytes
health check ok
```

In the daemon's terminal, you'll see the WAB receive the record (with
`log_level = "debug"` you'd see per-record activity; at `info` the
record passes through silently — which is the production default).

## Verify the metrics endpoint

```bash
curl http://localhost:9185/metrics | head -30
```

You'll see all 19 Prometheus metric families:

```
# HELP weir_records_accepted_total Records accepted from producers
# TYPE weir_records_accepted_total counter
weir_records_accepted_total{tier="sync"} 1
# HELP weir_records_ack_total Records acknowledged to producers
# TYPE weir_records_ack_total counter
weir_records_ack_total{tier="sync"} 1
...
```

The full metrics catalogue lives in
[architecture.md](../architecture.md#metrics) (will move to
`docs/reference/metrics.md` in a later phase).

## What just happened

When you called `client.push(...)`:

1. The client opened the Unix socket and sent a 16-byte header + your
   12-byte payload + a 4-byte CRC32.
2. The daemon validated the header (magic → version → CRC → payload
   cap), allocated a 12-byte buffer, read the payload, verified the
   payload CRC.
3. The record was pushed onto the global queue. A worker thread
   picked it up and routed it to shard 0's bridge channel.
4. The bridge thread accumulated it into a batch. Because you asked
   for `Sync` durability, the batch was fsynced immediately (no
   waiting for siblings).
5. The daemon sent the Ack frame back. Your client returned.
6. The drain thread later read the sealed WAB segment, "committed"
   it via the `NoopSink` (which accepts everything), and wrote a
   `.confirmed` sidecar so the segment can be safely deleted.

For the full pipeline detail, see
[architecture.md](../architecture.md).

## Cleaning up

```bash
# Stop the daemon (Ctrl-C in its terminal, or):
pkill weir-server

# Remove the quickstart data.
rm -rf /tmp/weir-quickstart
```

## Common issues

**`bind: address already in use`**
A stale socket from a previous run wasn't cleaned up. Either delete
it manually (`rm /tmp/weir-quickstart/run/weir.sock`) or, more
typically, the previous daemon is still running. Check with
`pgrep weir-server`.

**`refusing to remove non-socket file at <path>`**
A regular file (or symlink) exists at the socket path. The daemon
refuses to remove it as a safety check — manually inspect and
remove if it's safe to do so.

**`config error: wab_dir does not exist`**
weir does not create the WAB directory (Postgres model). Create it
with `mkdir -p` before starting the daemon.

**Metrics endpoint not reachable from another container**
The metrics server binds to `0.0.0.0:9185`. In Docker, ensure the
port is mapped: `-p 127.0.0.1:9185:9185`. The 0.0.0.0 bind is **not**
a security boundary — control access via the port mapping or
firewall, not the bind address.

**`Nack(PayloadTooLarge)`**
Your record exceeded the configured `max_payload_bytes` (default 16
MiB). Either raise the config value (up to the 16 MiB hard cap) or
shrink the payload.

## Next steps

- [Configuration reference](../operations/configuration.md) — every
  option, default, range, and when to tune.
- [Install options](install.md) — building from source, container
  images, Docker Compose, and (planned) `cargo install`.
- [Wire protocol](../wire_protocol.md) — frame layout if you're
  writing a non-Rust client.
- [Security threat model](../security/threat-model.md) — what weir
  defends against and what is the operator's responsibility.
