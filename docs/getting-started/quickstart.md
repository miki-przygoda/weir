# Quickstart

> **TL;DR** — Build, point `wab_dir` at a directory you own, run.
> The daemon listens on a Unix socket and serves Prometheus metrics
> on `127.0.0.1:9185`. With no sink configured, records are accepted,
> acked, and drained to the built-in no-op sink (nothing downstream);
> configure a sink (HTTP / MySQL / Postgres / ClickHouse) to forward them.

This guide gets you to a running weir daemon in under five minutes,
then walks through pushing your first record and reading the
metrics. For installation options other than building from source,
see [install.md](install.md). For the full configuration surface,
see [configuration.md](../operations/configuration.md).

## Prerequisites

- **Rust 1.88+** (edition 2024) — `rustup toolchain install stable` (1.88
  is the declared MSRV, enforced in CI)
- **Unix host** — Linux or macOS. `weir-server` builds on Windows but is a
  non-functional stub there (no Unix-socket listener), so run the daemon only
  on Linux or macOS. `weir-core` is genuinely cross-platform; `weir-client`
  compiles everywhere but its client type is Unix-only.
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

You should see a few `INFO` startup lines ending with the socket and the metrics
endpoint coming up — the two stable signals that it's ready:

```
INFO weir_server::socket: socket listening  path=/tmp/weir-quickstart/run/weir.sock
INFO weir_server::metrics::server: metrics endpoint bound  addr=127.0.0.1:9185
```

(Exact wording varies by version. You may also see benign `WARN` lines at
startup — e.g. a CPU-affinity note, or on macOS a durability note that
`F_BARRIERFSYNC` is not power-loss-durable; run production on Linux. `workers`
defaults to `shard_count`.)

That's a fully-functional weir daemon. Leave it running and move on
to pushing a record.

## Push your first record

**Fastest path — `weir-ctl push`.** If you built `weir-ctl` (it's in the
workspace), one already-compiled command pushes a record and prints the ack —
nothing to wire up:

```bash
weir-ctl push --socket /tmp/weir-quickstart/run/weir.sock 'hello, weir!'
# ack  12 bytes, Batched
```

**A small load — the bundled example.** From the weir checkout, in a second
terminal, run the ready-made `push_simple` example against the socket from the
run above:

```bash
cargo run --release -p weir-client --example push_simple -- \
    --socket /tmp/weir-quickstart/run/weir.sock --count 5
```

> `push_simple` pushes `--count` records at **each** durability tier (Sync,
> Batched, Buffered), so `--count 5` sends **15** records in total.

> **Before you build a producer — three gotchas that compile and pass a smoke
> test, then bite under load:**
> 1. **`push()` is blocking — never call it directly from an `async fn`.** It
>    blocks the executor thread and starves the runtime (a multi-thread runtime
>    masks it; a single-threaded one freezes). Use the bridge in
>    [Integrating → Producing from an async runtime](integrating.md#producing-from-an-async-runtime).
> 2. **Ack ≠ delivered.** A successful `push` means *durably buffered*, not
>    delivered to your sink. Nothing reaches the sink until a WAB segment seals,
>    and the **default `noop` sink discards everything**. For low volume, set
>    [`wab_segment_max_age_secs`](../operations/configuration.md#wab_segment_max_age_secs)
>    so idle segments seal on a timer; configure a real
>    [`sink_type`](../operations/configuration.md#sink_type) to actually deliver.
>    Details in the [Ack ≠ delivered](#what-just-happened) note below.
> 3. **`Buffered` is not power-loss durable.** It acks *before* fsync. A process
>    crash survives (page cache), but power loss / OS crash is a loss window — use
>    `Sync`/`Batched` for data you can't lose. (macOS is not power-safe at any
>    tier; see [How it earns the "durable" claim](../../README.md#how-it-earns-the-durable-claim).)

**From your own project.** When you're ready to push from your own code,
add just `weir-client` and write a small program against the synchronous
client API. `Durability` is re-exported from `weir-client`, so the basic
producer path needs **only this one dependency** — pull in `weir-core`
separately only if you need the lower-level wire types directly. weir isn't
on crates.io yet (it publishes with 1.0), so until then use a git or path
dependency:

```toml
[dependencies]
# Until the crates.io publish lands with 1.0, depend on the repo directly:
weir-client = { git = "https://github.com/miki-przygoda/weir" }
# (or a local path: weir-client = { path = "../weir/crates/weir-client" })
# weir-core is only needed for the lower-level wire types, not a basic producer.
```

Then write the program against the synchronous client API:

```rust
// examples/hello.rs
use weir_client::{Durability, WeirClient}; // Durability is re-exported here

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = WeirClient::connect("/tmp/weir-quickstart/run/weir.sock")?;

    // push() returns Result<(), ClientError>: Ok(()) means the record was
    // durably acked at the requested tier. There is no ack payload — no
    // server-assigned offset or id comes back. A dropped/ignored result hides a
    // failed push, so `push` is #[must_use]; handle it (here via `?`).
    let payload = b"hello, weir!";
    client.push(payload, Durability::Sync)?; // Sync = fsync before ack
    println!("pushed {} bytes", payload.len());

    // Verify the daemon is healthy.
    client.health_check()?;
    println!("health check ok");

    Ok(())
}
```

The client API is synchronous — one blocking socket round-trip per call, so you
don't *need* an async runtime to use it. (If you're calling from inside one,
`push()` blocks the executor thread — see
[Integrating → Producing from an async runtime](integrating.md#producing-from-an-async-runtime)
for the bridge pattern.) Add `weir-client` to your example/dev deps, then:

```bash
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

You'll see weir's Prometheus metric families (the full catalogue is in
[monitoring.md](../monitoring.md)):

```
# HELP weir_records_accepted_total Total records accepted from producers, by durability tier
# TYPE weir_records_accepted_total counter
weir_records_accepted_total{tier="sync"} 1
# HELP weir_records_ack_total Total records acknowledged to producers, by durability tier
# TYPE weir_records_ack_total counter
weir_records_ack_total{tier="sync"} 1
...
```

The full metrics catalogue — every family, its labels, and the alerts
that matter — lives in [monitoring.md](../monitoring.md).

## What just happened

When you called `client.push(...)`:

1. The client opened the Unix socket and sent a 16-byte header + your
   12-byte payload + a 4-byte CRC32.
2. The daemon validated the header (magic → version → CRC → payload
   cap), allocated a 12-byte buffer, read the payload, verified the
   payload CRC.
3. The record was pushed onto the bounded queue. A worker thread
   picked it up and routed it to shard 0's flusher.
4. The shard's flusher accumulated it into a batch. Because you asked
   for `Sync` durability, the batch was fsynced immediately (no
   waiting for siblings).
5. The daemon sent the Ack frame back. Your client returned.
6. The drain thread later read the sealed WAB segment, committed it
   via the configured sink (the built-in no-op sink by default, which
   accepts everything), and wrote a `.confirmed` sidecar so the
   segment can be safely deleted.

For the full pipeline detail, see
[architecture.md](../architecture.md).

> **Ack ≠ delivered.** A successful `push` means the record is durably
> *buffered* (fsynced for `Sync`/`Batched`), **not** that it has reached the
> sink. The drain forwards records only after a WAB segment *seals* — at its
> size threshold (`segment_max_bytes`, default 256 MiB) or on shutdown. So with
> a handful of small records the sink isn't touched until you stop the daemon,
> and with the default `noop` sink nothing is forwarded at all (it's a soak-test
> sink). Confirm acceptance with `weir_records_ack_total`, not the sink-commit
> metric. To make a low-volume deployment drain promptly, set
> `wab_segment_max_age_secs` (e.g. `2`) so an idle segment seals on a timer
> instead of waiting to fill 256 MiB; for a one-off demo you can also just send
> `SIGTERM` (graceful shutdown seals the open segment).

## Cleaning up

```bash
# Stop the daemon: Ctrl-C in its terminal, or kill the specific PID you started:
kill "$(pgrep -f -- '--wab-dir /tmp/weir-quickstart')"
# (This matches only the quickstart daemon by its --wab-dir flag. Avoid
#  `pkill weir-server` on a shared host — it kills every weir daemon, not
#  just this quickstart's.)

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

**`invalid path for 'wab_dir': cannot canonicalize '…': No such file or directory`**
weir does not create the WAB directory (Postgres model). Create it
with `mkdir -p` before starting the daemon.

**Metrics endpoint not reachable from another container**
The metrics server binds to `127.0.0.1:9185` by default (localhost
only). To expose it, set `metrics_bind = "0.0.0.0"` (CLI
`--metrics-bind`, env `WEIR_METRICS_BIND`) and map the port in Docker:
`-p 127.0.0.1:9185:9185`. A `0.0.0.0` bind is **not** a security
boundary — control access via the port mapping or firewall.

**`Nack(PayloadTooLarge)`**
Your record exceeded the configured `max_payload_bytes` (default 16
MiB). Either raise the config value (up to the 16 MiB hard cap) or
shrink the payload.

## Next steps

- [Integrating & extending](integrating.md) — use weir's crates without the
  daemon: produce from your own app, write a custom sink, or read the WAB directly.
- [Configuration reference](../operations/configuration.md) — every
  option, default, range, and when to tune.
- [Install options](install.md) — building from source, container
  images, Docker Compose, `cargo install`, and pre-built release binaries.
- [Wire protocol](../wire_protocol.md) — frame layout if you're
  writing a non-Rust client.
- [Security threat model](../security/threat-model.md) — what weir
  defends against and what is the operator's responsibility.
