# Integrating & extending weir

weir isn't only a daemon you run as-is — it's a set of crates you can compose.
Each piece is usable **without** pulling in the daemon, so you can take just the
parts you need: produce records from your own app, forward to your own
destination, read the on-disk buffer directly, or run operations against a live
daemon. The crate boundaries are real (enforced by the dependency graph, not
convention — see [Architecture → Workspace & crate boundaries](../architecture.md)).

## Which crate do I need?

| You want to… | Use | Pulls in the daemon? |
|--------------|-----|----------------------|
| Run the buffer as a service | `weir-server` (the `weir-server` binary) | it *is* the daemon |
| Push records from your own app | `weir-client` | no |
| Forward records to a custom destination | `weir-sink-sdk` (implement `Sink`) | no (to author + test) |
| Read / inspect / recover WAB segments off disk | `weir-wab` | no |
| Operate a running daemon from the shell | `weir-ctl` | no |
| Build a producer/client in another language | the [wire protocol](../wire_protocol.md) (no Rust dep at all) | no |

All weir crates are sibling directories under `crates/` — `weir-client`,
`weir-sink-sdk`, `weir-wab`, `weir-core`, `weir-server`, `weir-ctl` — so a
`path` dependency points at `crates/<name>` (e.g.
`weir-client = { path = "../weir/crates/weir-client" }`).

## Produce from your own program

The synchronous client is one blocking round-trip per push — you don't *need* an
async runtime to use it. See the [Quickstart](quickstart.md#push-your-first-record)
for the full example and the `Cargo.toml` dependency block; in short:

```rust
use weir_client::WeirClient;
use weir_core::Durability;

let mut client = WeirClient::connect("/run/weir/weir.sock")?;
client.push(b"hello, weir!", Durability::Batched)?;
```

For throughput, fan out across connections (one `WeirClient` per producer
thread) — a single connection is bounded by the round-trip time. See the
`weir-client` crate docs for the ordering caveat.

> **Already inside an async runtime?** `push()` is a *blocking* call — calling it
> directly from an `async fn` blocks the executor thread and starves the runtime.
> See [Producing from an async runtime](#producing-from-an-async-runtime) below.

## Producing from an async runtime

`WeirClient` is **synchronous and blocking**: every `push()` writes a frame and
then *blocks the calling thread* until the daemon's ack comes back. On an async
runtime that calling thread is an executor thread — so a naive
`client.push(..)?` inside an `async fn` blocks the executor and starves every
other task and timer scheduled on it. It compiles, passes a one-record smoke
test, and then falls apart under load: a measured 300-push burst run this way
took ~2.4 s and fired **zero** of the ~47 heartbeat ticks expected in that
window — the timers never got to run because the push monopolised the thread.

**Why `spawn_blocking` is not the fix here.** The obvious reflex —
`tokio::task::spawn_blocking(move || client.push(..))` — does not fit this
client. `push()` takes `&mut self` and the client owns a single live connection
(one record per round-trip, no pipelining), so it can't be shared across the
blocking-pool tasks tokio hands work to. You'd either reconnect on every push
(throwing away the persistent connection) or wrap the one client in a `Mutex`
and serialise every push through it — which collapses to a single connection and
kills the per-connection fan-out the client docs recommend for throughput.

**Recommended bridge: a pool of dedicated producer threads.** Stand up a fixed
pool of plain `std::thread`s, each owning **one** persistent `WeirClient` and
draining a bounded channel of jobs. The async side enqueues a job and `.await`s
a reply channel for the durable ack:

- **bounded channel → real back-pressure**: when producers can't keep up, the
  channel fills and the async caller's `send` awaits, instead of unboundedly
  buffering;
- **one client per thread → fan-out**: N threads = N independent connections,
  exactly the parallelism the [Produce](#produce-from-your-own-program) note
  recommends, with each connection driven sequentially (ordering holds within a
  connection, not across them);
- **per-job reply channel → the ack contract survives end-to-end**: the async
  caller learns the push was durably acked (or why it failed), so the
  at-least-once guarantee reaches your async code.

**Reconnect after poison.** A producer thread must handle a poisoned client. Once
a connection-fatal failure occurs, the client poisons itself and **every**
subsequent call fails fast until it is dropped and rebuilt. Branch on the client
API rather than matching the `#[non_exhaustive]` error enum:

- `WeirClient::is_poisoned(&self) -> bool` — `true` once the client must be
  rebuilt;
- `ClientError::is_recoverable(&self) -> bool` — `true` means the **connection is
  still usable** (retry/continue on the *same* client); `false` means **drop the
  client and reconnect**.

Recoverable (connection untouched, keep using it): the *local* pre-send
`ClientError::PayloadTooLarge` rejection, `ClientError::NoDefaultDurability`, and
`ClientError::Nack(NackReason::InternalError)` (the one transient Nack the daemon
keeps the connection open for). Everything else is non-recoverable and the daemon
has closed (or the socket is dead): all *other* server Nacks, plus
`ClientError::Io` and `ClientError::Protocol`. Note the name collision — the
recoverable *local* `ClientError::PayloadTooLarge` (payload over the protocol
hard cap, rejected before any bytes are sent) is a **distinct variant** from the
non-recoverable *server* `ClientError::Nack(NackReason::PayloadTooLarge)` (payload
over the daemon's configured `max_payload_bytes`, which the daemon Nacks then
closes). After a push error, if `client.is_poisoned()` (equivalently
`!err.is_recoverable()`), drop and rebuild the client before the next job.

A sketch of one producer thread (the async-side glue — channel wiring and the
spawn of N of these — is left out for brevity):

```rust
use tokio::sync::oneshot;
use weir_client::{ClientError, Durability, WeirClient};

// One job: bytes to push, the tier, and where to send the ack back.
struct Job {
    payload: Vec<u8>,
    durability: Durability,
    reply: oneshot::Sender<Result<(), ClientError>>,
}

// Runs on a dedicated std::thread; owns ONE persistent connection.
fn producer_thread(sock: &str, jobs: std::sync::mpsc::Receiver<Job>) {
    let mut client = WeirClient::connect(sock).expect("initial connect");
    for job in jobs {                       // blocks until the next job; ends on channel close
        let result = client.push(&job.payload, job.durability);
        if let Err(e) = &result {
            // Drop the dead client and reconnect; a poisoned client fails every
            // later call. Recoverable errors (local rejects, InternalError Nack)
            // leave the connection usable, so we keep it.
            if !e.is_recoverable() || client.is_poisoned() {
                client = WeirClient::connect(sock).expect("reconnect");
            }
        }
        let _ = job.reply.send(result);     // async caller .await's this for the durable ack
    }
}
```

> A first-class async producer type is a candidate for a future release; today
> this bridge is the recommended way to produce from async code.

## Write a custom sink

Implement the [`Sink`](https://docs.rs/weir-sink-sdk) trait from `weir-sink-sdk`
(depends only on `weir-core` — no daemon, no tokio needed to build or test it):

```rust
use weir_sink_sdk::{BasicSinkError, CommitResult, Payload, Sink, SinkHealth};

struct MySink { /* a client handle, etc. */ }

impl Sink for MySink {
    type Record = Payload;          // opaque bytes (the usual choice)
    type Error = BasicSinkError;    // ready-made error; or your own SinkError

    async fn commit(&self, batch: Vec<Payload>)
        -> Result<CommitResult<Payload>, BasicSinkError>
    {
        let mut committed = Vec::new();
        let mut dead_lettered = Vec::new();   // Vec<(Payload, String)>
        for record in batch {
            match deliver(record.as_ref()) {  // your delivery logic
                Ok(()) => committed.push(record),
                // PERMANENT rejection (e.g. a 4xx): dead-letter the record paired
                // with a human-readable reason. dead_lettered is Vec<(Payload,
                // String)> — the record AND why it was rejected, for the operator.
                Err(reason) => dead_lettered.push((record, reason)),
            }
        }
        // A *transient* failure instead returns Err, so the drain retries the
        // WHOLE segment with backoff (don't dead-letter on transient errors):
        //   return Err(BasicSinkError::transient("503 from upstream"));
        Ok(CommitResult::new(committed, dead_lettered))
    }

    async fn health(&self) -> SinkHealth { SinkHealth::Healthy }
}
```

Every record handed to `commit` must end up in exactly one of `committed` or
`dead_lettered` (the drain enforces this before confirming the segment). To build
a `Payload` yourself — e.g. in a unit test — use `Payload::from(vec)`,
`Payload::from("text")`, `Payload::from(&b"bytes"[..])`, or
`Payload::copy_from_slice(bytes)`; read its bytes with `payload.as_ref()` /
`&payload[..]`.

`CommitResult` exposes both partitions as **public fields** for reading back what
a commit returned — `committed: Vec<R>` and `dead_lettered: Vec<(R, String)>`,
where each dead-letter entry pairs the record with its human-readable reason
(it's `#[non_exhaustive]`, so *construct* via `CommitResult::new`, but read the
fields directly):

```rust
let result = my_sink.commit(batch).await?;
println!("committed {} records", result.committed.len());
for (record, reason) in &result.dead_lettered {
    eprintln!("dead-lettered {} bytes: {reason}", record.as_ref().len());
}
```

You can **unit-test** the sink against the contract with no runtime — the test in
the `weir-sink-sdk` crate (`a_custom_sink_can_be_driven_and_unit_tested`) shows a
tiny `block_on` helper that polls a ready future to completion, then asserts on
`result.committed` / `result.dead_lettered`. See also
[Testing → Sink integration](../testing/sink-integration.md).

> **Running it inside the shipped daemon** today means building a `weir-server`
> with your sink wired into the sink-selection path (effectively a small fork) —
> there is no dynamic plugin/registration path yet. The SDK's present value is
> *authoring and testing* a sink against a stable, frozen contract; a first-class
> entry point for downstream sinks is a candidate for a future minor release.

## Read or recover WAB segments directly

`weir-wab` is a standalone, runtime-free reader of the on-disk segment format
(depends only on `weir-core` + `crc32fast`). Point `SegmentReader` at any segment
file and it streams each record, verifying its CRC32:

```rust
use weir_wab::SegmentReader;

// The ACTIVE segment holds records buffered but not yet drained:
for record in SegmentReader::open("/var/lib/weir/wab/shard_00/seg_00000001.wab")? {
    let payload = record?;          // io::Error on a CRC mismatch / truncation
    // … inspect, re-emit, or recover `payload` …
}
```

> **Do not read a segment back to confirm a push "landed" — that races the drain.**
> Segment files are *transient*: the moment a segment drains, the daemon writes a
> `seg_*.wab.confirmed` sidecar and **deletes the segment file**. This is true for
> the **active** `seg_*.wab` too, not just sealed segments — so a
> "push, then open the segment with `SegmentReader` to verify" workflow can find
> **zero records** while everything is working perfectly: the records were
> accepted, drained, and the file deleted out from under you. The source of truth
> for "did it land" is **metrics** (`weir_records_ack_total` for acceptance), not
> the on-disk file. Use `SegmentReader` to *inspect or recover* buffered data, not
> to prove a write succeeded.

**Which file holds your data depends on its lifecycle:**
- `seg_*.wab` (active) — currently being written; un-drained records live here.
  Disappears the instant the segment drains (see the warning above).
- `seg_*.wab.sealed` (sealed) — full/idle, awaiting drain. **Short-lived:** once
  the sink commits it, the daemon writes a `seg_*.wab.confirmed` sidecar and
  **deletes the sealed file**. So pointing at a `.sealed` path in a live, healthy
  deployment often finds nothing — and a reader that opens one and gets **zero
  records is not necessarily an error**, the segment may simply have drained. For
  live un-delivered data, read the active `.wab`.
- `dead_letter/dl_*.wab.sealed` — records the sink permanently rejected; this is
  the durable, non-transient place to find data (and what `weir-ctl dl requeue`
  reads).

This is the parser the daemon and `weir-ctl` both use (one implementation, no
drift), so an offline inspector or a bespoke recovery tool reads exactly what the
daemon wrote. The [WAB on-disk format](../wab_format.md) documents the byte
layout. (`weir-ctl dl requeue` is built on exactly this — it reads dead-letter
segments with `weir-wab` and re-submits them through the daemon's socket.)

## Operate a running daemon

`weir-ctl` is a thin admin tool over the socket + the WAB reader; it does **not**
depend on `weir-server`, so installing it doesn't drag in the daemon's
dependencies. It covers `health`, `push`, `metrics`, `segments` (per-shard WAB
inspect), and `dl` (dead-letter `list` / `drop` / `requeue`). See
[Monitoring](../monitoring.md) and the `dl requeue` notes there.

---

The takeaway: depend on `weir-server` only when you actually run the daemon.
Everything else — producing, forwarding, reading the buffer, operating — is
available from the smaller crates on their own.
