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

## Produce from your own program

The synchronous client is one blocking round-trip per push, no async runtime
required. See the [Quickstart](quickstart.md#push-your-first-record) for the full
example and the `Cargo.toml` dependency block; in short:

```rust
use weir_client::WeirClient;
use weir_core::Durability;

let mut client = WeirClient::connect("/run/weir/weir.sock")?;
client.push(b"hello, weir!", Durability::Batched)?;
```

For throughput, fan out across connections (one `WeirClient` per producer
thread) — a single connection is bounded by the round-trip time. See the
`weir-client` crate docs for the ordering caveat.

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

You can **unit-test** the sink against the contract with no runtime — see the
test in the `weir-sink-sdk` crate for a `block_on` pattern, and
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

**Which file holds your data depends on its lifecycle** — this trips people up:
- `seg_*.wab` (active) — currently being written; un-drained records live here.
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
