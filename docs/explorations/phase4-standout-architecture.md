# Phase 4 — Standout Architecture Exploration

**Status:** Research / proposal only — nothing here is committed to.
**Date:** 2026-06-11
**Branch context:** `v1/phase-3-performance` (0.8.0), heading toward 1.0 crates.io release.

---

## 1. Honest Competitive Landscape + weir's Wedge

### The field

**Kafka / Redpanda**
Full distributed log brokers. Multiple partitions, consumer groups, offset management,
schema registry, and a large operational surface. Kafka alone requires ZooKeeper (or
KRaft) and a JVM heap. Redpanda is leaner but still a multi-process cluster. Both solve
the "global event bus" problem. They do not solve the "I have one service that needs to
reliably hand off writes to a slow or flaky sink without losing data on crash" problem —
they introduce an entirely different operational tier into that stack.

**NATS JetStream**
A lightweight pub/sub layer with durable streams bolted on top. Better operational
story than Kafka. Still a separate broker that producers and consumers must reach over
the network. JetStream stores subjects, not opaque byte buffers; it has its own
serialization assumptions. Fan-out and consumer groups are first-class. Single-producer,
single-sink "absorb bursts and drain to DB" is not the primary design target.

**Vector / Fluent Bit**
These are the closest structural relatives. Vector is an agent that ingests, transforms,
and routes log/event data to sinks. It supports batching, retries, back-pressure, and
pluggable sources and sinks. The critical differences: (1) Vector is a data pipeline
tool — it owns serialization, routing, and transformation as first-class concerns;
(2) it lacks WAL-based segment durability with crash-recovery guarantees (it has disk
buffers, not a checksummed, sequenced, seekable WAB); (3) producers do not get a
per-record durable Ack before the record is safe — it is a pipeline, not a buffer with
explicit producer-facing acknowledgement semantics; (4) it is not a library, not
embeddable, and not instrumentable from Rust code at zero cost.

**Chronicle Queue / Aeron**
Low-latency, memory-mapped, single-producer designs built for financial HFT. Chronicle
Queue is a persisted ring-buffer (off-heap, memory-mapped). Aeron is a transport layer
with reliable UDP. Both are Java-first. Neither has the "absorb bursts, drain to a
pluggable slow sink, guarantee at-least-once" story as the central abstraction.

**Plain WAL library (e.g. `glommio`, raw `io_uring` wrappers, or a hand-rolled segment
file)**
The lowest-level alternative: just implement the WAB yourself. weir is better than this
for most users because it gives you crash recovery, drain-to-sink orchestration,
dead-letter handling, durability tiers, Prometheus metrics, Unix socket and mTLS TCP
transports, and a tested client library — out of the box.

**Managed buffers (Kinesis, Cloud Pub/Sub)**
Network round-trip per record, vendor lock-in, cost per message, not suitable for
on-prem or edge, and latency is 10s of ms not ~150 µs. Different trade-off space
entirely.

### weir's genuine wedge

weir occupies a gap that none of the above fill well: **a local, process-adjacent,
durable write buffer that absorbs producer bursts at local-socket speed and drains them
to any slow or flaky sink with at-least-once delivery, without running a broker, without
a schema, and without ops overhead.**

The specific wedge is:

1. **Latency tier.** One local socket round-trip + one local fsync (150 µs on NVMe, 1.4
   ms on SATA SSD). No network hop before the Ack. Producers are as fast as the storage
   device.
2. **IOPS compression, not fan-out.** N producer writes → 1 downstream commit (249:1
   measured). weir is a coalescing buffer, not a router. This is the opposite of Kafka's
   fan-out model.
3. **Crash-safe WAB with explicit Ack.** Every record that gets an Ack is on stable
   storage, period. The drain confirms delivery at the segment level; unconfirmed
   segments replay on restart. No message broker needed to achieve this.
4. **Zero-ops local daemon.** Binary, TOML config, Unix socket or mTLS TCP, Prometheus
   metrics. No cluster, no ZooKeeper, no JVM. Runs in a Docker sidecar or a systemd
   unit.
5. **Pluggable sink, Rust-native.** A 3-method trait (`commit`, `max_batch_size`,
   `health`) plus an error contract that is enough to connect weir to any downstream
   store.

The wedge is **not** scalability beyond one node, not fan-out, not pub/sub, not schema
management. Those are non-goals and the wedge is sharper for it.

---

## 2. Ideal User and Use Case

**Primary:** A Rust (or polyglot, via TCP+mTLS) backend service that writes to a slow
or flaky downstream — a SQL database, ClickHouse, an HTTP ingest endpoint — and cannot
afford to block producers on downstream latency or lose records on process crash. The
service runs on a single machine (sidecar, edge node, embedded appliance) and does not
need a broker. Examples:

- Rails or FastAPI app that inserts event records into MySQL or Postgres. Each insert
  blocks the request thread; with weir the write is a local socket push (~150 µs) and
  the bulk INSERT happens asynchronously.
- ClickHouse ingest from a high-throughput Rust service. ClickHouse inserts are slow
  and idempotency requires dedup tokens; weir handles both with the
  `insert_deduplication_token` pattern already implemented.
- An edge IoT collector that must buffer records locally when the upstream HTTP endpoint
  is unreachable, and drain them when it comes back.
- Any service that currently uses an in-memory channel to decouple a hot write path
  from a slow sink, and needs crash safety without adding a broker.

**Secondary:** Teams evaluating whether they need Kafka. weir is a "prove the pattern
locally before committing to a broker" tool. If 249:1 IOPS compression is all you need,
you never need Kafka.

**Anti-target:** Multi-node fan-out, pub/sub, consumer groups, schema evolution,
real-time stream processing. These users should reach for Kafka, Redpanda, or Flink.

---

## 3. Distinctive Architecture Bets

The following are opinionated proposals, not a backlog. Each is assessed for fit with
weir's fsync-bound, single-node, single-sink design and rated by implementation effort
(S/M/L) and risk (low/medium/high).

---

### Bet A — Generalised Dedup Token as a Protocol Primitive

**What it is.** weir's ClickHouse sink already derives a `sha256(batch)` dedup token
and passes it as `insert_deduplication_token`. The same pattern is useful for any
idempotent downstream: Postgres `ON CONFLICT`, HTTP `Idempotency-Key` headers
(already done per-record in the HTTP sink), custom application-level dedup tables. The
proposal: promote a *segment-scoped* dedup token to the WAB format itself (seal-time
sha256 of the segment's record bytes, stored in the confirmed sidecar and surfaced to
the Sink trait), so any sink implementation gets it without re-computing it.

**Fit.** weir is already fsync-bound; adding a hash over records during the WAB seal
operation adds ~microseconds (SHA-256 is ~2 GB/s on modern CPUs; a 256 MiB segment
takes ~130 ms at seal time, which is a one-time background cost, not on the hot path).
The ClickHouse sink's existing `dedup_token` function is the proof of concept. The only
thing missing is plumbing the token through the `CommitResult` contract and the
confirmed sidecar.

**What it unlocks.** A sink implementor can write truly idempotent downstream logic
without needing to design their own token scheme. This is the move from "at-least-once"
to "practically exactly-once" — weir's semantic story becomes "we guarantee you the
dedup token; your sink decides what to do with it." That is a meaningful differentiation
from Vector (which does not give sinks a stable dedup handle) and from raw WAL libraries
(which have no sink abstraction).

**How it would work.** The segment footer already stores `record_count` and `data_bytes`.
Add a `content_sha256: [u8; 32]` field (accumulated incrementally during writes, same as
the running `file_crc32`, so no re-read at seal time). Surface it in a new `SinkBatch`
struct that wraps `Vec<Record>` and carries the token. The `Sink::commit` signature
gains this context: `async fn commit(&self, batch: SinkBatch<Self::Record>) -> ...`.
Existing sinks ignore the token; new sinks use it.

**Effort:** M — WAB format version bump (SEGMENT_FORMAT_VERSION 1 → 2), confirmed
sidecar extended, `Sink` trait API change (breaking, but pre-1.0 so acceptable).

**Risk:** Medium — format version bump requires a migration story for existing WAB dirs.
Must document clearly. Recovery code must handle both versions.

---

### Bet B — Embeddable Library Mode (weir-embedded)

**What it is.** The README's current non-goal: "Not embedded — weir is a daemon;
producers talk to it over a socket." Flip this for library users: expose a
`WeirBuffer<S: Sink>` type in a new `weir-embedded` crate that inlines the WAB
flusher, worker pool, and drain directly into the caller's process, with no socket layer.
The daemon mode becomes one possible deployment of the same core engine.

**Fit.** This is the highest-leverage move for the Rust ecosystem specifically.
A Rust service that wants crash-safe buffering to a DB sink today has two options: run
the daemon (ops overhead, socket round-trip) or implement it themselves. A library crate
solves both problems. It also makes weir testable in integration tests without spawning a
child process — the existing load test workaround (`env!("CARGO_BIN_EXE_weir-server")`)
goes away.

**Why it fits the fsync-bound finding.** The fsync floor cannot be improved by removing
the socket layer — it is storage physics. But the socket round-trip adds ~5–20 µs (kernel
buffer copy + context switch). For library users, eliminating that hop is a real win on
the latency floor. The existing `weir-server` tests already spawn real binaries; moving
the core to a library makes that a first-class use case.

**What it would look like.**

```rust
// In the caller's process — no daemon, no socket
let buffer = WeirBuffer::builder()
    .wab_dir("/var/lib/myapp/wab")
    .sink(MyClickHouseSink::new(config))
    .durability(Durability::Batched)
    .build()?;

// Push a record — blocks until durable (same guarantee as daemon)
buffer.push(record_bytes).await?;
```

Internally `WeirBuffer` owns the WAB flusher threads, the drain thread, and the Sink.
It shuts down cleanly on `Drop` (drains pending segments). The daemon becomes
`weir-server` which instantiates a `WeirBuffer` and wraps it in the socket layer.

**Effort:** L — requires factoring `weir-server`'s core into a library crate
(`weir-engine`?), keeping the daemon thin, writing a public API with stability
commitments. This is the biggest refactor but probably the most impactful bet for
crates.io.

**Risk:** Medium — API design is hard to get right before 1.0; getting it wrong and
semver-breaking it is painful. Recommend shipping a `0.1.0` of `weir-embedded` on
crates.io simultaneously with `weir-server 1.0`, clearly marked as unstable until `1.0`.

---

### Bet C — Multi-Sink Fan-Out (Selective, Not Full Pub/Sub)

**What it is.** weir's current model: one producer stream, one sink. A selective
extension: allow a single WAB to drain to multiple sinks simultaneously, where each
sink gets every record (broadcast) or a subset (routed by a user-provided predicate on
the payload bytes). This is explicitly *not* pub/sub (no consumer groups, no offset
management) — it is "drain to two sinks with one WAB."

**Fit.** The drain is already a single thread reading sealed segments and forwarding
them to one `Arc<S>`. Extending to `Vec<Arc<dyn ErasedSink>>` (or a typed tuple via
const generics) is structurally sound. The confirmed sidecar logic becomes "segment is
confirmed only when all sinks confirm it." The dead-letter story remains per-sink.
The fsync-bound write path is unchanged — fan-out cost is only on the drain side.

**Who asks for this.** The "write to both MySQL and ClickHouse" pattern is common for
teams running SQL + analytics side by side. Without fan-out they run two weir daemons
(or write twice from the producer), doubling WAB storage and socket round-trips.

**Risks and hard design decisions.**
- What happens if sink A confirms but sink B is in `RetryingTransient`? The segment
  cannot be deleted until both confirm. The drain must track per-sink confirmation
  state per segment — more complex state machine.
- A permanently-failed sink should not block the other sink indefinitely. This requires
  a per-sink dead-letter path and a per-sink `BlockedDeadLetterFull` state. The current
  single-threaded drain serialises on segment order; with 2 sinks either the drain
  becomes concurrent (two drain threads sharing a read lock on the sealed segment file)
  or sequential (sink A then sink B per segment, which serialises latency).
- The `Sink` trait API changes: multiple sinks imply either a type-erased `dyn Sink`
  (AFIT makes this awkward — boxing futures) or a tuple-based const-generic approach.

**Verdict.** Valuable, but the state machine complexity is real and the API design
problem is not trivial. Recommend as a post-1.0 / 1.1 feature with a clean design
phase, not bundled into 1.0.

**Effort:** L. **Risk:** High (state machine, API design, potential for correctness
regressions in the confirmed/dead-letter logic).

---

### Bet D — Observability as a Product Feature (not just Prometheus metrics)

**What it is.** weir already has 19 Prometheus metrics. The proposal: go further and
make observability a first-class *product* differentiator — the thing that makes weir
the option you reach for when you need to understand what happened.

Concretely:
1. **Structured event log** (`WEIR_LOG=jsonl` mode): every segment seal, confirm,
   retry, dead-letter, and health transition emitted as a structured JSON log line with
   timestamps, segment path, record count, byte count, outcome, and sink identity. Not
   just `tracing` spans — a parseable audit trail that ops teams can pipe to Loki, ELK,
   or a simple `jq` query.
2. **Dead-letter UI** (optional CLI subcommand): `weir dl list` / `weir dl replay` /
   `weir dl drop` — inspect and manage the dead-letter directory without writing a
   custom `SegmentReader`. Dead-lettered records are already valid WAB segments; the
   tooling is a thin wrapper over the existing `SegmentReader`.
3. **Replay-on-demand** (non-crash path): `weir replay <segment-path> [--to-sink]` —
   feed a historical sealed segment back through the drain. Useful for schema migrations
   (replay dead-lettered records after fixing the schema) or load testing sinks.

**Fit.** weir is already fsync-bound and the pipeline is lean. The value at 1.0 is
not more throughput — it is operational confidence. The dead-letter writer already
stores dead-lettered records as valid WAB segments. A `weir dl` CLI subcommand is ~100
lines of Rust using the existing `SegmentReader`. The structured log emitter is a
`tracing` subscriber config, not a new subsystem.

**Why this makes weir stand out.** Vector has rich observability but it is a pipeline
tool. Kafka has consumer-lag metrics but not segment-level audit trails. A Rust binary
that gives you structured JSON logs, a dead-letter management CLI, and on-demand replay
is meaningfully more operable than a raw WAL library. This is the kind of feature that
gets cited in "why we chose weir" blog posts.

**Effort:** S–M. The dead-letter CLI and structured log mode are small. Replay-on-demand
is M (needs plumbing to inject a segment path into a drain that normally reads from the
WAB directory).

**Risk:** Low. All three sub-features are additive and do not touch the hot path.

---

### Bet E — Pluggable Sink SDK (weir-sink-sdk crate)

**What it is.** Today, writing a custom sink means depending on the internal
`weir-server` crate. The `Sink` trait, `SinkError`, `CommitResult`, `SinkHealth`, and
`SinkRecord` traits live in `src/sink/mod.rs`. For a crates.io ecosystem to form
("weir-sink-s3", "weir-sink-kafka", "weir-sink-redis"), these types need to be in a
stable, minimal, published crate — `weir-sink-sdk` or `weir-types` — that third-party
crate authors can depend on without pulling in the entire daemon.

**Fit.** The `Sink` trait has no internal dependencies beyond `weir_core::Payload`. It
is already AFIT-based (stable since Rust 1.75). The `SinkError::retry_after()` hint
mechanism is clean. Extracting these types to a dedicated crate is a one-day refactor
with broad ecosystem value.

**What an ecosystem looks like.**
```toml
# A community-maintained S3 sink
[dependencies]
weir-sink-sdk = "1"
aws-sdk-s3 = "..."
```

This is how `tower::Service`, `axum`, and `diesel` built their ecosystems — a stable
trait crate that the community implements.

**Effort:** S (extract existing types, publish crate, update import paths). **Risk:**
Low, but the API must be stable at 1.0 — get the trait shape right before publishing.
The `SinkBatch` type from Bet A (if pursued) must be in this crate.

---

### Bet F — Backpressure Signal to Producers (Flow Control Beyond Queue Saturation)

**What it is.** Currently, weir's only backpressure signal to producers is queue
saturation: when the 65,536-slot MPMC queue fills, `push_timeout` blocks for 5 seconds
then returns `InternalError`. This is coarse. A richer flow-control protocol:

1. **Drain-rate advisory in HealthCheckResponse.** The `HealthCheckResponse` frame today
   carries zero payload. Add an optional backpressure byte or a small TLV extension:
   current drain rate (records/s), estimated queue depth, and sink health grade. A
   producer that polls `HealthCheck` before writing can self-throttle based on actual
   drain capacity rather than waiting for a queue-full Nack.
2. **`Nack(Backpressure)` with retry-after hint.** A new Nack reason (alongside
   `InternalError`) that carries a recommended retry delay derived from the current fsync
   latency and queue depth. Producers can back off intelligently instead of spinning on
   5-second timeouts.

**Fit.** This is a protocol extension (wire version 2 would be needed for the
HealthCheckResponse change, unless the TLV is backward-compatible within the existing
payload field). The adaptive coalesce work from Phase 3 already computes an EWMA of
fsync latency — that value is exactly the right input for the backpressure signal. The
drain's `weir_queue_depth` gauge and `weir_sink_commit_duration_seconds` histogram are
already on-hand; the backpressure signal is an in-process read of those counters.

**Who benefits.** Producers that can gracefully degrade: they check health before
committing to a write and shed load upstream rather than hammering weir's queue. Useful
for high-throughput producers that would otherwise saturate the queue and incur 5-second
stalls.

**Effort:** M (protocol extension, wire format change, client-side adaptation).
**Risk:** Medium (wire version bump is a breaking change; must handle v1 clients gracefully).

---

## 4. Ranked Recommendation

**What would most make weir stand out at 1.0:**

**Tier 1 — Do these for 1.0 (highest impact, lowest risk):**

1. **Bet E — Pluggable Sink SDK crate.** This is the lowest-effort, highest-leverage
   move for the crates.io release. Without a stable `weir-sink-sdk` crate, the ecosystem
   cannot form. Ship this at 1.0 unconditionally. It is the difference between "a daemon
   with five built-in sinks" and "a platform that the community can extend."

2. **Bet D — Observability as a product feature.** The dead-letter CLI and structured
   JSON log mode are small in effort and directly answer the question "what actually
   happened to my records?" This is the kind of thing that shows up in the README and
   makes weir credible for production use. The replay-on-demand feature is the killer
   app for schema migration users.

3. **Bet A — Generalised dedup token as a protocol primitive.** The ClickHouse dedup
   story is already weir's strongest correctness claim. Generalising it from a
   sink-specific implementation to a first-class WAB-level primitive makes the "at-least-
   once with idempotency handle" story universal. This is the semantic move that
   differentiates weir from "yet another WAL" — you get a stable dedup identity for free
   on every batch, regardless of sink.

**Tier 2 — Strong candidates for 1.1 / 1.2:**

4. **Bet B — Embeddable library mode.** The highest potential impact for the Rust
   ecosystem, but the API design risk is real. Ship as `weir-embedded 0.1.0` (unstable
   API, semver-exempt) alongside `weir-server 1.0`. Promote to 1.0 after a cycle of
   real-world use.

5. **Bet F — Backpressure signal to producers.** Genuinely useful for high-throughput
   producers but requires a wire protocol change and careful backward compatibility
   handling. Worth designing at 1.0 and shipping at 1.1 so the protocol extension is
   clean.

**Tier 3 — Post-1.0 only:**

6. **Bet C — Multi-sink fan-out.** The user value is clear but the implementation
   complexity is highest and the correctness surface is largest. Do not ship before the
   confirmed-sidecar and dead-letter state machine has been stable in production for a
   release cycle.

**The one move that changes the narrative.** If forced to pick a single bet that most
shifts how weir is perceived: **Bet B (embeddable library)**. "A durable write buffer
you add as a crate, not a daemon you operate" is a much stickier story for the Rust
ecosystem than a sidecar daemon with nice metrics. The daemon remains the right
deployment for polyglot producers, but the library is what makes Rust users choose weir
over hand-rolling a WAB.

---

## 5. Open Questions

**Protocol / format:**
- Bet A requires a WAB format version bump. What is the migration story for operators
  with existing WAB directories on SEGMENT_FORMAT_VERSION=1? Options: (a) read-only
  support for v1 segments in recovery, refuse to write new v1 segments; (b) in-place
  migration on startup (dangerous); (c) document that the WAB is ephemeral by design
  (confirmed segments are deleted; the only v1 segments that survive are sealed-but-
  unconfirmed at upgrade time, and those will be drained and then replaced with v2).
  Option (c) is probably correct but needs to be stated explicitly.

- Bet F: can the backpressure advisory be backward-compatible as a TLV extension in the
  HealthCheckResponse payload (v1 wire clients ignore the payload; they only look at the
  message_type byte)? If yes, no wire version bump needed. Check the client library's
  HealthCheckResponse parsing path.

**Embeddability:**
- For Bet B: what is the right crate name and where does it live? `weir-embedded` is
  accurate but awkward. `weir` (the library) with a feature gate for the socket layer?
  This has precedent in `axum` (the router is the library; `axum::serve` is the HTTP
  layer on top). weir could be `weir` (library, `WeirBuffer<S>`) + `weir-server`
  (daemon, socket layer on top).

- For Bet B: the current daemon uses `#[cfg(unix)]` gating for the socket layer. The
  library core (WAB, drain, worker) is cross-platform (Windows paths work; the WAB
  format is platform-neutral). Does the library ship on Windows? The `fdatasync` path
  is Unix-only but `sync_all()` is cross-platform. A Windows build with relaxed-
  durability-only semantics is possible but the value is unclear.

**Sink SDK:**
- For Bet E: the `Sink` trait uses AFIT (`async fn` in trait, stable since Rust 1.75).
  Third-party crate authors targeting older MSRV will be blocked. What is weir's MSRV
  commitment at 1.0? Currently implied as "latest stable." Fixing MSRV at 1.75 or
  later is a reasonable call; it should be documented in `Cargo.toml` and the README.

- For Bet E: `SinkRecord::from_payload(payload: Payload) -> Self` takes a `Payload`
  (`bytes::Bytes`, post-phase-3). Should the SDK crate re-export `bytes::Bytes` to
  avoid version conflicts, or should `Payload` be a newtype? The newtype approach is
  cleaner for the SDK's API stability.

**Fan-out:**
- For Bet C: is the right abstraction a `MultiSink<A: Sink, B: Sink>` type that
  implements `Sink` by forwarding to both, or a runtime-dispatched `Vec<Box<dyn Sink>>`?
  The former is zero-cost but requires const-generic or macro ergonomics for >2 sinks.
  The latter avoids the type complexity but boxes futures (AFIT + dyn = boxing required).
  This design question should be answered before any implementation starts.

**Positioning:**
- weir's "IOPS compression" framing (249:1) is its strongest quantitative claim. Is
  there a standard benchmark format (e.g. an independent `weir-bench` binary or a
  published Docker-compose stack) that lets a potential user reproduce the number on
  their own hardware? Reproducibility turns a claim into a proof.

- What is the right comparison benchmark to publish? "weir vs. in-memory channel + crash
  loss" is the honest baseline (that is the thing weir replaces). "weir vs. Kafka" is
  a different product and the comparison is unfair. Publishing the honest comparison —
  crash-safe local buffer vs. in-memory channel — and being explicit about what weir
  does not do (fan-out, pub/sub) is more credible than trying to beat Kafka on its own
  terms.

---

*This document is a research artifact. Nothing here constitutes a commitment to
implement any feature. All design proposals should go through a spec + plan cycle
before any code is written.*
