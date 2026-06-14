# weir — net-new feature directions (scout, 2026-06-14)

> **What this is:** a menu, not a commitment. Output of a 4-agent scout of "what
> could weir add," each grounded in the actual codebase + the existing
> `parked-future-directions.md` / `phase4-*.md` notes. Scoped on branch
> `v1/phase-4-cleanup`, heading to a 1.0 crates.io release. Companion doc:
> [`v1-hardening-findings.md`](v1-hardening-findings.md) (refactor / bugs / security).

Effort key: **S** ≈ <1 day · **M** ≈ a few days · **L** ≈ a week+.

---

## 0. The headline: the 1.0 freeze forces ~4 irreversible decisions *now*

Three independent scouts converged on this. These are the only genuinely
**1.0-gating** items, because they're near-impossible to change after a 1.0
semver without a breaking release:

1. **WAB on-disk format headroom.** Bump to format v2 *once*, reserving a
   per-record flags byte (compression / encryption) **+** a `content_sha256`
   footer field. Compression, encryption-at-rest, and a first-class dedup token
   all need this headroom; retrofitting after the freeze means a migration story
   each time. The format is *already* versioned with quarantine-on-unknown-version,
   so the safety net exists — reserve the slots before the freeze. *(Durability #1/#2.)*
2. **The `Sink::commit()` signature.** Decide before `weir-sink-sdk` hits 1.0
   whether it stays `commit(Vec<Payload>)` or becomes
   `commit(SinkBatch { records, dedup_token })`. Changing it later breaks every
   third-party sink. *(Sinks #3.)*
3. **Wire protocol `v1` + language-neutral conformance vectors.** Export the
   reference frames (currently Rust `const` arrays in a test file) as a
   `wire-vectors.json` (hex + field decode + expected outcome) and stamp the spec
   `wire/v1`. This is the keystone that lets any non-Rust client self-verify
   byte-for-byte. *(Positioning #1.)*
4. **A reserved Nack-reason byte** in the wire protocol for future backpressure
   signaling — cheap to reserve now, needs a version bump later. *(Durability #4.)*

---

## 1. Quick wins (all S-effort, several "do regardless")

| Item | Why | Source |
|---|---|---|
| **Batched/bulk HTTP delivery** | The flagship HTTP sink does **one POST per record** through a single-threaded drain — violates weir's own "N→1 IOPS compression" promise (a 1000-record segment at 5ms RTT = 5s of drain time). One-file fix; unlocks Elasticsearch/OpenSearch/Loki/Splunk-HEC `_bulk`. | Sinks #1 |
| **README + wire-doc truthing** | Both still say "3 crates" + `Payload = Vec<u8>`; it's 6 crates + `bytes::Bytes`. Embarrassing on a 1.0 launch. | Positioning #4 |
| **`/status` page + `weir-ctl status --watch`** | A presentation layer over logic `weir-ctl metrics` *already* computes — health story for `cargo install` users who won't run Grafana. | Reach #2 |
| **Wire conformance vectors** | (see freeze cluster #3) emit `wire-vectors.json` from the same encoder the test already asserts, so it can't drift. | Positioning #1 |

---

## 2. Direction A — Sinks & integrations

Grounding constraints: the **drain is globally single-threaded and single-sink**
(one `drain_rx`, one `Arc<S>`); commit is **segment-batch, at-least-once,
idempotency-is-the-sink's-job**; the SQL/ClickHouse sinks already do true bulk
delivery, the HTTP sink does not; `weir-sink-sdk` is already extracted/published.

1. **Batched/bulk HTTP delivery** — *S, 1.0-gating.* See quick wins. One
   `Idempotency-Key: sha256:<batch>` instead of per-record keys. Tradeoff:
   whole-batch-or-retry semantics (an ES-`_bulk`-aware partial-success parser is a
   later refinement). **Pursue — #1 pick for this direction.**
2. **Object storage / S3 sink (`weir-sink-s3`)** — *M, 1.0-nice-to-have.* Best
   architectural fit of any new sink: weir already seals immutable,
   content-addressable segments = the shape of an object-store write. Content-hash
   object keys make at-least-once *naturally idempotent* (strongest dedup story of
   any sink). Separate crate (`object_store` or `aws-sdk-s3`); doubles as the proof
   `weir-sink-sdk` works for third parties. **Pursue (flagship new sink).**
3. **SDK viability hardening** — *S, 1.0-gating (trait shape).* A published example
   sink + a `BoxSink` dyn-erasure shim (the drain is generic-only; AFIT can't be
   `dyn` without boxing — every fan-out/runtime-selection use case hits this) +
   MSRV pin + the `Payload`-re-export decision. Forces the irreversible `commit()`
   signature call (see freeze cluster #2). **Pursue.**
4. **File sink with rotation** — *S, 1.0-nice-to-have.* Zero-dep, std-only, lives
   in core; the "I just want durable data on disk for logrotate/Filebeat/Vector"
   on-ramp + the ideal soak/debug sink above `noop`. Caveat: append + at-least-once
   = duplicate lines on crash-replay (prefix each with its sha256; document
   bluntly). **Pursue if time / Maybe.**
5. **Sink registry** (open the closed `SinkType` enum) — *M, post-1.0.* Adding a
   built-in sink today = edits in 4 files. A name→constructor table fixes that, but
   it's YAGNI until ~8+ sinks. The one piece worth pulling forward is `BoxSink`
   (folded into #3). **Maybe / defer.**

**Skipped:** Kafka/NATS/Redis-streams sinks (credible Kafka *sink* is a 1.1
bridge-pattern item, but inverts weir's "you don't need a broker" wedge); gRPC
sink + webhook-templating (templating is Vector's job — explicit non-goal);
multi-sink fan-out (highest-risk change; needs `BoxSink` first; stays post-1.0).

---

## 3. Direction B — Durability & performance

Grounding: the fsync floor is **real and proven** (`fdatasync` ≈ 89% of Sync
latency on NVMe, 99% on SATA; io_uring empirically tested and rejected; the
software pipeline is exhausted at ≤30µs). Any write-path perf work is DOA. The
three tiers already span the space — **`Buffered` already acks before fsync**, so
a "LazySync" tier would just add a third point on the false-ack-invariant surface
for a win that sits between two existing tiers.

1. **WAB v2 format headroom + generalized dedup token** — *L (shared with #2),
   1.0-gating for the headroom.* See freeze cluster #1. Promotes the ClickHouse
   dedup token from a sink-local hack to a first-class primitive every sink gets
   free (computed incrementally like the existing `file_crc_hasher`). Recovery
   becomes a version *router*; migration is trivial (the WAB is ephemeral by
   design). **Pursue — #1 pick for this direction.**
2. **Payload compression (zstd, transparent, per-segment)** — *M,
   1.0-nice-to-have.* The one perf-adjacent feature the fsync floor *doesn't*
   swallow: `fdatasync` cost scales with dirty bytes, so fewer bytes = faster fsync
   on every tier + more records/segment = fewer seals. zstd at GB/s hides behind a
   1.4ms fsync. Rides on the #1 format bump. Needs a decompress-bomb guard.
   **Pursue (rides #1).**
3. **`weir-ctl dl replay`** (+ generic `replay <segment>`) — *S–M,
   1.0-nice-to-have, sequenced after the dedup token.* The deferred command; its
   blocker ("needs a shared WAB-format reader") no longer exists — `SegmentReader`
   *is* that reader, already public. Re-push via the client so records re-enter the
   normal pipeline; the generalized dedup token makes replay safe-by-construction.
   **Pursue, after the dedup token.**
4. **Backpressure as a typed Nack reason + health advisory** — *M, design at 1.0
   / ship 1.1.* Replace "queue full → block 5s → InternalError" with
   `Nack(Backpressure { retry_after })`. The EWMA fsync latency + `queue_depth`
   inputs already exist. Only the **reserved Nack-reason byte** is 1.0-timing (see
   freeze cluster #4). **Maybe / reserve the slot now.**

**Skipped:** a new LazySync/group-commit tier (already exists as `Buffered`);
write-path/io_uring/syscall perf (exhausted, rejected); read-back of *unsealed*
buffered data as a query API (collides with the "not a database" non-goal —
sealed-segment replay in #3 is the legitimate slice); standalone encryption-at-rest
logic (fold only its format slot into #1 now; build the crypto post-1.0).

---

## 4. Direction C — Reach (embeddability / deployment / egress)

Grounding: the core engine is **already decomposed into clean spawn seams**
(`wab::spawn` → `worker::spawn_workers` → `drain::spawn<S>`; `main.rs` is just
wiring). The one coupling blocker for a public library API is `Metrics` being
`pub(crate)` and threaded through every spawn signature.

1. **`weir-engine` — extract the in-process core as a library** — *M (revised down
   from L; the seams exist), 1.0-nice-to-have as `0.1`.* `WeirBuffer<S: Sink>` with
   `push(bytes, durability).await -> Ack`; `weir-server` becomes the thin
   socket/TLS shell. Flips the "not embedded" non-goal into "`cargo add` it." The
   real work is the API boundary: replace the `pub(crate) Metrics` param with an
   optional public `Recorder` trait; define in-process ack semantics; `Drop`-drains
   cleanly. Ship as explicitly-unstable `0.1` beside `weir-server 1.0`.
   **Pursue — #1 pick for this direction.**
2. **`/status` page + `weir-ctl status --watch`** — *S, 1.0-nice-to-have.* See
   quick wins. Smallest footprint here; do it regardless. **Pursue.**
3. **`weir-sink-tap` — a fan-out tap *sink*** (not a firehose endpoint) — *M,
   post-1.0.* `Tap<S>` forwards to the real sink *and* mirrors records to live SSE
   subscribers. Gets 80% of the "data tap / firehose" value at a fraction of scope.
   **Must be lossy/non-blocking toward observers** (a slow client must never
   backpressure the drain). **Maybe — after #1/#2.**
4. **Helm chart (StatefulSet + PVC + pre-upgrade drain hook)** — *S–M, post-1.0,
   demand-driven.* The chart already designed in `phase4-k8s-*.md` (full operator
   rejected — network storage kills fsync). No code change for the chart; a
   `--check-segment-format` flag + a `/healthz` route would harden it. **Maybe /
   skip until a real k8s user.**
5. **SIGHUP hot config reload beyond TLS** — *M, post-1.0.* Most config is
   structurally un-reloadable (`shard_count` defines thread topology at spawn);
   only ~5 scalar tunables qualify, and each needs a thread-safe path into running
   threads. High expectation-mismatch risk. **Skip (for now).** Multi-tenant
   namespacing: **skip** (contradicts the one-stream-one-sink wedge).

---

## 5. Direction D — Positioning & ecosystem

**weir's one-sentence positioning (as the scout would write it):**
> *weir is a single-node, process-adjacent durable write buffer that gives your
> app a synchronous, fsync-honest "it's safe" ack at local-socket speed, then
> drains records at-least-once to any slow or flaky sink — no broker, no schema,
> no cluster.*

Competitive read: NATS JetStream is the nearest "lightweight + durable" rival but
is a *broker reached over the network*, not a process-adjacent producer-acked
buffer. Vector's disk buffer is *pipeline-shaped* (it owns serialization/routing).
Redpanda Connect (Benthos) deliberately has *no* disk buffer. SQLite-WAL-queue
crates are embedded-only with no drain/dead-letter/sink orchestration. **What weir
should NOT become:** a transform pipeline (Vector), a multi-node broker with
pub/sub (Kafka/NATS), or a queryable store. Defending the non-goals *is* the
strategy.

1. **Language-neutral wire conformance vectors + frozen `wire/v1` spec** — *S,
   1.0-gating.* See freeze cluster #3. The keystone that unblocks every non-Rust
   client. **#1 ecosystem pick.**
2. **An official Go client** — *M, 1.0-nice-to-have.* Pick Go: the
   sidecar/edge/observability ecosystem weir competes in. Consumes
   `wire-vectors.json` as its test oracle (proves the spec is implementable from
   the doc alone). Python/Node gated behind real demand — don't gold-plate three
   SDKs for 1.0. **Pursue Go.**
3. **Async Rust client** — *M, 1.0-nice-to-have.* The current client is blocking;
   weir's warmest leads are async services (axum/tonic) that must `spawn_blocking`
   around every push today — undercutting the latency pitch. Sinks are already
   async; the client being blocking-only is an inconsistency. **Pursue.**
4. **Onboarding polish: doc truthing + one-command crash-survival demo** — *S
   (truthing) / S–M (demo), truthing is 1.0-gating.* A `docker-compose`
   "weir + flaky sink + load + Grafana" that shows records surviving `kill -9` and
   draining on restart — the product in 30 seconds, and the reproducible
   honest-benchmark vehicle. **Pursue.**
5. **`weir-ctl watch` live TUI** — *S–M, post-1.0.* Polish, not adoption-gating.
   **Maybe.**
6. **Embeddable library mode** — *L, post-1.0 as `0.x`.* Same as Reach #1; the
   Positioning scout rates it *lower priority than clients* (clients move more
   producers sooner at far less API-lock-in risk). **Maybe, not 1.0.**

---

## 6. The one open tension to decide

**`weir-engine` (embeddable library) priority.** The **Reach** scout rates it the
#1 highest-ceiling bet ("changes what weir fundamentally is"). The **Positioning**
scout rates it post-1.0 `0.x` and argues the wire conformance vectors + Go client +
async client move more producers sooner at a fraction of the API-lock-in risk.
Both agree it should NOT lock its API at 1.0. **Decision needed:** is the next big
bet *embeddability* (Rust-deep) or *polyglot reach* (clients-wide)?

---

## 7. Suggested 1.0 cut (synthesis)

**Gate 1.0 on:** the 4 freeze decisions (§0) + the S-effort truthing/quick-wins
(§1) + batched HTTP (A1). **Strong 1.0 nice-to-haves:** wire conformance vectors,
`/status`, async Rust client. **Fast-follow 1.1:** S3 sink, Go client, `dl replay`,
compression, `weir-engine 0.1`. **Post-1.0/demand-driven:** tap-sink, Helm, sink
registry, fan-out, backpressure advisory.
