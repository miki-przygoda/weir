# Parked / Post-1.0 Future Directions

Deliberately deferred during the v1 push — **not dropped**. The full analysis for
each lives in the dated exploration docs in this directory. Revisit after 1.0.

## Kubernetes — full deployment + operator
Cut from Phase 4 (see `phase4-k8s-deployment.md`, `phase4-k8s-operator.md`).
Verdict: a full operator is not justified for a single-instance daemon with no
clustering, and k8s's default network-attached storage fights weir's fsync-bound
nature (NFS/EFS make `fdatasync` 5–50× slower — degrading exactly what Phase 3
optimized). A **Helm chart is a cheap 2–3 day add** when a user actually deploys
weir on k8s. Kept for when there's real demand.

## ~~Object storage / S3 sink~~ — SHIPPED

Landed as the published crate `weir-sink-s3`, behind `weir-server`'s opt-in
`s3-sink` feature. See [`docs/sinks/s3.md`](../sinks/s3.md).

Two things worth not rediscovering:

**The reusable half is the credential and signing layer, not the sink.**
`sigv4.rs` and `creds.rs` are provider-agnostic AWS plumbing — hand-rolled to
keep `aws-lc-sys` out of a workspace unified on `ring`, and pinned by 31 of
AWS's own published vectors. A Kinesis, SQS or CloudWatch Logs sink is now
mostly a request shape on top of them, which is what makes those cheap in a way
they were not before.

**The idempotency story turned out stronger than expected, but not for the
reason the original entry assumed.** The note below on the dedup token said a
content-addressed object key would make at-least-once "naturally idempotent".
That is exactly half right and the wrong half is dangerous: a `DedupToken` is a
pure content hash, so two batches of byte-identical records share one, and an S3
key is last-write-wins — a content-named object is *destroyed* by the next
identical batch, with no crash involved. The working key derives from the WAB
coordinate (`RecordId`) plus the segment's `created_at`, both replay-invariant
and both unique. The result is genuinely what the entry hoped for — no
downstream dedup support required at all — but a content hash could not have
delivered it.

## k8s-native sinks
User idea: sinks that target Kubernetes-native systems (or weir deployed as a
k8s-integrated buffer). Park until the sink ecosystem (`weir-sink-sdk`) exists and
there's a concrete k8s use case to design against.

## Hot-processing fast-path for the WAB
User idea, drawn from another of the user's projects: a "hot-processing" technique
to speed up the WAB. Phase 3 proved the WAB write path is **fsync-bound (89–99%)**,
so in-software write-path speedups are exhausted on the current architecture — BUT
a hot-processing fast-path could be a *semantic / architectural* change worth
exploring (akin to the `LazySync` tier in `phase4-wab-optimization.md`, which acks
before fsync). Kept; needs a design pass when revisited (likely post-1.0).

## `RecordId` is unique only within one `wab_dir`
Parked 2026-10-15 from the safety-floor review (findings conf-durability-1 and
conf-regressions-10). Needs a future major: it changes the id format, which is
public SDK surface.

**The issue.** `RecordId::for_record` (weir-sink-sdk) is SHA-256 over the
segment identity, the record's index in the segment and the payload. The
segment identity is the file's path relative to `wab_dir`
(`shard_03/seg_00000042.wab.sealed`), and every `wab_dir` numbers its segments
from 1 in each shard; the `.confirmed` tombstones are what stop the counter
rewinding within one directory. Nothing in the id names the directory or the
instance. So two daemons feeding one destination, or one daemon whose
`wab_dir` is wiped, recreated, restored from an older backup or stripped of its
tombstones, reuse coordinates. A record whose bytes match the earlier record at
the same coordinate (a heartbeat, a fixed-shape metering event: the case the id
was introduced for) gets an identical id. Every sink that dedups on the id
then drops it silently: MySQL `ignore`, Postgres `on_conflict_do_nothing`, and
`plain` mode with an `id_column` (which counts a present id as already
delivered), and an HTTP receiver that honours the per-record
`Idempotency-Key`. weir acked and delivered the record; it is never stored and
nothing is dead-lettered. The S3 sink is not affected in practice: its object
key also carries the segment's `created_at`.

**Mitigation today (documentation).** `docs/operations/configuration.md`
(the MySQL and Postgres `id_column` and `insert_mode` sections) says to feed
each table from one weir instance with a persistent `wab_dir`, and to give
each instance its own table when several must feed one destination.

**Candidate fix.** Persist a random per-`wab_dir` instance nonce, created once
with the directory (next to `.weir.lock`, fsynced, never rewritten), and mix it
into the id: `SHA-256(nonce ++ segment identity ++ index ++ payload)` with the
same length prefixes. A recreated directory gets a new nonce, so its ids cannot
meet the old ones; a replay of the same directory keeps its nonce, so retries
still dedup. Open questions for the design pass: every id changes across the
upgrade, so a segment written before it and replayed after would be inserted a
second time (derive pre-upgrade segments' ids the old way, keyed on the WAB
format version, or drain to empty before upgrading); a backup restored *with*
its nonce still collides with its own future, which only a nonce rotated on
restore detects; and `weir-attest` and the published test vectors recompute
ids and would need the nonce too, which puts it on the attest verification
path.

## Other deferred items (detail in the exploration docs)
- **`LazySync` durability tier** (ack after write, before fsync) — Phase 5 / API-lock.
- **Embeddable library** (`weir-embedded`) — Phase 5.
- ~~**Generalized dedup token → WAB format v2**~~ — **DONE in 2.0**, but not as
  described here. The dedup token turned out not to need an on-disk field at all:
  it must be *batch*-scoped (the drain splits a segment into `max_batch_size`
  commits, so a segment-scoped token would make a dedup-capable sink discard
  every chunk after the first), and a batch-scoped token is derived by the drain
  at commit time. Format v2 therefore carries only a header flags byte for
  compression.

  **Consequence worth not rediscovering: encryption-at-rest has no reserved slot
  waiting.** The 2.0 design deliberately declined to grow the footer — reserving
  32 bytes for a digest nothing consumed would have charged the hot path and
  contradicted the documented "CRC32 detects accidental corruption, not malicious
  modification" position in `weir-wab`'s format module. Encryption will need its
  own format bump. That is cheap now: the v2 work built a version-routing reader,
  which is the actual reusable asset — more so than reserved bytes would have
  been.

  **Follow-up, 2.0.3: the *per-record* half landed too, also without a format
  change.** This entry originally wanted a token "stable across retries but
  distinct per record" and assumed it needed an on-disk field. It did not.
  `weir_sink_sdk::RecordId` mixes the record's WAB *coordinate* — the segment
  file name plus its index within that segment — into the digest, and the drain
  knows both at read time without storing anything. That is now the HTTP sink's
  per-record `Idempotency-Key`; `DedupToken` stayed exactly as described above,
  because the two answer opposite questions (a retry token must be *stable*, an
  idempotency key must be *unique*) and one digest cannot be both.

  The cost, worth not rediscovering: a coordinate-derived key does **not**
  survive `weir-ctl dl requeue`, which re-pushes records into a new segment. The
  drain's own retry is unaffected — it re-reads the same coordinate.

## Observability extras (parked from thread #4 — 2026-06-13)

Thread #4 ships the standard stack (the Prometheus exposition `/metrics` is
already built; we add a Grafana `weir-dashboard.json`, a Prometheus
`weir-alerts.yml`, a `docker-compose` Prometheus+Grafana+weir example, and a
runbook). The decision **not** to build a historical-data API or a custom view
framework into weir-core stands — that reinvents Prometheus (a TSDB) and Grafana
(the customizable view layer); weir's leverage is speaking the *standard* format
so it plugs into the whole ecosystem for free. Two kernels of the user's idea are
worth revisiting later:

- **Built-in status page** — a tiny, read-only "am I healthy" HTML/text view off
  the existing metrics (e.g. a `/status` route or a `weir-ctl status` TUI), for
  `cargo install` users who don't run Prometheus/Grafana. Small, optional,
  **not** a framework — Phase 5 polish, not a monitoring platform.
- **Data tap / firehose** — a live *stream of the records themselves* flowing
  through the buffer (vs health metrics): an SSE/WebSocket endpoint or a fan-out
  `tap` sink to observe buffered data in real time. A genuine product feature,
  distinct from health monitoring and larger in scope. Post-1.0.
- **TLS-failure chaos scenario** — the monitoring stack's `chaos` profile already
  demonstrates dead-letter, degraded-sink, and peer-UID-rejection panels lighting
  up under real faults (`deploy/monitoring/`, opt-in `--profile chaos`). The one
  gap is **TLS handshake failures / cert reloads**: those metrics are gated behind
  the `tls` build feature (absent from the default image), so the panel renders a
  clean `0` for now. Phase 5: ship a `--features tls` chaos image + a bad-cert
  probe so that panel earns its place too. The chaos harness (failmock + a
  mismatched-uid probe + a tight dead-letter cap) is the reusable foundation —
  a TLS probe drops straight into it.

## Publish readiness (Phase 5 — crates.io + public repo)

Confirmed weir publishes fine as a "thick" multi-crate workspace:
- **Size is a non-issue** — crates.io's ~10 MiB per-crate *source* tarball limit
  is far above weir's pure-Rust crates (each ≪ 1 MiB). LOC/feature thickness is
  irrelevant.
- **It's an application, which is fine** — crates.io hosts full daemons/binaries
  (ripgrep, fd, the cargo-* tools …); `cargo install weir-server` builds it.
- **Publish each member separately, in dependency order**: `weir-core` →
  `weir-sink-sdk` → `weir-client` / `weir-server` / `weir-ctl`. `weir-testkit`
  stays `publish = false` (dev-only). Phase 1 already added the crates.io
  metadata + pinned the internal `version` deps; `cargo publish --dry-run` per
  crate validates.
- **Making the GitHub repo public** is a separate toggle from crates.io and a
  timing call (now exposes the WIP + full history; at 1.0 is the clean cut).
