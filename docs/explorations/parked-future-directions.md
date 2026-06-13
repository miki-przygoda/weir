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

## Other deferred items (detail in the exploration docs)
- **`LazySync` durability tier** (ack after write, before fsync) — Phase 5 / API-lock.
- **Embeddable library** (`weir-embedded`) — Phase 5.
- **Generalized dedup token → WAB format v2** — Phase 5 (on-disk format change).

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
