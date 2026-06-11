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
