# Phase 4 Exploration — Kubernetes Deployment & Packaging

**Status:** exploration only — no code committed  
**Branch context:** `v1/phase-3-performance` (→ 0.8.0)  
**Author context:** weir is a durable write-buffer daemon; the WAB (write-ahead buffer) is on-disk and fsync-bound. Every decision in this doc flows from that fact.

---

## 1. Opportunity and Why It Fits weir

weir ships with a solid Docker foundation (`deploy/docker/Dockerfile`, `deploy/docker/docker-compose.yml`) and a non-root `weir` user pinned at UID/GID 10001. The image already declares `VOLUME ["/var/lib/weir/wab", "/run/weir"]`, sets `STOPSIGNAL SIGTERM`, and has a TCP-port healthcheck on `:9185`. Kubernetes packaging is the natural next step: it broadens the addressable deployment base significantly, especially for shops already running k8s, and it aligns with the Phase 4 theme of "operational maturity."

The driver tension is that **weir is stateful in a durable, crash-recovery sense.** The WAB is not a cache — it contains segments that may not yet be confirmed to the sink. Lose that directory between process lifetimes and you lose records. That makes weir's stateful contract materially stronger than most microservices, and it shapes every decision below.

The secondary tension is the **fsync-bound write path**. Phase 3 demonstrated that on a SATA SSD, `fdatasync` is ~99% of Sync-tier latency (~1.4 ms p50). Any storage class that introduces extra network round-trips into the fsync call will directly harm Sync-tier write latency — not just throughput, but the latency that producers observe on every acknowledged record.

---

## 2. Concrete Approaches

### Approach A — StatefulSet with a Per-Pod PVC (Recommended)

**What:** Deploy weir as a Kubernetes `StatefulSet` with a `volumeClaimTemplate` for the WAB. Each pod gets its own `PersistentVolumeClaim` (PVC) backed by a local-storage class or a block-storage class (EBS, GCP PD, Azure Disk).

**How it maps to weir:**

- The WAB directory (`/var/lib/weir/wab`, configured via `WEIR_WAB_DIR`) is mounted from the PVC. The `wab_dir` path validation in `config/mod.rs` (`validate_path`) calls `fs::canonicalize` at startup, so the mount point must exist before the daemon starts — the PVC mount satisfies this.
- The Unix socket (`/run/weir/weir.sock`, configured via `WEIR_SOCKET_PATH`) lives on an `emptyDir` tmpfs. It does not need to survive pod restarts (the hardened `bind_hardened` in `socket/mod.rs` already handles stale sockets via `unlinkat`). Sidecar producers connect to the socket from within the same pod via a shared `emptyDir` volume.
- The StatefulSet ordinal gives each pod a stable DNS name (`weir-0.weir`, `weir-1.weir`, ...) which is useful if producers need to address specific shards.

**Tradeoffs:**

- Correct model for weir's durability contract: the PVC follows the pod across reschedules (assuming the pod returns to the same node or the storage is network-attached and the PVC is rebound).
- StatefulSets update pods sequentially by default, which is desirable: weir's crash-recovery path (`recover_open_segments` + `replay_unconfirmed` in `wab/mod.rs`) must run to completion before the new version accepts traffic.
- Pods are not interchangeable (each has its own WAB state), so a `Deployment` would be wrong — it does not guarantee PVC-per-pod identity.

---

### Approach B — Deployment with Host-Path Volume (Simpler, Node-Pinned)

**What:** Run weir as a `DaemonSet` or a `Deployment` with `nodeName`/`nodeSelector` pinning, backed by a `hostPath` volume pointing to a local directory on the node.

**How it maps to weir:**

- `hostPath` maps directly to the metal: the fsync goes to the local NVMe/SSD without a network hop. This is the fastest possible storage option and matches the bare-metal benchmark environment from Phase 3 (`beast`, ext4 on SATA SSD, `fdatasync` ~1.4 ms).
- The `DaemonSet` model makes sense for deployments where every node runs producers (e.g. log collection, metrics buffering) — one weir per node, producers local via Unix socket, no cross-node communication.

**Tradeoffs:**

- The pod is pinned to a specific node. If that node is drained (maintenance, failure), the pod is evicted and the WAB data remains on that node's disk until the pod reschedules back there. This is not a data-loss risk (the WAB is durable), but it may cause a gap in drain throughput.
- `hostPath` volumes bypass the Kubernetes storage API entirely — no capacity accounting, no access control, no dynamic provisioning. The `chmod 0700` on `/var/lib/weir/wab` (set in the Dockerfile) must be pre-applied on the host, or done via an `initContainer`.
- Less portable than a PVC-backed StatefulSet.

---

### Approach C — TCP+mTLS with Per-Node StatefulSet (Cross-Node Producers)

**What:** Build weir with `--features tls` and enable `tcp_bind` (e.g. `0.0.0.0:7100`). Producers on other nodes connect via TCP+mTLS. weir is still a StatefulSet (approach A) for its WAB, but now accessible from outside the pod.

**How it maps to weir:**

- The `tcp_bind` / `WEIR_TCP_BIND` config key triggers the mTLS listener in `main.rs` (the `#[cfg(feature = "tls")]` block). TLS cert material (`WEIR_TLS_CERT`, `WEIR_TLS_KEY`, `WEIR_TLS_CLIENT_CA`) must be mounted into the container — Kubernetes `Secret` volume mounts are the right mechanism here.
- The `SIGHUP` handler in `main.rs` (`spawn_tls_reload_task`) enables cert rotation without pod restart. In k8s, cert-manager can rotate the Secret in-place and a `kubectl rollout restart` or a custom SIGHUP sender (e.g. a `lifecycle.postStart` or a sidecar watcher) can trigger the reload.
- The shared connection semaphore (`conn_sem`) enforces a single `max_connections` cap across both Unix and TCP listeners (`max_connections = 256` default in `config/mod.rs`). If both transports are active, tune `max_connections` to account for both local and remote producers.
- `peer_uid_check` must be set to `false` when TCP producers connect (UID check is only meaningful for the Unix socket path; the config note in `config/mod.rs` and `socket/mod.rs` is explicit about this).

**Tradeoffs:**

- Adds TLS infrastructure overhead: a CA, cert issuance, rotation pipeline. cert-manager or Vault PKI handles this well in k8s, but it is non-trivial to set up.
- Increases latency slightly for producers on remote nodes (TCP RTT + TLS handshake overhead vs Unix socket). For Sync-tier records where the entire latency budget is ~1.4 ms (SATA) to ~150 µs (NVMe), a same-AZ TCP RTT (~0.1–0.5 ms) is meaningful. Plan for producers to use the Unix socket wherever co-location is possible.
- Appropriate for deployments where producers and weir cannot share a pod (different scaling dimensions, different teams, etc.).

---

### Approach D — Minimal StatefulSet (Unix Socket Only, Sidecar Pattern)

**What:** Deploy weir as a StatefulSet pod with producers as sidecars in the same pod. Unix socket shared via an `emptyDir`. No TCP listener needed.

**How it maps to weir:**

- This is the tightest integration model: Unix socket lives on `emptyDir` (tmpfs), producers share it as a volume mount. No network hop for the hot path.
- `peer_uid_check` can stay `true` only if producers run as the same UID as the daemon (10001). If the producer process runs as a different user, set `WEIR_PEER_UID_CHECK=false` and rely on the `emptyDir` volume's restricted access (pod-level isolation) as the trust boundary — exactly what the config doc recommends.
- WAB on a PVC (as in Approach A).

**Tradeoffs:**

- Least operational complexity. No TLS, no cross-node plumbing.
- Couples producer and weir lifecycle: a producer crash can affect the weir pod and vice versa. For latency-sensitive workloads where the producer and weir are naturally co-located (e.g. an application pod that emits events), this is fine.
- Does not work for producers that are not Kubernetes workloads (bare-metal machines, external clients).

---

## 3. Mapping to weir's Actual Code and Config

### 3.1 StatefulSet vs Deployment

The WAB stores sealed segments in `<wab_dir>/shard_XX/` (created by `create_dir_private` in `wab/mod.rs`). At startup, `recover_open_segments` seals any unsealed `.wab` files from a previous crash, and `replay_unconfirmed` re-queues any `.wab.sealed` segments not yet confirmed to the sink. These paths run on the calling thread before any shard flusher is spawned — this is the Postgres "startup replay" model referenced in `config/mod.rs`. A Deployment would not guarantee that the same pod gets the same PVC on reschedule; a StatefulSet does, via `volumeClaimTemplate`.

### 3.2 PVC Storage Class and the Fsync Caveat

**This is the most important operational constraint in this document.**

Phase 3 found that `fdatasync` is ~99% of Sync-tier write latency on SATA SSD and ~89% on NVMe (see `docs/benchmarks/phase3-results.md`). Network-attached storage introduces additional round-trips inside `fdatasync`:

| Storage type | Expected `fdatasync` latency | Safe for Sync tier? |
|---|---|---|
| Local NVMe (instance store) | ~150 µs | Yes — matches Phase 3 Mac baseline |
| AWS EBS io2 (same-AZ) | ~200–600 µs | Marginal — provisioned IOPS helps |
| AWS EBS gp3 (same-AZ) | ~1–5 ms | Similar to SATA SSD; acceptable for most workloads |
| GCP Persistent Disk SSD | ~1–3 ms | Similar to EBS gp3 |
| Azure Premium SSD | ~1–2 ms | Similar |
| NFS / EFS / networked filesystems | 5–50+ ms | Not safe for Sync tier |
| `hostPath` on local SSD | As fast as the hardware | Best; use for latency-critical deployments |

Recommendation for Sync-tier deployments: use a storage class backed by block-level storage with per-AZ placement (`WaitForFirstConsumer` binding mode) to guarantee the PVC lands on a node local to the pod. For the lowest latency, use instance-store NVMe (not network-attached) with a `hostPath` or a local-volume provisioner (e.g. the Kubernetes local PV provisioner).

For Buffered-tier deployments (where `fdatasync` is never called), any network-attached storage class is fine — the bottleneck shifts to queue throughput and sink network RTT.

The `wab_segment_max_bytes` default of 256 MiB means a full segment rotation creates a 256 MiB drain-to-sink event. Ensure the PVC capacity is at least `shard_count × wab_segment_max_bytes × 2 + dead_letter_max_bytes` (the factor of 2 covers the active segment plus one sealed-but-unconfirmed segment per shard). The default `dead_letter_max_bytes` is 1 GiB (`config/mod.rs` line 577), so a single-shard default setup needs at minimum ~1.5 GiB.

### 3.3 Probes

**Liveness probe:** HTTP GET `http://localhost:9185/metrics` (or the `/dev/tcp` bash trick from the Dockerfile). The metrics server (`metrics/server.rs`) comes up only after the Tokio runtime starts and the pipeline is running (it is spawned before `socket::run` in `main.rs`). A successful HTTP response confirms the daemon is alive. Recommended: `initialDelaySeconds: 10`, `periodSeconds: 10`, `failureThreshold: 3`.

**Readiness probe:** Same endpoint. weir is ready to accept producers as soon as the metrics endpoint responds — the Unix socket listener is bound before the metrics server (`socket::run` is the final blocking call, but the metrics server and queue are set up before it). A more precise readiness signal would be a custom `/health` endpoint that also checks the WAB flusher state (non-zero `weir_wab_flusher_panics_total` means a shard is offline), but that requires a new HTTP handler. For now, the metrics endpoint is sufficient.

**Startup probe:** The WAB crash-recovery path (`recover_open_segments` + `replay_unconfirmed`) runs on the calling thread before the metrics server comes up. On a large WAB with many unconfirmed segments, this can take tens of seconds. Set `startupProbe.failureThreshold` high enough (e.g. 30 failures × 10s = 300s budget) or use `initialDelaySeconds` to cover the expected recovery window. Do not set `livenessProbe.initialDelaySeconds` so short that it fires during crash recovery.

### 3.4 Graceful Shutdown

`main.rs` installs a SIGTERM handler (`signal(SignalKind::terminate())`) that:
1. Signals the Unix socket accept loop to stop accepting.
2. Waits up to `shutdown_timeout_secs` (default 30s) for in-flight connections to drain.
3. After the socket layer returns, waits for workers → WAB flushers → drain thread to finish (the sequential join order documented in `main.rs` lines 579–589).

The total shutdown time under load can be `shutdown_timeout_secs` (for the socket layer) plus the time for the drain thread to flush the last segment to the sink. The Kubernetes `terminationGracePeriodSeconds` must exceed this sum. A safe formula:

```
terminationGracePeriodSeconds = shutdown_timeout_secs + drain_segment_flush_budget_secs + buffer_secs
```

Where `drain_segment_flush_budget_secs` is the time to commit one full segment to the sink at the observed `sink_commit_duration` rate. For most deployments, `terminationGracePeriodSeconds: 120` with `WEIR_SHUTDOWN_TIMEOUT_SECS=90` is a safe starting point. The config doc's advice to set `shutdown_timeout_secs` a few seconds below the orchestrator's grace matches this formula.

### 3.5 Resource Requests and Limits

The `advise_agent_count` function in `main.rs` (lines 620–656) provides the empirical heuristic: `recommended = max(1, (cores - 2) / 2)`. Each "agent" is a (worker thread + flusher thread) pair. For the default `shard_count=1, worker_count=1` config, the thread pool is:

- 1 flusher thread (`wab-flusher-0`)
- 1 worker thread
- Tokio multi-thread runtime workers (default: `std::thread::available_parallelism()` — typically core count)
- 1 drain thread
- Tokio blocking thread pool (used by `spawn_blocking` in the socket accept loop)

A sensible baseline for a single-shard deployment:
- `requests.cpu: 500m` (0.5 cores) — covers steady-state under moderate load
- `limits.cpu: 2000m` (2 cores) — allows bursts; the tokio runtime adapts to available cores
- `requests.memory: 256Mi` — WAB scratch buffer (pre-touched in `flusher_thread`, 64 KiB), socket frame buffers, payload heap
- `limits.memory: 512Mi` — with default 256 MiB `wab_segment_max_bytes`, the active segment's backing store is on disk, not in memory; heap usage is dominated by the queue depth and in-flight payloads

For multi-shard deployments, scale requests roughly linearly with `shard_count`. The `advise_agent_count` advisory will log a warning at startup if the configured agent count looks wrong for the container's visible core count — watch for that in pod logs during initial sizing.

### 3.6 Unix Socket vs TCP Transport Decision In-Cluster

**Same pod (sidecar model):** Unix socket via `emptyDir`. Zero-overhead, no TLS needed. This is the preferred model when the producer is a sidecar container.

**Cross-pod, same node:** Unix socket via `hostPath` shared between pods. Possible but fragile (depends on node-level directory sharing, not a k8s primitive). Not recommended.

**Cross-pod, any node:** TCP+mTLS (`--features tls`). Enable `tcp_bind`, mount certs from a `Secret`. Use cert-manager for issuance and rotation; configure a `SIGHUP`-sending sidecar or `lifecycle.postStart` hook for cert reload without pod restart.

**Cross-node recommendation:** mTLS is mandatory for the TCP path (the config system enforces this — `tcp_bind` without TLS paths is a fatal startup error per `config/mod.rs` lines 618–623). Plaintext TCP is never exposed by weir, which is a strong correctness guarantee: a misconfigured deployment cannot accidentally expose an unprotected buffer.

### 3.7 Secrets Management

Config values that are secrets:

| Secret | Env var | Recommended k8s source |
|---|---|---|
| Sink credentials (DB URL or HTTP bearer token) | `WEIR_SINK_URL`, `WEIR_SINK_BEARER_TOKEN` | `Secret` → `envFrom` or `env[].valueFrom.secretKeyRef` |
| TLS server cert + key | Files at `WEIR_TLS_CERT`, `WEIR_TLS_KEY` | `Secret` → volume mount at `/etc/weir/tls/` |
| TLS client CA | File at `WEIR_TLS_CLIENT_CA` | Same Secret volume |

**Do not** put sink credentials in the TOML file (the config doc is explicit: `sink_url` and `WEIR_SINK_BEARER_TOKEN` are env-only for this reason). The `main.rs` startup log redacts the URL for MySQL/Postgres/ClickHouse sinks (table/column logged, not the URL) and the bearer token is logged only as a presence boolean.

---

## 4. Helm Chart Structure

A Helm chart is the right packaging primitive: it handles templating of config values across environments (dev/staging/prod), lifecycle of the PVC + StatefulSet together, and upgrades with `helm upgrade`.

Proposed top-level layout:

```
charts/weir/
  Chart.yaml
  values.yaml
  templates/
    statefulset.yaml
    service.yaml               # ClusterIP exposing port 9185 for Prometheus scraping
    servicemonitor.yaml        # Optional: Prometheus Operator ServiceMonitor
    configmap.yaml             # weir.toml rendered from values
    secret.yaml                # sink credentials (if not using external-secrets)
    pdb.yaml                   # PodDisruptionBudget: minAvailable=1
    _helpers.tpl
```

Key `values.yaml` knobs (mapping to weir's config system):

```yaml
image:
  repository: weir-server
  tag: "0.8.0"
  pullPolicy: IfNotPresent

replicaCount: 1  # StatefulSet replicas; each gets its own PVC

storage:
  storageClassName: ""       # "" = cluster default; set to "local-nvme" for latency-critical
  size: 10Gi                 # Must exceed: shard_count × wab_segment_max_bytes × 2 + dead_letter_max_bytes
  accessMode: ReadWriteOnce

config:
  shardCount: 1
  workerCount: 1             # Defaults to shardCount in weir; set explicitly here to be clear
  batchSize: 256
  batchDeadlineMs: 1
  maxConnections: 256
  shutdownTimeoutSecs: 90    # Must be < terminationGracePeriodSeconds

  metrics:
    port: 9185
    bind: "0.0.0.0"          # In-cluster, 0.0.0.0 is safe; restricted by NetworkPolicy

  sink:
    type: "noop"             # Override per environment
    url: ""                  # Always inject from Secret, not values.yaml
    timeoutSecs: 10

  deadLetter:
    maxBytes: 1073741824     # 1 GiB default

  logLevel: "info"

  # TCP+mTLS (optional)
  tcp:
    enabled: false
    bind: "0.0.0.0:7100"
    # cert, key, ca: paths to files mounted from a Secret

terminationGracePeriodSeconds: 120

resources:
  requests:
    cpu: "500m"
    memory: "256Mi"
  limits:
    cpu: "2000m"
    memory: "512Mi"

probes:
  liveness:
    initialDelaySeconds: 10
    periodSeconds: 10
    failureThreshold: 3
  readiness:
    initialDelaySeconds: 5
    periodSeconds: 5
    failureThreshold: 3
  startup:
    failureThreshold: 30     # 300s budget for WAB crash recovery
    periodSeconds: 10

podDisruptionBudget:
  minAvailable: 1

serviceMonitor:
  enabled: false             # Requires Prometheus Operator CRD
  interval: "30s"

existingSecretName: ""       # External secrets approach: name of a pre-created k8s Secret
                             # that contains WEIR_SINK_URL and optionally WEIR_SINK_BEARER_TOKEN
```

**`configmap.yaml` approach:** render `weir.toml` from `values.yaml` into a ConfigMap. Mount it at `/etc/weir/weir.toml`. The TOML config precedence (CLI > env > file > defaults) means env vars injected from Secrets override the file-level config — the sink URL from the Secret overrides any stale value in the ConfigMap.

**PodDisruptionBudget:** with a single-replica StatefulSet (the typical weir deployment), `minAvailable: 1` means no voluntary disruption is permitted. This is conservative but appropriate: draining the pod means the WAB is temporarily offline for producers. Operators who want to allow rolling node maintenance should use a 2-replica setup (two independent weir instances, producers route to both).

---

## 5. Effort, Risk, and Dependencies

### Effort

| Component | Estimated effort | Notes |
|---|---|---|
| Helm chart (StatefulSet + PVC + probes + Service) | 2–4 days | Straightforward templating; no code changes to weir |
| CI: chart lint + `helm install` in kind cluster | 1 day | `helm lint`, `helm test`, basic smoke test |
| TCP+mTLS in-cluster guide (cert-manager integration) | 1–2 days | Docs; cert-manager is well-understood |
| SIGHUP-based cert reload helper (sidecar or hook) | 1 day | Small sidecar that watches a cert Secret and sends SIGHUP |
| `/health` endpoint (optional, improves readiness probe) | 1–2 days | New HTTP handler in `metrics/server.rs`; checks flusher panic count |
| Helm chart documentation | 1 day | |

Total: 7–11 days, mostly in the chart and CI plumbing.

### Risk

**High: network-storage fsync latency.** The most likely operational mistake is deploying weir on a network-attached storage class with a default StorageClass that does not optimize for fsync. EFS or NFS would make Sync-tier writes catastrophically slow (5–50+ ms `fdatasync`). The chart's `values.yaml` should default `storageClassName` to `""` (cluster default) with a prominent comment warning about this, and the docs should prominently flag the storage class selection as the highest-impact operational choice.

**Medium: WAB crash-recovery startup time.** If a pod is evicted while mid-segment (e.g. an OOM kill), the next startup runs `recover_open_segments`. On a large WAB with many segments, this can be slow. The startup probe budget of 300s is generous but operators should be aware. A future improvement (out of scope for Phase 4) would be a recovery progress metric.

**Medium: terminationGracePeriodSeconds misconfiguration.** If the orchestrator's grace period is shorter than `shutdown_timeout_secs + drain_flush_budget`, pods will be SIGKILL'd with in-flight records. The chart defaults should be conservative (`terminationGracePeriodSeconds: 120`, `shutdown_timeout_secs: 90`) and the docs must explain the formula clearly.

**Low: Unix socket across containers.** Sharing the socket via `emptyDir` between sidecar containers is well-supported in k8s, but operators may be surprised that the socket path must match between the weir container's `WEIR_SOCKET_PATH` and the producer's configured path. The chart should template both and document the volume mount.

**Low: peer_uid_check with sidecars.** If a sidecar producer runs as a different UID, the default `peer_uid_check=true` will reject connections. The chart should document this and recommend `WEIR_PEER_UID_CHECK=false` for multi-container pods unless the producer UID can be pinned to 10001.

### Dependencies

- Kubernetes 1.25+ (StatefulSet, VolumeClaimTemplates, StartupProbe — all available in 1.18+, but 1.25 is a reasonable minimum for production)
- For TCP+mTLS: `weir-server` built with `--features tls` — the default Dockerfile does not enable this feature. A second image tag (e.g. `weir-server:0.8.0-tls`) or a build arg is needed.
- For cert-manager integration: cert-manager 1.x in the cluster
- For ServiceMonitor: Prometheus Operator 0.50+

---

## 6. Recommendation

**Start with a StatefulSet + PVC-backed chart (Approach A) targeting local-block storage classes.**

Rationale:
1. It correctly models weir's durability contract without requiring operators to understand the WAB internals — the PVC is the natural k8s abstraction for "this pod needs durable local state."
2. It avoids the TCP+mTLS complexity for the common case (sidecar producers, same pod). Most initial k8s deployments will use the sidecar pattern.
3. The Unix socket sidecar model (Approach D) can be layered on top of the StatefulSet without any additional weir code changes.
4. TCP+mTLS (Approach C) should be documented but treated as an opt-in, requiring the `--features tls` image build. It's the right choice for cross-node producers but adds operational surface that not all deployments need.

**The single most important thing to get right in the chart is the storage class selection.** The comment in `values.yaml` and the phase4 docs must say clearly: "for Sync-tier workloads, use a local-block storage class backed by NVMe or SSD; do not use NFS, EFS, or other network filesystems — they will cause unacceptable fdatasync latency." This is the finding from Phase 3 applied directly to k8s operations.

**Secondary priority: probe configuration.** The startup probe must have enough budget to cover WAB crash recovery. The liveness probe must not fire during recovery. The termination grace period must exceed `shutdown_timeout_secs + drain_budget`. These are all mechanical once the timing model is understood, but getting them wrong causes data-loss-risk situations in production.

---

## 7. Open Questions for the User

1. **Storage class strategy:** Do you already have a preferred StorageClass in your target cluster(s)? If it's EBS gp3 or similar block storage, that's fine for most workloads. If it's EFS/NFS-backed, we need to either scope weir to Buffered-only in that environment or make the local-PV path a first-class option in the chart.

2. **Producer topology:** Are producers expected to be (a) sidecars in the same pod, (b) separate pods on the same node, or (c) separate pods on different nodes? This determines whether TCP+mTLS is needed at all for Phase 4, or whether it can be deferred.

3. **Image build strategy for TLS:** The current Dockerfile does not pass `--features tls`. Should the chart assume a TLS-capable image exists as a separate tag, or should the Dockerfile be amended to build with TLS by default (accepting the larger binary and the OpenSSL-free rustls dependency)?

4. **`/health` endpoint:** The current health signal is "TCP connection to `:9185` succeeds." A richer readiness probe (one that returns 503 if `weir_wab_flusher_panics_total > 0`) would avoid sending traffic to a pod with a permanently offline shard. Is this worth implementing before the chart, or ship with the metrics-endpoint probe and add it in a follow-up?

5. **Multi-replica topology:** The current architecture assumes weir is a singleton per node (or per set of producers). Is there a use case for running N weir replicas behind a load balancer (horizontally sharded producers)? If so, that changes the StatefulSet design significantly (producers need to know which replica to connect to, or TCP+round-robin load balancing must be fine with Buffered-tier only).

6. **Cert lifecycle:** If TCP+mTLS is in scope, what's the intended cert management approach — cert-manager (automated), Vault PKI (operator-managed), or manual rotation? The SIGHUP cert-reload path in `main.rs` (`spawn_tls_reload_task`) supports in-place rotation without restart, but triggering SIGHUP from a k8s cert lifecycle event requires a small integration shim.

---

*Generated during Phase 3 → Phase 4 scoping. Ground truth: `crates/weir-server/src/main.rs`, `socket/mod.rs`, `wab/mod.rs`, `config/mod.rs`, `deploy/docker/Dockerfile`, `deploy/docker/docker-compose.yml`, `docs/benchmarks/phase3-results.md`, `docs/operations/configuration.md`.*
