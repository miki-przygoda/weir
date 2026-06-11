# Phase 4 Exploration — Kubernetes Operator + CRD for weir

**Status:** research / proposal only — nothing implemented.
**Scope:** Whether a declarative Kubernetes operator is worth building, what it would
manage, what the hard problems are, and how it compares to a plain Helm chart.

---

## 1. Opportunity and Is-It-Worth-It?

### The problem a Kubernetes operator solves

weir's config surface is non-trivial: 30+ fields spanning shard topology, WAB tuning,
durability tier, sink credentials (database URL, bearer token, table names), TLS cert
paths, and Prometheus settings. A production deployment that runs multiple weir instances
(e.g. one per application namespace, or one per ClickHouse cluster) today requires
manually templating TOML files, provisioning PersistentVolumeClaims for `wab_dir`,
keeping ConfigMaps in sync with Secret rotations, and coordinating rolling restarts
when config or TLS certs change. There is no audit trail of "who changed
`shard_count`", no automatic remediation if a pod is deleted, and no structured place
to put readiness semantics beyond "is the pod Running?".

A Kubernetes operator adds:

- **Declarative config** — one CRD spec drives the StatefulSet, ConfigMap, and Secret
  projection, so diffs are visible via `git diff` and `kubectl diff`.
- **Cert rotation automation** — the operator can detect cert Secret changes and send
  SIGHUP to the running pod rather than requiring a restart.
- **Status subresource** — surfaces health (sink state, WAB bytes, drain backlog)
  scraped from `/metrics` into the CR's `.status`, making `kubectl get weirinstance`
  meaningful.
- **Shard topology safety gates** — can refuse or drain safely before applying a
  `shard_count` change (explained in §3).

### Is it worth it now?

**Probably not yet.** weir just finished Phase 3 (performance) and is at v0.8.x. The
daemon has no HA clustering, no replication, and a single PVC per instance — the
operational surface is still simple enough for a Helm chart to cover. An operator
requires a non-trivial Rust binary (kube-rs reconciler loop, CRD YAML, RBAC manifests,
a separate Docker image) and a long-lived maintenance commitment. The value/complexity
ratio only tips positive when:

1. Users run 3+ weir instances, or
2. Cert rotation becomes a support burden (today: manual SIGHUP), or
3. The shard-topology safety gate is blocking a real operational mistake (a user
   changing `shard_count` on a live pod without draining).

For Phase 4 the right investment is a **Helm chart** (low-effort, covers 95% of the
use-case) with the operator left as a future "if operators ask for it" item. The rest of
this document designs what the operator *would* look like so the decision is grounded.

---

## 2. CRD Design Sketch

### 2.1 Resource: `WeirInstance`

A single-instance resource (one Kubernetes pod backed by a StatefulSet with
`replicas: 1`). Multiple instances across namespaces are represented by multiple
`WeirInstance` objects; there is no cluster-scoped peer awareness.

```yaml
apiVersion: weir.io/v1alpha1
kind: WeirInstance
metadata:
  name: analytics
  namespace: data-pipeline
spec:
  # Image
  image: ghcr.io/your-org/weir-server:0.8.0
  imagePullPolicy: IfNotPresent

  # WAB storage — operator creates PVC if not pre-existing
  storage:
    size: 20Gi
    storageClassName: fast-ssd   # must support ReadWriteOnce

  # Shard topology (the "hard" field — see §3.1)
  shardCount: 4
  workerCount: 4

  # Durability / batching
  batchSize: 256
  batchDeadlineMs: 1
  wabSegmentMaxBytes: 268435456   # 256 MiB

  # Connection limits
  maxConnections: 256
  maxPayloadBytes: 65536

  # Sink — exactly one of the sub-sections must be present
  sink:
    type: clickhouse               # noop | http | mysql | postgres | clickhouse
    clickhouse:
      url:
        secretKeyRef:
          name: weir-ch-creds
          key: url
      database: default
      table: weir_records
      column: payload
      maxBatchSize: 100
      timeoutSecs: 10

  # TLS for TCP+mTLS listener (optional)
  tls:
    tcpBind: "0.0.0.0:7100"
    certSecret: weir-tls-cert     # Secret with keys: tls.crt, tls.key, ca.crt
    handshakeTimeoutSecs: 10

  # Metrics
  metrics:
    port: 9185
    bind: "0.0.0.0"              # allow Prometheus scraping from other pods

  # Shutdown
  shutdownTimeoutSecs: 30

status:
  # Populated by operator from /metrics scrape
  phase: Running                  # Pending | Running | Degraded | Unknown
  conditions:
    - type: SinkHealthy
      status: "True"
    - type: WabDrained
      status: "True"
  wabBytesOnDisk: 1048576
  drainBacklogSegments: 0
  sinkState: Draining
  observedShardCount: 4           # what the running pod reports
  desiredShardCount: 4
  lastReconciledAt: "2026-06-11T10:00:00Z"
```

### 2.2 What the operator reconciles

| CR field change | Reconciler action |
|---|---|
| `image` | Rolling update of StatefulSet (replace pod; WAB persists on PVC) |
| `shardCount` (decrease) | **Block** unless WAB is drained; drain gate + annotation |
| `shardCount` (increase) | Update ConfigMap + restart pod; recovery creates new shard dirs |
| `batchSize`, `batchDeadlineMs`, tunables | Regenerate ConfigMap + SIGHUP if possible; else restart |
| `sink.*` non-credential | Regenerate ConfigMap + restart |
| `sink.*.url` Secret rotation | Detect Secret resourceVersion change → restart pod |
| `tls.certSecret` rotation | Detect Secret resourceVersion change → send SIGHUP to pod |
| `storage.size` | PVC resize (if StorageClass supports it); restart not required |
| CR deletion | `kubectl delete` → operator sets graceful drain annotation → pod SIGTERM |

### 2.3 Resources the operator manages

- `StatefulSet` (1 replica, `volumeClaimTemplates` or existing PVC reference)
- `ConfigMap` (rendered TOML from CR spec)
- `Secret` projection (sink URL, bearer token injected as env vars via `envFrom`)
- `Service` (ClusterIP exposing metrics port; optional LoadBalancer/NodePort for TCP)
- `ServiceMonitor` (if Prometheus Operator is present — opt-in annotation)
- `PodDisruptionBudget` (`minAvailable: 1` when multi-instance label matches)

---

## 3. The Hard Problems

### 3.1 Shard topology changes (`shard_count` change)

This is the most dangerous operator action. The WAB layout is:

```
wab_dir/
  shard_00/   seg_00000001.wab, seg_00000002.wab.sealed, ...
  shard_01/
  ...
  shard_N-1/
```

Record routing is `shard_id = hash(record) % shard_count` (actually round-robin in the
current impl, but the point is that `shard_count` is embedded in the routing decision at
the socket layer at startup). The critical code path:

- `main.rs`: `wab::spawn` is called with `config.shard_count`, creating exactly N
  flusher threads and N `Sender<Batch>` channels.
- `replay_unconfirmed` in `wab/mod.rs` (line 226) iterates `0..shard_count` and only
  looks at those N shard directories.

**Implication of increasing `shard_count` (e.g. 4 → 8):** On restart with the new
config, `recover_open_segments` in `recovery.rs` (line 22) iterates `wab_dir` and finds
all shard dirs — including `shard_00..shard_03` from the old run. `replay_unconfirmed`
only iterates `0..8` and would include the old shards (still numbered 0–3). New shard
dirs (4–7) are created empty. **This is safe**: old sealed segments replay normally
because recovery doesn't care which flusher originally wrote them; it just checks
`check_confirmed`. No data loss.

**Implication of decreasing `shard_count` (e.g. 8 → 4):** On restart,
`replay_unconfirmed` only scans `shard_00..shard_03`. Sealed segments in `shard_04..
shard_07` are completely **invisible to replay** — they will never be drained. Those
records are not lost (they remain on disk) but they are orphaned until an operator
manually moves them into an active shard directory or the shard count is restored.

**What the operator must do for a decrease:**
1. Before applying the new `shard_count`, check `.status.wabBytesOnDisk` and
   `.status.drainBacklogSegments`. If non-zero, block the change and surface a
   condition `ShardTopologySafe: False` with a human-readable reason.
2. Alternatively: drain all shards to completion (send SIGTERM, wait for drain thread
   to confirm all segments, verify via metrics that `weir_drain_segments_confirmed`
   matches `weir_wab_segments_total{state="sealed"}`), then apply.
3. After drain: rename or merge orphaned shard directories into the surviving shards
   (this requires a one-off migration job, not something weir itself supports today).

The `observed_shard_count` in `.status` (populated from the pod's startup log or a
metrics label) lets the operator detect config/runtime divergence during a botched
resize.

**Code anchor:** `wab/mod.rs::replay_unconfirmed` at line 289–330 is the exact loop
that would silently skip orphaned shards; any fix would need to iterate *all* existing
shard directories regardless of configured `shard_count`.

### 3.2 TLS cert rotation with zero-downtime SIGHUP

weir already supports zero-downtime cert reload: `main.rs::spawn_tls_reload_task` (line
68) installs a SIGHUP handler that calls `tls.reload()` — re-reading cert/key/CA from
disk. The current cert paths are baked into the config at startup; they point to
filesystem paths, not Kubernetes Secret mount paths.

In a Kubernetes pod, cert Secrets are projected into a `volumeMount` (e.g.
`/etc/weir/tls/`). Kubernetes rotates the mounted files automatically when the Secret
is updated (kubelet syncs within the `--sync-frequency`, default 60 s). The operator
does not need to restart the pod; it needs to:

1. Watch the `tls.certSecret` Secret for `resourceVersion` changes.
2. After a change, wait ~70 s for kubelet to sync (or watch the mounted file's mtime
   via a sidecar), then send `SIGHUP` to PID 1 of the weir container.

Sending SIGHUP to a pod in Kubernetes requires either:
- A sidecar container with `kubectl exec` + `kill -HUP` (messy, requires RBAC).
- A small lifecycle hook or a dedicated "cert-watcher" sidecar that uses `inotify` on
  the mounted cert files and sends SIGUP to the weir process via `kill(1, SIGHUP)`.
- The operator directly calling `pod/exec` via the Kubernetes API (cleanest).

The operator approach (pod/exec) is cleanest but requires the operator Pod's
ServiceAccount to have `pods/exec` verb on the target namespace — a meaningful RBAC
privilege that some security teams will object to.

**Code anchor:** `socket/tls.rs::ReloadableServerConfig::reload` is called by
`spawn_tls_reload_task`. It re-reads the cert paths stored at startup. If the operator
mounts certs at fixed paths (e.g. `/etc/weir/tls/tls.crt`), the reload works without
any code changes.

### 3.3 Stateful rolling upgrades

Because weir uses a StatefulSet with `replicas: 1` there is no true rolling upgrade
(no second replica to traffic-shift to). A version bump means a pod replacement with
a brief gap. The operator should:

1. Verify the WAB drain queue is empty before the old pod is terminated
   (`weir_drain_segments_confirmed` catches up to `weir_wab_segments_total{state=sealed}`
   within the `shutdown_timeout_secs` window — default 30 s).
2. Set `terminationGracePeriodSeconds` on the pod to match or exceed
   `shutdown_timeout_secs` so Kubernetes does not SIGKILL before the graceful drain
   completes. The current graceful drain sequence in `main.rs` (lines 579–589) handles
   this: SIGTERM → socket exits → workers flush → WAB seals → drain drains.
3. Validate the new image has format-compatible WAB segments before allowing the old
   pod to exit. Today FORMAT_VERSION is `1` (see `wab/format.rs`); the operator should
   check that the image's reported format version (e.g. via a `weir-server --version`
   exec) matches the on-disk segments. This is currently not exposed as a CLI flag —
   adding `--check-compat` would be a small addition.

---

## 4. Operator-in-Rust (kube-rs) vs. Helm-Only

### Helm chart only

**Effort:** 2–4 days for a complete chart with templated TOML ConfigMap, PVC, Service,
ServiceMonitor, and sensible values.yaml defaults.

**What it covers well:**
- Initial deployment, config templating, Secret reference injection.
- Image upgrades via `helm upgrade`.
- Basic shard count tuning (operator sets the right value before upgrading).

**What it does not cover:**
- Shard-decrease safety gate (nothing prevents an operator from changing `shardCount`
  in values.yaml without draining first).
- Automatic cert-rotation SIGHUP (requires manual `kubectl exec kill -HUP`).
- Status subresource (`kubectl get weirinstance` is just pod status).
- Drain-aware upgrade sequencing (relies on `terminationGracePeriodSeconds` being set
  correctly in values.yaml, but doesn't validate it matches the daemon's config).

**Risk:** Low. Helm is well-understood, easy to maintain, and trivially auditable.

### kube-rs Rust operator

**Effort:** 4–8 weeks for a production-quality operator (CRD generation, reconciler,
status population, SIGHUP exec, integration tests in `kind`). This is a separate
binary, a separate Docker image, separate RBAC manifests, and separate CI pipeline.

**kube-rs specifics:**
- The `kube` crate (version 0.90+) provides a controller-runtime analog with
  `Controller::new` + `reconcile` function pattern.
- CRD schemas are generated via `schemars` derive macros on the CR struct.
- The operator would live at `crates/weir-operator/` and share `weir-core` types but
  not `weir-server` internals.
- Prometheus metrics scraping from the pod requires either an HTTP client in the
  operator (hitting `pod-ip:9185/metrics`) or a ServiceMonitor + Prometheus query.

**Advantages over Helm:**
- The shard-decrease safety gate is enforceable (reconciler returns `Err` and surfaces
  a condition rather than silently applying the change).
- Cert-rotation SIGHUP can be automated (watch Secret + pod/exec).
- Status subresource gives operators a single source of truth.
- Upgrades can be drain-validated.

**Disadvantages:**
- Operator bugs can leave CRs stuck in a bad state indefinitely.
- RBAC footprint: needs `get/list/watch/update` on StatefulSets, ConfigMaps, Secrets,
  Pods, Services, and `create` on Events — a significant privilege set for a daemon
  that otherwise runs with a minimal footprint.
- Maintenance burden: operator must track Kubernetes API deprecations and kube-rs
  releases independently of weir-server releases.
- The `pods/exec` privilege for SIGHUP delivery is a security concern.

### A middle path: Helm + pre-upgrade hook

A Helm pre-upgrade hook job can:
1. Query the weir pod's `/metrics` to check drain backlog before `helm upgrade` proceeds.
2. Block the upgrade if `wab_bytes_on_disk > 0` or `drain_segments_confirmed < sealed`.
3. Document the `terminationGracePeriodSeconds` requirement in values.yaml comments.

This covers the two highest-risk failure modes (shard decrease without drain, upgrade
without drain) at roughly 2 days of effort. It is the recommended middle ground.

---

## 5. Recommendation

**Phase 4: ship a Helm chart with a pre-upgrade drain-check hook.**

1. Helm chart at `deploy/helm/weir/` — covers installation, config templating, PVC,
   Service, ServiceMonitor, RBAC for the daemon pod. Estimated effort: 2–3 days.
2. Pre-upgrade hook job that queries `/metrics` and fails the upgrade if WAB is not
   fully drained. Estimated effort: 1 day.
3. Document the shard-decrease procedure in `docs/operations/shard-resize.md` — manual
   steps to drain and then decrease, with the exact metric names to watch.
4. Add `weir-server --check-segment-format` (or `--version` that includes format
   version) as a CLI flag to support future automated compat checks.

**Do not build the kube-rs operator in Phase 4.** Revisit if any of these become true:
- Users report cert-rotation SIGHUP as a pain point (today SIGHUP requires manual
  `kubectl exec`, which is rare enough to be tolerable).
- Users deploy 5+ instances and ask for a GitOps-native declarative API.
- The shard-decrease protection in the Helm hook is insufficient (operator accidentally
  bypasses the hook via `--no-hooks`).

---

## 6. Open Questions

1. **PVC naming and reuse.** The Helm chart's StatefulSet `volumeClaimTemplates` names
   the PVC after the pod. If a user renames the Helm release, the PVC is orphaned.
   Should the chart support an `existingPvcName` value to decouple lifecycle?

2. **Shard-count decrease path.** Today `replay_unconfirmed` in `wab/mod.rs` only scans
   `0..shard_count` directories. Should weir itself be changed to scan *all*
   `shard_*/` directories regardless of config (recovering orphaned shards automatically
   after a decrease)? This would make the operator safety gate unnecessary but would
   change the daemon's recovery semantics. Worth a separate ADR.

3. **Format version exposure.** The `FORMAT_VERSION` constant in `wab/format.rs` is not
   exposed externally. An `--inspect-format` CLI flag or a startup log field would let
   operators (and an operator hook) detect a format mismatch before a bad upgrade is
   applied.

4. **Multi-instance fan-out.** If weir grows to support active-active sharding (multiple
   pods sharing a PVC or a distributed WAB), the `WeirInstance` CRD design above is
   insufficient. A `WeirCluster` resource with `replicas > 1` would be needed — but
   weir has no inter-instance coordination today, so this is purely speculative.

5. **`pods/exec` RBAC trade-off.** The cert-rotation SIGHUP via operator exec is the
   most operationally clean approach but requires a broad RBAC privilege. Is the inotify
   sidecar approach (weir-managed cert-watcher container) acceptable, or does it add
   more complexity than it saves? The sidecar avoids `pods/exec` at the cost of an
   extra container per pod.

6. **Dead-letter visibility.** The status subresource sketch above surfaces
   `drainBacklogSegments` but not the dead-letter directory size. Dead-letter growth
   (permanent sink failures) is an equally important operational signal — should the
   status include `deadLetterBytes` from `weir_dead_letter_bytes_total`?
