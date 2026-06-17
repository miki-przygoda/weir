# Monitoring weir

weir exposes Prometheus metrics at `/metrics`. This page is the operator's
companion: how to wire up monitoring, what each alert means and how to respond,
and a reference for every metric.

**The one thing to internalise:** weir is **fsync-bound**. `weir_wab_fsync_duration_seconds`
is the dominant latency signal in any durable-write deployment — Sync latency is
≈100% fsync (measured: NVMe ~150 µs, SATA SSD ~1.4 ms; see
`docs/benchmarks/snapshot-2026-06-13-comparison.md`). Durable throughput is set
by your disk's fsync latency, full stop. The dashboard and alerts are organised
around making that obvious and actionable.

**A reassurance baked into the failure modes:** weir's durability guarantees are
deterministically fault-tested (the DST harness — `wab::dst`). A failed fsync, a
torn write, a crash between sync and rename, or a panic mid-flush all produce a
**Nack, never a false ack** (verified by the `i1_acked_true_is_durable`
invariant). So when a durability alert fires, the system already did the safe
thing — your job is to fix the underlying disk/sink, not to chase lost data.

## Setup

The artifacts live under `deploy/`:

- `deploy/grafana/weir-dashboard.json` — the overview Grafana dashboard (an
  at-a-glance health strip, then Ingest / Durability / Drain / System rows).
- `deploy/grafana/dashboards/` — per-instance dashboards (`weir-min/med/high/max/
  chaos`) generated DRY by `deploy/grafana/gen-dashboards.py`; the demo's
  `levels`/`chaos` profiles populate them.
- `deploy/prometheus/weir-alerts.yml` — the Prometheus alert rules.
- `deploy/monitoring/` — a turnkey `docker compose` stack (Prometheus + Grafana +
  weir + a load generator) to see it all live. See `deploy/monitoring/README.md`.

**Existing Prometheus + Grafana:** add a scrape target for weir's `/metrics`
(default port 9185), load the alert rules via `rule_files`, and import the
dashboard JSON (pick your Prometheus datasource on import). **Turnkey demo:**
`cd deploy/monitoring && docker compose up --build`, then open Grafana at
`http://localhost:3000`.

**See an alert actually fire.** The demo's opt-in `chaos` profile
(`docker compose --profile chaos up`) deliberately drives a permanently-failing
sink, a full dead-letter dir, and peer-UID rejections — so you can watch
`WeirDeadLettered`, `WeirDrainBlocked`, `WeirSinkDown`, and
`WeirUnauthorizedConnections` go red and rehearse the runbook below against a
live signal before you need it in anger.

Alert selectors assume a `job="weir"` scrape job — adjust `job=~"weir"` to match
your scrape config.

---

## Alerts & runbook

Severity convention: **critical** = page-worthy (durability or total-stall);
**warning** = investigate during business hours. Thresholds marked `[TUNE]` in
the rules depend on your storage + workload — calibrate to the measured
baselines in the benchmark snapshots.

### Durability (page-worthy)

#### WeirInstanceDown
Prometheus can't scrape weir's `/metrics` (`up == 0`). The daemon is unreachable
or dead; producers are likely failing to connect.
**Respond:** check the process (`systemctl status weir` / container state), the
socket, and recent logs. If it crashed, the WAB replays sealed-but-unconfirmed
segments on restart — no acked data is lost.

#### WeirFsyncFailure
`fdatasync` returned an error — a record could **not** be guaranteed on stable
storage. **The producer was Nacked (no false ack — DST-verified)**, so nothing
was silently lost, but durability is compromised until resolved.
**Respond:** this is a disk/filesystem problem, not a weir bug. Check `dmesg` for
I/O errors, the disk's SMART health, and free space. On network-attached storage
(NFS/EFS) `fdatasync` can fail under contention — local SSD is strongly
recommended (it's also 5–50× faster). Producers retry under at-least-once.

#### WeirFlusherPanic
A WAB flusher thread panicked. The supervisor respawns it up to 10× (linear
backoff) and then takes that shard **permanently offline** (Nack-everything until
restart). A panic mid-flush does **not** produce a false ack (DST
`panic_at_fsync_barrier` scenario).
**Respond:** grab the panic payload from the logs (`grep "flusher panicked"`).
Real flusher panics are logical bugs — capture the backtrace and restart the
daemon to clear the offline shard. Repeated panics on the same shard = file a bug.

#### WeirAckTimeout
A record's ack never fired within the timeout, with no panic — the wedged-flusher
signature (e.g. a stuck `fsync` on a hung disk).
**Respond:** correlate with `WeirFsyncLatencyHigh` and `weir_queue_depth`. If
fsync latency is spiking, it's storage. If a `WeirFlusherPanic` follows shortly,
treat it as a flusher panic. A hung daemon may need a restart.

#### WeirSegmentQuarantined
Crash recovery found a segment with bad magic, an unknown format version, or a
CRC mismatch and moved it to `<wab_dir>/quarantine/`. A data-integrity event.
**Respond:** inspect the quarantined file (`weir-ctl segments` shows shard
state). CRC mismatches usually mean storage corruption (failing disk / bad RAM).
The records up to the first bad one were already recovered; the quarantined tail
is preserved for manual inspection, not auto-discarded.

#### WeirFsyncLatency
(`WeirFsyncLatencyHigh` warning / `WeirFsyncLatencyCritical` critical.) Sustained
fsync p99.9 above your storage baseline. Because weir is fsync-bound, this
directly caps durable throughput.
**Respond:** identify storage contention (a noisy neighbour, a backup job, a
failing disk), or confirm you're not on network-attached storage. Calibrate the
thresholds: measured p99.9 is ~2.4 ms on NVMe and ~6 ms on a SATA SSD — set
`High` to ~3–4× your medium's baseline and `Critical` an order of magnitude up.
If latency is *normal for your medium*, raise the threshold rather than chase it.

### Drain / dead-letter

#### WeirDrainBlocked
The dead-letter directory hit its cap; **all** drain activity is paused and the
WAB accumulates on disk. High severity — durable buffering continues, but nothing
is being delivered.
**Respond:** free dead-letter space (`weir-ctl dl list` / `weir-ctl dl drop`) or
raise `dead_letter_max_bytes`. The drain re-checks the cap every
`dead_letter_check_interval_secs` and resumes automatically once there's headroom.

#### WeirSinkDown
The configured sink reports itself unhealthy. The WAB keeps buffering durably (no
data loss), but delivery is stalled.
**Respond:** check the downstream system (the HTTP/SQL endpoint). weir retries
transient errors with backoff; sustained `down` means the sink needs attention.
Records stay safe on disk until it recovers.

#### WeirDeadLettered
The sink **permanently** rejected records (e.g. a 4xx). They are written to the
dead-letter directory for manual handling — not retried.
**Respond:** inspect the dead-letter files and the sink's rejection reason
(a schema mismatch, auth failure, malformed payload). Fix the upstream cause;
`weir-ctl dl` inspects/clears the directory. `dl replay` is a Phase-5 addition.

### Ingest / backpressure

#### WeirHighNackRate
Producers are being told records were not accepted durably. Causes: a shard taken
offline by repeated flusher panics, fsync failures, or sustained backpressure.
**Respond:** correlate with the durability alerts. At-least-once means producers
should retry, but a *sustained* Nack rate means a shard or the storage is
unhealthy — chase the root cause above.

#### WeirQueueSaturation
The work queue is backing up — the WAB flushers can't keep pace with ingest
(usually storage pressure). Producers see backpressure (slower acks).
**Respond:** correlate with fsync latency. If fsync is the bottleneck, you're at
the storage durability ceiling — faster disk, more shards (one flusher/disk), or
shift latency-tolerant traffic to the `Batched`/`Buffered` tiers. `[TUNE]` the
threshold to your `batch_size × shard_count` and burst profile.

### Security / connection

#### WeirUnauthorizedConnections
A process whose UID isn't allowed tried to connect over the Unix socket and was
rejected by the `SO_PEERCRED` check.
**Respond:** either a misconfigured producer (wrong user) or an unauthorized
client. Run producers as the weir user, or review who has access to the socket
directory.

#### WeirTlsHandshakeFailures
(`tls` feature) Repeated mutual-TLS handshake failures — client-cert
expiry/rotation, a CA mismatch, or a probe hitting the TCP port.
**Respond:** check client certificate validity and the CA bundle. A burst right
after a cert rotation suggests the new certs aren't trusted;
`weir_tls_config_reloads_total{outcome="ok"}` confirms whether a SIGHUP
reload landed (and `outcome="failed"` that it was rejected).

---

## Metric reference

All metrics are prefixed `weir_`. Counters carry a `_total` suffix in the
exposition; histograms expose `_bucket` / `_sum` / `_count`.

### Ingest
| Metric | Type | Meaning |
|---|---|---|
| `weir_records_accepted_total{tier}` | counter | Records accepted, by durability tier (`sync`/`batched`/`buffered`). |
| `weir_records_ack_total` | counter | Records acked durable to the producer. |
| `weir_records_nack_total` | counter | Records Nacked (not durably accepted). Should be ~0. |
| `weir_accept_latency_seconds` | histogram | Socket-accept → enqueue latency (independent of fsync). |
| `weir_queue_depth` | gauge | In-flight records between the socket layer and workers. |

### Durability (WAB)
| Metric | Type | Meaning |
|---|---|---|
| `weir_wab_fsync_duration_seconds` | histogram | **The dominant latency signal.** Per-fsync duration. |
| `weir_wab_fsync_failures_total` | counter | fsync returned an error. **Must be 0.** |
| `weir_wab_flusher_panics_total` | counter | Flusher thread panics. **Must be 0.** |
| `weir_drain_panics_total` | counter | weir-drain thread panics caught and respawned by its supervisor. **Must be 0**; sustained values indicate a sink/drain logic bug, and exhausting the respawn budget stops delivery. |
| `weir_wab_segments_total{state}` | counter | Lifecycle: `open` → `sealed` → `confirmed`; `quarantined` on corruption. |
| `weir_wab_bytes_on_disk` | gauge | Un-drained segment bytes. |
| `weir_wab_unexpected_mode_total` | counter | Segment file found with unexpected permissions (tampering guard). |
| `weir_recovery_records_replayed_total` | counter | Records replayed from sealed-but-unconfirmed segments on startup. |
| `weir_recovery_segments_quarantined_total` | counter | Corrupt segments quarantined during recovery. |
| `weir_ack_timeout_total` | counter | Acks that never fired within the timeout (wedged flusher). |
| `weir_stage_{queue,bridge_wait,write,total}_seconds` | histogram | Per-stage latency decomposition (`bench-trace` builds only — diagnostic). |

### Drain / sink
| Metric | Type | Meaning |
|---|---|---|
| `weir_drain_state{state}` | gauge | One of `draining` / `retrying_transient` / `blocked_dead_letter_full` is 1. |
| `weir_sink_commit_records_total{outcome}` | counter | `committed` / `retried` / `dead_lettered` per record. |
| `weir_sink_commit_duration_seconds` | histogram | Sink `commit()` call duration. |
| `weir_sink_health{state}` | gauge | Current sink health: exactly one of `state=healthy` / `degraded` / `down` is 1. Alert on `weir_sink_health{state="down"} == 1`; `degraded` is an early warning. |
| `weir_dead_letter_bytes_on_disk` | gauge | Dead-letter directory size. |
| `weir_dead_letter_full_total` | counter | Count of distinct BlockedDeadLetterFull episodes (each entry into the blocked state). For the current-blocked boolean use `weir_drain_state{state="blocked_dead_letter_full"}`. |
| `weir_dead_letter_blocked_duration_seconds` | gauge | Seconds since the drain entered BlockedDeadLetterFull (resets to 0 on exit); alert when it exceeds your threshold. |
| `weir_drain_segments_stranded_total` | counter | Segments abandoned after exhausting `max_retries` **transient** sink failures. The segment stays on disk and is only re-attempted on daemon restart, so any increase means delivery has stalled for at least one segment. **Alert on `increase(weir_drain_segments_stranded_total[15m]) > 0`** and correlate with `weir_sink_health{state="down"}`; once the sink recovers, restart the daemon to replay the stranded segment(s). Distinct from `weir_dead_letter_full_total` (permanent rejections). |

### System / security
| Metric | Type | Meaning |
|---|---|---|
| `weir_connection_rejected_peer_uid_total` | counter | Connections rejected by the `SO_PEERCRED` UID check. |
| `weir_accept_resource_exhaustion_total` | counter | `accept(2)` failures from resource exhaustion (EMFILE/ENFILE/ENOBUFS/ENOMEM); the accept loop backs off on each. A rising value means the daemon is near an fd/memory limit. |
| `weir_connection_idle_timeout_total` | counter | Connections dropped for read-idle (slowloris guard). |
| `weir_connections_aborted_at_shutdown_total` | counter | Connections force-closed at shutdown after the grace period. |
| `weir_tls_handshake_failures_total` | counter | (tls) Mutual-TLS handshake failures. |
| `weir_tls_config_reloads_total{outcome}` | counter | (tls) SIGHUP TLS cert/key/CA reload attempts, by `outcome` (`ok` / `failed`). **Alert on `outcome="failed"`** — a failed reload means the daemon keeps serving the old certificate. |
