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
CRC mismatch and moved it (or, for mid-file corruption, a copy of it) to
`<wab_dir>/quarantine/`. A data-integrity event. CRC mismatches usually mean
storage corruption (failing disk / bad RAM).
**Respond:** `weir-ctl quarantine list` shows every quarantined segment;
`weir-ctl quarantine inspect <segment>` reports how much of it is actually
recoverable, and `weir-ctl quarantine requeue` re-delivers it. Full procedure —
including why a "clean end" is not "everything was recovered," and why
`requeue` re-sends the already-delivered prefix — in
[`docs/operations/quarantine-recovery.md`](operations/quarantine-recovery.md).
For **mid-file** corruption specifically: the records up to the first bad one
were already recovered and delivered normally; only the corrupt record and
whatever follows it in the file went to quarantine, and quarantine's `inspect`/
`requeue` can usually recover the good records that sit *after* the
corruption too.

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

#### WeirDrainStopped
The drain supervisor has exited and will not restart itself: the respawn budget
ran out after repeated panics, the dead-letter writer failed to open at start, or
the daemon is shutting down. This is terminal. Records are still accepted and
still acked durably — no ack becomes false — but nothing is delivered and the WAB
grows until the disk fills.

**`weir_sink_health` is not trustworthy while this fires.** That gauge is written
only by the drain thread, so once the drain is gone it is frozen at its last
reading; a green sink-health panel beside a `stopped` drain means nothing is
probing the sink, not that the sink is fine.

**Respond:** restart the daemon. First check the logs for the cause —
`weir_drain_panics_total` climbing to `max_respawns` points at a panicking sink
implementation, and a dead-letter open failure names its path. Set
`wab_max_bytes` if it is not already set, so the next occurrence bounds the WAB
instead of filling the disk. On a deliberate shutdown this resolves when the
instance stops being scraped.

#### WeirSinkDown
The configured sink reports itself unhealthy. The WAB keeps buffering durably (no
data loss), but delivery is stalled.
**Respond:** check the downstream system (the HTTP/SQL endpoint). weir retries
transient errors with backoff; sustained `down` means the sink needs attention.
Records stay safe on disk until it recovers.
**Detection lag — alert on more than this one signal.** `weir_sink_health{state}`
is driven *only* by the periodic HEAD health probe (`probe_health`), not by commit
failures, and that probe runs on the `health_poll_interval_secs` cadence *from the
`Draining` state*. During an active outage the drain spends its time in
`RetryingTransient` (backing off between commit retries), where the probe does not
run — so `sink_health{down}` can lag the actual outage by a full poll interval or
more, while `weir_drain_state{state="retrying_transient"}` flips on the *first*
failed commit. For prompt detection, alert on the faster signals **in addition to**
`WeirSinkDown`: `weir_drain_state{state="retrying_transient"} == 1` (immediate on
the first transient failure) and `increase(weir_drain_segments_stranded_total[15m]) > 0`
(`WeirSegmentStranded`, fires once retries are exhausted) — see those entries.

#### WeirSinkDegraded
The sink reports itself **degraded** — reachable but not fully healthy (e.g. the
HEAD health probe returned a 4xx other than 401/403/405; 404 is not special and
falls here too). 401/403/405/501 are treated as Healthy, since they mean the
endpoint is reachable but just rejects the unauthenticated HEAD probe (the real
authenticated POST path still works). Delivery may still work but is impaired;
the WAB keeps buffering durably. Warning, not critical.
**Respond:** check the downstream system before it tips to `down`; correlate with
`WeirSegmentStranded` / `WeirDeadLettered`. Transient flaps below 15m don't fire.

#### WeirSegmentStranded
The drain exhausted `sink_max_retries` (default 3) of **transient** sink failures
on a segment and left it on disk ("stranded"). The data is durable (no loss), but
delivery for that segment is paused.
**Respond:** fix the sink (correlate with `WeirSinkDown` / `weir_sink_health`).
Once the sink **recovers**, the drain **automatically re-drains** stranded
segments on the next health poll — watch `weir_drain_segments_resumed_total`
converge toward `weir_drain_segments_stranded_total` and the alert clear. A daemon
restart also replays them. **Note: `stranded − resumed` is not a strict count of
currently-stuck segments** — both are event counters, and a segment that strands,
resumes, then re-strands (e.g. a POST-only outage where the HEAD probe stays
healthy) bumps both, so the difference can over- or under-state the live backlog.
For the actual stuck files, list them: `weir-ctl segments` shows the
sealed-but-undelivered files. If the gap persists, the sink isn't healthy yet (or is
recovering then re-failing). Raise `sink_max_retries` / `sink_retry_base_delay_ms` to ride out longer
outages before stranding. Distinct from `WeirDeadLettered` (*permanent* rejections).

#### WeirDeadLettered
The sink **permanently** rejected records (e.g. a 4xx). They are written to the
dead-letter directory for manual handling — not retried.
**Respond:** inspect the dead-letter files and the sink's rejection reason
(a schema mismatch, auth failure, malformed payload). Fix the upstream cause,
then reprocess with `weir-ctl dl requeue` — it re-submits the dead-lettered
records back through the daemon's socket and deletes each segment once all its
records are re-accepted. Re-delivery is at-least-once (a record may be delivered
more than once if a requeue run is interrupted; the HTTP sink's idempotency key
dedupes identical payloads). `weir-ctl dl` also offers `list` (inspect) and
`drop` (discard) for when the records aren't worth reprocessing.

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

## Capacity: surviving a sink outage

weir's pitch is "buffer durably while the uplink is down." That's bounded by
**disk**: while the sink is unreachable, every accepted record stays in the WAB
(and, for permanent rejections, the dead-letter dir), so the buffer grows at the
ingest rate until storage fills. Size it deliberately:

> **time-to-WAB-full ≈ free_disk_bytes / (ingest_rate × avg_record_bytes)**

For example, 50 GiB free and 5 MiB/s of accepted data ≈ **~2.9 hours** of outage
headroom before the disk fills. Once the WAB partition is full, fsync fails and
producers are Nacked (no false acks — they retry/backpressure), so you lose
*availability*, not durability.

Wire these so you see it coming, in order of urgency:

1. **WAB disk %** — a node/host alert on the filesystem holding `wab_dir` (weir
   doesn't measure free disk itself); page well before 100%. Cross-check
   `weir_wab_bytes_on_disk` for weir's own contribution.
2. **Dead-letter vs cap** — `weir_dead_letter_bytes_on_disk` approaching
   `dead_letter_max_bytes`; at the cap the drain blocks (see below).
3. **Drain blocked** — `weir_drain_state{state="blocked_dead_letter_full"} == 1`
   (`WeirDrainBlocked`) means all delivery is paused.
4. **Sink down / segments stranded** — `WeirSinkDown` + `WeirSegmentStranded`
   are the leading indicators that the buffer has started growing at all.

Mitigations: raise `dead_letter_max_bytes` only if the disk can take it; add disk;
lower the ingest rate (producer backpressure); or fix the sink. Stranded segments
re-drain automatically on recovery (watch `weir_drain_segments_resumed_total`).

---

## Metric reference

All metrics are prefixed `weir_`. Counters carry a `_total` suffix in the
exposition; histograms expose `_bucket` / `_sum` / `_count`.

### Ingest
| Metric | Type | Meaning |
|---|---|---|
| `weir_records_accepted_total{tier}` | counter | Records admitted for processing, by durability tier (`sync`/`batched`/`buffered`). |
| `weir_records_ack_total{tier}` | counter | Records durably acked to the producer, by durability tier (`sync`/`batched`/`buffered`). The gap `accepted − ack` is in-flight or failed (Nacked) work; it is only ~0, and the amortization ratio only meaningful, while `weir_records_nack_total` stays flat. |
| `weir_records_nack_total{tier,reason}` | counter | Records Nacked (not durably accepted), by durability tier and `reason`. Should be ~0. `reason` is one of `bad_magic`, `version_mismatch`, `bad_header_crc`, `payload_too_large`, `bad_payload_crc`, `internal_error`, `empty_payload`, `unknown_message`, `reserved_flags_set` (the wire `NackReason` variant names, lowercased). `internal_error` is the only *transient* reason (queue saturation / ack timeout / write-fsync error / the [`wab_max_bytes`](operations/configuration.md#wab_max_bytes) cap rejecting — connection stays open, producer retries); the rest are permanent protocol/payload errors that close the connection. The cap is the one cause with its own counter — `weir_wab_cap_rejections_total` — so subtracting it from `internal_error` separates "the WAB is full" from the other three. |
| `weir_accept_latency_seconds` | histogram | Socket-accept → enqueue latency (independent of fsync). |
| `weir_queue_depth` | gauge | In-flight records between the socket layer and workers. |

> **These counters are per-process and reset to 0 on every restart** (they are
> in-memory atomics, not persisted). After a crash/restart, `accepted − ack` is
> *not* a lifetime loss figure: records that were durably written before the
> restart but not yet drained are **replayed from the WAB on startup and counted
> under `weir_recovery_records_replayed_total`** (Durability section), not
> re-counted under `accepted`/`ack`. So a fresh process legitimately shows
> `accepted ≈ ack` near zero while the recovery counter carries the
> pre-restart backlog. Use `rate(...)`/`increase(...)` over a window (which
> tolerate counter resets) rather than raw cumulative differences across a
> restart boundary.

### Durability (WAB)
| Metric | Type | Meaning |
|---|---|---|
| `weir_wab_fsync_duration_seconds` | histogram | **The dominant latency signal.** Per-fsync duration. |
| `weir_wab_fsync_failures_total` | counter | fsync returned an error. **Must be 0.** |
| `weir_wab_flusher_panics_total` | counter | Flusher thread panics. **Must be 0.** |
| `weir_drain_panics_total` | counter | weir-drain thread panics caught and respawned by its supervisor. **Must be 0**; sustained values indicate a sink/drain logic bug, and exhausting the respawn budget stops delivery. |
| `weir_wab_segments_total{state}` | counter | Segment lifecycle transitions: `open` → `sealed` → `confirmed`; `quarantined` on corruption. A segment is counted `sealed` whichever way it was sealed — rotation, idle-seal, shutdown-seal, **or crash recovery** (recovery finalises an unsealed `.wab` and renames it, which is a real transition and is counted). A mid-file-corrupt segment counts **both** `quarantined` (the preserved forensic copy) and `sealed` (the truncated valid prefix, which is still delivered). **Backlog caveat across a restart** — see the note below. |
| `weir_wab_bytes_on_disk` | gauge | Bytes used by **live** WAB shard segments: the open active segment (`.wab`) **plus** sealed segments awaiting drain (`.wab.sealed`). It scans on a 5 s cadence, so it trails real-time by up to that interval. It is **not** a total-disk-usage gauge: it excludes drained-marker files (`.wab.confirmed`) and the `dead_letter/` (own gauge: `weir_dead_letter_bytes_on_disk`) and `quarantine/` subdirs, and it doesn't measure the filesystem's free space — for the "disk filling up" signal use a node/host filesystem alert on the partition holding `wab_dir` (see *Capacity*). |
| `weir_wab_record_logical_bytes_total` | counter | Cumulative record payload bytes **as accepted from producers**, before any compression. Equal to `weir_wab_record_stored_bytes_total` when `wab_compression = "none"`. Counts payload bytes only — not the 8-byte per-record framing, the segment header, or the footer — so it is not a file-size measure. |
| `weir_wab_record_stored_bytes_total` | counter | Cumulative record payload bytes **as written to disk**, after compression. Divide the logical counter by this one for the live compression ratio (see *Compression ratio* below). Bumped only after a successful write, so a rejected record moves neither counter. |
| `weir_wab_unexpected_mode_total` | counter | Segment file found with unexpected permissions (tampering guard). |
| `weir_wab_cap_rejections_total` | counter | Pushes Nacked because live WAB bytes exceeded [`wab_max_bytes`](operations/configuration.md#wab_max_bytes). These surface to clients as `NackReason::InternalError` — **the same byte queue saturation uses** — so this counter is the **only** way to distinguish a cap rejection from queue-saturation `InternalError` Nacks. Zero when `wab_max_bytes` is unset (the default). |
| `weir_recovery_records_replayed_total` | counter | Records replayed from sealed-but-unconfirmed segments on startup. |
| `weir_recovery_segments_quarantined_total` | counter | Corrupt segments quarantined during **active**-segment recovery (bad header, or mid-file corruption with a copied tail). **Does not cover every way a file ends up in `quarantine/`** — a `.confirmed` sidecar that fails verification during sealed-segment replay quarantines both it and its segment without touching this counter (only a log line and the bytes gauge move). Use `weir_quarantine_bytes_on_disk` if you need a signal that catches every case. Recover with `weir-ctl quarantine requeue` — see [the runbook](operations/quarantine-recovery.md). |
| `weir_recovery_segments_failed_total` | counter | Segments whose active-segment recovery (`recover_segment`) failed **and left the segment at its original path**, so nothing will ever reach it. **Deliberately excludes quarantined segments** — a header-level quarantine (bad magic / unknown version / too-short header) moves the file and is counted by `weir_recovery_segments_quarantined_total` instead, because it is handled, not abandoned. (Such a segment is listed by `weir-ctl quarantine list`, though `inspect` cannot parse it — the header is what's corrupt.) **Does NOT fire for the mid-file-corruption case** this toolchain exists for: recovery returns `Ok` there, having sealed and delivered the valid prefix. Non-zero therefore means a genuine I/O failure left a segment stranded — check the startup logs for the path and cause. |
| `weir_quarantine_bytes_on_disk` | gauge | Total bytes under `<wab_dir>/quarantine/`. **The one signal that moves on every quarantine event**, including the `.confirmed`-sidecar case neither counter above catches. Survives a restart (unlike the counters), refreshed on the same cadence as `weir_dead_letter_bytes_on_disk`. Recover with `weir-ctl quarantine requeue`. |
| `weir_recovery_quarantine_copy_failed_total` | counter | Mid-file-corrupt segments whose quarantine copy failed (disk full / read-only / inode exhaustion); recovery left the segment un-truncated to preserve acked-durable tail records and will retry. **Non-zero means recovery is stuck on that segment — clear the disk/read-only state and restart.** |
| `weir_ack_timeout_total` | counter | Acks that never fired within the timeout (wedged flusher). |
| `weir_stage_{queue,bridge_wait,write,total}_seconds` | histogram | Per-stage latency decomposition (`bench-trace` builds only — diagnostic). |

> **Using `weir_wab_segments_total` for drain backlog.** Within a single process
> run the family conserves: every segment counted `sealed` is later counted
> `confirmed` (or `quarantined`), so a growing `sealed − confirmed` really does
> mean the drain is falling behind. **It does not survive a restart.** Like every
> other counter here it is a per-process in-memory atomic, and segments that were
> sealed by the *previous* process and replayed on startup are counted
> `confirmed` by this one with no matching `sealed` — a replay is not a
> transition, so it is deliberately not re-counted (`weir_recovery_records_replayed_total`
> is the signal for that backlog). So after a restart the raw difference can go
> negative and stay negative. For the live backlog use
> `weir_wab_bytes_on_disk`, or list the files with `weir-ctl segments`; use
> `rate(...)`/`increase(...)` over a window for the trend.

### Drain / sink
| Metric | Type | Meaning |
|---|---|---|
| `weir_drain_state{state}` | gauge | One of `draining` / `retrying_transient` / `blocked_dead_letter_full` / `stopped` is 1. `stopped` is **terminal**: the drain supervisor has exited (clean shutdown, the respawn budget exhausted, or the dead-letter writer failing to open at start) and nothing will be delivered until the daemon restarts — the WAB grows until then. Alert on `weir_drain_state{state="stopped"} == 1` on a daemon you expect to be running. |
| `weir_sink_commit_records_total{outcome}` | counter | `committed` / `retried` / `dead_lettered` per record. |
| `weir_sink_commit_duration_seconds` | histogram | Sink `commit()` call duration. |
| `weir_sink_health{state}` | gauge | Current sink health: exactly one of `state=healthy` / `degraded` / `down` is 1. Alert on `weir_sink_health{state="down"} == 1` (`WeirSinkDown`); `degraded` is an early warning (`WeirSinkDegraded`). **This gauge is driven solely by the periodic HEAD health probe, not by commit failures** — see the lag note below. **It is not a delivery-success signal.** A sink that answers `HEAD` with 2xx (or with 401/403/405/501, which read as healthy) but then 4xx-es every `POST` reads `healthy` here while **dead-lettering all traffic** — the only metric that rises is `weir_sink_commit_records_total{outcome="dead_lettered"}`. **Alert on the dead-lettered outcome rate** (`rate(weir_sink_commit_records_total{outcome="dead_lettered"}[5m]) > 0`, the shipped `WeirDeadLettered` rule) in addition to `weir_sink_health` — health alone will not catch a HEAD-healthy / POST-rejecting endpoint. |
| `weir_sink_info{sink_type}` | gauge | The configured sink type, set to 1 for the active one. `sink_type="noop"` means records are acked then **DISCARDED** (not forwarded) — alert/indicate on `weir_sink_info{sink_type="noop"} == 1` in any non-soak-test deployment. |
| `weir_dead_letter_bytes_on_disk` | gauge | Dead-letter directory size. |
| `weir_dead_letter_full_total` | counter | Count of distinct BlockedDeadLetterFull episodes (each entry into the blocked state). For the current-blocked boolean use `weir_drain_state{state="blocked_dead_letter_full"}`. |
| `weir_dead_letter_blocked_duration_seconds` | gauge | Seconds since the drain entered BlockedDeadLetterFull (resets to 0 on exit); alert when it exceeds your threshold. |
| `weir_drain_segments_stranded_total` | counter | Segments left on disk after exhausting `sink_max_retries` **transient** sink failures. The data is durable; delivery is paused. **Alert on `increase(weir_drain_segments_stranded_total[15m]) > 0`** and correlate with `weir_sink_health{state="down"}`. They are re-drained automatically when the sink recovers (see below) or on restart. This is an **event counter, not a live gauge of distinct stuck segments**: the same segment can strand, auto-resume on the next healthy probe, and re-strand — each strand increments it — so it can exceed the number of distinct stuck segments. That happens notably in a **POST-only outage**: deliveries (`commit`) keep failing while the HEAD health probe stays healthy, so each recovery edge re-queues the segment, it re-fails, and it strands again. Distinct from `weir_dead_letter_full_total` (permanent rejections). |
| `weir_drain_segments_resumed_total` | counter | Stranded segments re-queued for delivery after a sink health **recovery** (down→up). Convergence with `weir_drain_segments_stranded_total` means an outage's backlog has been picked back up; a persistent gap means segments are still stranded (sink not healthy yet). |

### System / security
| Metric | Type | Meaning |
|---|---|---|
| `weir_connection_rejected_peer_uid_total` | counter | Connections rejected by the `SO_PEERCRED` UID check. |
| `weir_accept_resource_exhaustion_total` | counter | `accept(2)` failures from resource exhaustion (EMFILE/ENFILE/ENOBUFS/ENOMEM); the accept loop backs off on each. A rising value means the daemon is near an fd/memory limit. |
| `weir_connection_idle_timeout_total` | counter | Connections dropped for read-idle (slowloris guard). |
| `weir_connections_aborted_at_shutdown_total` | counter | Connections force-closed at shutdown after the grace period. |
| `weir_tls_handshake_failures_total` | counter | (tls) Mutual-TLS handshake failures. |
| `weir_tls_config_reloads_total{outcome}` | counter | (tls) SIGHUP TLS cert/key/CA reload attempts, by `outcome` (`ok` / `failed`). **Alert on `outcome="failed"`** — a failed reload means the daemon keeps serving the old certificate. |

### Compression ratio

When `wab_compression = "zstd"` is enabled, the live ratio is:

```promql
rate(weir_wab_record_logical_bytes_total[5m])
  / rate(weir_wab_record_stored_bytes_total[5m])
```

A value of 1.0 means compression is achieving nothing on your payloads; **below
1.0 means it is actively expanding them**, which happens when records are small
enough that zstd's frame overhead exceeds the savings. Each record is compressed
independently, so the ratio reflects redundancy *within a single record*, not
across the stream — see
[`wab_compression`](operations/configuration.md#wab_compression) for measured
figures by payload size and the rule of thumb for when to leave it off.

This is the number to watch when sizing `wab_dir` for a sink outage: it is the
multiplier on how long an outage the same disk can absorb.

