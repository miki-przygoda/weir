# Phase 4 Observability — Design Exploration

**Scope:** Production-ready observability story for weir 0.9.0 — Grafana dashboard, Prometheus alert rules, and a monitoring runbook.
**Status:** Exploration (design only; no code changes). Implementable as committed artifacts in Phase 4.
**Grounded in:** `crates/weir-server/src/metrics/mod.rs` (all metric names verified against real registrations).

---

## Context

Phase 3 established that **weir is fsync-bound**: `weir_wab_fsync_duration_seconds` is the dominant latency signal in any durable-write deployment. The observability layer needs to make this obvious in a dashboard and actionable in alerts. At the same time, the drain, dead-letter, and connection layers each have their own failure modes that need separate visibility.

All metric names below use the Prometheus text format convention: counters are registered without `_total` and `prometheus-client` appends it automatically. PromQL examples use the `_total` suffix accordingly.

---

## 1. Grafana Dashboard — Panel Inventory

The dashboard is organized into four rows, each covering one concern: ingest throughput, write durability (WAB + fsync), drain health, and system health.

### Row 1 — Ingest

Answers: "Is the daemon accepting records, at what rate, and are any being rejected?"

---

#### Panel 1.1 — Record Throughput

| Attribute | Value |
|-----------|-------|
| Title | Record Throughput (accepted / acked / nacked) |
| Type | Time series — 3 series |
| Y-axis | records/s |

PromQL:
```promql
# Accepted rate, by tier
rate(weir_records_accepted_total{tier="sync"}[$__rate_interval])
rate(weir_records_accepted_total{tier="batched"}[$__rate_interval])
rate(weir_records_accepted_total{tier="buffered"}[$__rate_interval])

# Acked rate, by tier (mirrors accepted during normal operation)
rate(weir_records_ack_total{tier="sync"}[$__rate_interval])
rate(weir_records_ack_total{tier="batched"}[$__rate_interval])
rate(weir_records_ack_total{tier="buffered"}[$__rate_interval])

# Total nack rate across all tiers and reasons
sum(rate(weir_records_nack_total[$__rate_interval]))
```

Usage: healthy gap between accepted and acked should be near zero. A widening gap indicates the WAB or drain is falling behind. Nack rate above zero is an immediate investigation trigger.

---

#### Panel 1.2 — Nack Breakdown

| Attribute | Value |
|-----------|-------|
| Title | Nack Rate by Reason |
| Type | Time series — one series per `reason` label |
| Y-axis | nacks/s |

PromQL:
```promql
sum by (reason) (rate(weir_records_nack_total[$__rate_interval]))
```

Reasons defined in code: `bad_magic`, `version_mismatch`, `bad_header_crc`, `payload_too_large`, `bad_payload_crc`, `internal_error`.

Usage: `internal_error` is the critical one — it indicates a flusher panic or ack timeout, not a producer bug. Protocol-error reasons (`bad_magic`, `bad_header_crc`, `bad_payload_crc`) in bulk indicate a misbehaving or misconfigured producer or a wire-tampering attempt. `payload_too_large` spikes indicate a misconfigured `max_payload_bytes`.

---

#### Panel 1.3 — Accept Latency (p50 / p99 / p99.9)

| Attribute | Value |
|-----------|-------|
| Title | Accept Latency — socket accept → handler spawn |
| Type | Time series |
| Y-axis | seconds (log scale recommended) |

PromQL:
```promql
histogram_quantile(0.50,  sum(rate(weir_accept_latency_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99,  sum(rate(weir_accept_latency_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.999, sum(rate(weir_accept_latency_seconds_bucket[$__rate_interval])) by (le))
```

Usage: normally sub-millisecond. Elevation means the tokio runtime is thread-starved (too many `spawn_blocking` tasks competing, or CPU saturation).

---

#### Panel 1.4 — Queue Depth

| Attribute | Value |
|-----------|-------|
| Title | Work Queue Depth |
| Type | Time series |
| Y-axis | records (QUEUE_CAPACITY = 65 536) |
| Threshold line | 52 429 (80% of capacity) |

PromQL:
```promql
weir_queue_depth
```

Usage: steady-state near zero. Sustained elevation means workers or flushers are not consuming fast enough. Hard-saturated at 65 536 means producers are backpressured. Correlate with `weir_wab_fsync_duration_seconds` — if fsync is slow, queue fills because flushers ack slowly.

---

#### Panel 1.5 — Connection Events

| Attribute | Value |
|-----------|-------|
| Title | Connection Events (idle timeouts / peer rejections / shutdown aborts) |
| Type | Time series |
| Y-axis | events/s |

PromQL:
```promql
rate(weir_connection_idle_timeout_total[$__rate_interval])
rate(weir_connection_rejected_peer_uid_total[$__rate_interval])
rate(weir_connections_aborted_at_shutdown_total[$__rate_interval])
```

Usage: any non-zero value for `connection_rejected_peer_uid` is a security signal (peer UID mismatch — unauthorized producer or misconfigured deployment). `connection_idle_timeout` spikes indicate slowloris activity or buggy clients. `connections_aborted_at_shutdown` non-zero on graceful restarts means `shutdown_timeout_secs` needs increasing.

---

### Row 2 — Durability (WAB + fsync)

Answers: "Is the WAB healthy, how fast are fsyncs, and have any failed?"

This is **the most important row** given the Phase 3 finding that weir is fsync-bound.

---

#### Panel 2.1 — fsync Latency Heatmap

| Attribute | Value |
|-----------|-------|
| Title | WAB fsync Latency — Heatmap |
| Type | Heatmap |
| Y-axis | latency bucket (1 ms … 1 s) |
| Color scale | cool→warm (yellow = frequent, red = slow) |

PromQL (for Grafana heatmap panel — use the histogram bucket series):
```promql
sum(rate(weir_wab_fsync_duration_seconds_bucket[$__rate_interval])) by (le)
```

Usage: on a SATA SSD, expect the heat to concentrate around 1–2 ms. On NVMe, 100–300 µs. The heatmap reveals bimodal distributions (normal batch vs. occasional slow flush), outlier tails, and how adaptive coalescing shifts the distribution under load.

---

#### Panel 2.2 — fsync Latency Percentiles

| Attribute | Value |
|-----------|-------|
| Title | WAB fsync Duration — p50 / p95 / p99 / p99.9 |
| Type | Time series |
| Y-axis | seconds (log scale) |

PromQL:
```promql
histogram_quantile(0.50,  sum(rate(weir_wab_fsync_duration_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.95,  sum(rate(weir_wab_fsync_duration_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99,  sum(rate(weir_wab_fsync_duration_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.999, sum(rate(weir_wab_fsync_duration_seconds_bucket[$__rate_interval])) by (le))
```

Usage: p99.9 is the key SLO signal. On a SATA SSD baseline, 4–5 ms is normal; >20 ms sustained indicates storage pressure. Crosses into alert territory at thresholds defined in Section 2.

---

#### Panel 2.3 — fsync Rate

| Attribute | Value |
|-----------|-------|
| Title | WAB fsync Rate (syncs/s) |
| Type | Time series |
| Y-axis | syncs/s |

PromQL:
```promql
sum(rate(weir_wab_fsync_duration_seconds_count[$__rate_interval]))
```

Usage: correlate with throughput panels. fsync rate / record throughput shows the batch amortization ratio. A drop in fsync rate while record throughput stays constant indicates larger batches (good). A spike in fsync rate with flat throughput indicates batch size collapse (check `weir_queue_depth`).

---

#### Panel 2.4 — WAB Hard Failures (stat panels)

| Attribute | Value |
|-----------|-------|
| Title | WAB fsync Failures (lifetime total) |
| Type | Stat (single value, red if > 0) |

PromQL:
```promql
weir_wab_fsync_failures_total
```

Companion stat panel for flusher panics:
```promql
weir_wab_flusher_panics_total
```

Usage: both should be zero at all times in production. Any non-zero value is a P0 incident trigger. See Section 3 runbook entries for remediation.

---

#### Panel 2.5 — WAB Bytes on Disk

| Attribute | Value |
|-----------|-------|
| Title | WAB Disk Usage |
| Type | Time series |
| Y-axis | bytes |

PromQL:
```promql
weir_wab_bytes_on_disk
```

Usage: steady-state depends on segment rotation rate and drain throughput. A monotonically growing value (without corresponding growth in drain throughput) indicates the drain is not keeping up — check sink health and dead-letter state. Sudden drops are segment confirmations (normal).

---

#### Panel 2.6 — WAB Segment Transitions

| Attribute | Value |
|-----------|-------|
| Title | WAB Segment Transitions |
| Type | Time series |
| Y-axis | segments/s |

PromQL:
```promql
sum by (state) (rate(weir_wab_segments_total[$__rate_interval]))
```

States: `open`, `sealed`, `confirmed`, `quarantined`. Note: `open` transitions are wired for future instrumentation (deferred per code comment). `quarantined` rate non-zero is a data integrity alert trigger.

---

#### Panel 2.7 — Per-Stage Latency (bench-trace builds only)

| Attribute | Value |
|-----------|-------|
| Title | Per-Stage Breakdown — p99 |
| Type | Time series (4 series) |
| Y-axis | seconds (log scale) |
| Note | Only visible when `bench-trace` feature is enabled |

PromQL:
```promql
histogram_quantile(0.99, sum(rate(weir_stage_queue_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99, sum(rate(weir_stage_bridge_wait_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99, sum(rate(weir_stage_write_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99, sum(rate(weir_stage_total_seconds_bucket[$__rate_interval])) by (le))
```

Usage: diagnostic-only panel, hidden or collapsed by default in production dashboards. Reveals if a regression is in the queue coalesce (`stage_queue`), bridge hop (`stage_bridge_wait`), write syscall (`stage_write`), or is purely fsync-bound (`stage_total` >> `stage_write` + `stage_bridge_wait` + `stage_queue` combined). Phase 3 baseline: queue ~2 µs, bridge_wait ~2 µs, write ~5 µs.

---

### Row 3 — Drain

Answers: "Is the drain progressing, what is the sink's health, and is the dead-letter queue growing?"

---

#### Panel 3.1 — Drain State

| Attribute | Value |
|-----------|-------|
| Title | Drain State |
| Type | State timeline or time series with boolean overlays |

PromQL:
```promql
weir_drain_state{state="draining"}
weir_drain_state{state="retrying_transient"}
weir_drain_state{state="blocked_dead_letter_full"}
```

Exactly one of these equals 1 at any time (pre-initialized on startup). Usage: `blocked_dead_letter_full` == 1 is a high-severity alert (all drain activity paused). `retrying_transient` == 1 at the same time as sink_health shows `down` confirms the sink is the bottleneck.

---

#### Panel 3.2 — Sink Health

| Attribute | Value |
|-----------|-------|
| Title | Sink Health State |
| Type | State timeline |

PromQL:
```promql
weir_sink_health{state="healthy"}
weir_sink_health{state="degraded"}
weir_sink_health{state="down"}
```

Usage: the sink health gauge is updated per-segment and on a 30 s wall-clock interval, so it stays fresh during idle periods. `down` maps directly to the drain entering `RetryingTransient` (exponential backoff). `degraded` is an early warning.

---

#### Panel 3.3 — Sink Commit Latency

| Attribute | Value |
|-----------|-------|
| Title | Sink Commit Duration — p50 / p99 |
| Type | Time series |
| Y-axis | seconds |

PromQL:
```promql
histogram_quantile(0.50, sum(rate(weir_sink_commit_duration_seconds_bucket[$__rate_interval])) by (le))
histogram_quantile(0.99, sum(rate(weir_sink_commit_duration_seconds_bucket[$__rate_interval])) by (le))
```

Usage: expected range depends on sink type. HTTP sink: 10–200 ms per record (one POST per record). MySQL/Postgres/ClickHouse sinks: 5–50 ms for a batch commit. Elevation indicates downstream pressure. Note: `weir_sink_commit_records_total / weir_sink_commit_duration_seconds_count` gives the average records-per-commit (the IOPS compression ratio the architecture documentation highlights).

---

#### Panel 3.4 — Sink Commit Record Outcomes

| Attribute | Value |
|-----------|-------|
| Title | Sink Commit Outcomes (committed / retried / dead-lettered) |
| Type | Time series |
| Y-axis | records/s |

PromQL:
```promql
sum by (outcome) (rate(weir_sink_commit_records_total[$__rate_interval]))
```

Outcomes: `committed`, `retried`, `dead_lettered`. Usage: `dead_lettered` rate non-zero means the sink is permanently rejecting records — schema error, auth failure, or misconfigured table. `retried` rate high without `committed` progressing means the sink is in a stuck retry loop.

---

#### Panel 3.5 — Dead-Letter State

| Attribute | Value |
|-----------|-------|
| Title | Dead-Letter Queue |
| Type | Time series — 2 series |
| Y-axis (left) | bytes |
| Y-axis (right) | seconds |

PromQL:
```promql
weir_dead_letter_bytes_on_disk
weir_dead_letter_blocked_duration_seconds
```

Stat panel companion — distinct blocking events:
```promql
weir_dead_letter_full_total
```

Usage: `weir_dead_letter_bytes_on_disk` growing continuously means records are permanently rejected and the drain is writing them to the dead-letter directory. `weir_dead_letter_blocked_duration_seconds` > 0 means all drain activity is paused. The `weir_dead_letter_full_total` counter tracks how many distinct blocking episodes have occurred — useful for change management (each increment is a distinct operator intervention event).

---

#### Panel 3.6 — IOPS Compression Ratio

| Attribute | Value |
|-----------|-------|
| Title | Sink Compression Ratio (records per commit) |
| Type | Time series |
| Y-axis | records/commit |

PromQL:
```promql
sum(rate(weir_sink_commit_records_total{outcome="committed"}[$__rate_interval]))
/
sum(rate(weir_sink_commit_duration_seconds_count[$__rate_interval]))
```

Usage: shows how many records are batched into a single sink commit. The Phase 3 load suite measured 249:1 under herd load. This ratio degrades toward 1:1 at low throughput or when `sink_max_batch_size` is set low.

---

### Row 4 — System Health

Answers: "Are there crash-recovery events, security anomalies, or TLS failures?"

---

#### Panel 4.1 — Recovery Events

| Attribute | Value |
|-----------|-------|
| Title | Recovery Events on Startup |
| Type | Stat panels |

PromQL:
```promql
weir_recovery_records_replayed_total
weir_recovery_segments_quarantined_total
weir_wab_unexpected_mode_total
```

Usage: `recovery_records_replayed_total` > 0 is normal after an unclean shutdown (crash recovery replaying the WAB). `recovery_segments_quarantined_total` > 0 indicates data corruption detected during recovery — requires operator examination of the quarantine directory. `wab_unexpected_mode_total` > 0 suggests tampering or operator error (segment files with permissions != 0o600).

---

#### Panel 4.2 — Ack Timeout Counter

| Attribute | Value |
|-----------|-------|
| Title | Ack Timeouts (wedged flusher signal) |
| Type | Time series or stat |
| Y-axis | timeouts/s |

PromQL:
```promql
rate(weir_ack_timeout_total[$__rate_interval])
```

Usage: `weir_ack_timeout_total` increments when `push_timeout` fires before the WAB flusher returns an ack. This is the pre-panic warning signal: a flusher stuck on a very slow fsync (or lock contention) triggers this before `wab_flusher_panics_total` increments. Any non-zero rate is high-severity.

---

#### Panel 4.3 — TLS Events (TLS-feature builds only)

| Attribute | Value |
|-----------|-------|
| Title | TLS Handshake Failures + Config Reloads |
| Type | Time series |
| Y-axis | events/s |

PromQL:
```promql
sum by (reason) (rate(weir_tls_handshake_failures_total[$__rate_interval]))
sum by (outcome) (rate(weir_tls_config_reloads_total[$__rate_interval]))
```

Failure reasons: `no_client_cert`, `bad_cert`, `timeout`, `other`. Usage: `no_client_cert` or `bad_cert` rate non-zero indicates unauthorized connection attempts or a misconfigured client. `timeout` rate high indicates a TLS slowloris attack or high-latency clients. `tls_config_reloads_total{outcome="failed"}` > 0 means a SIGHUP cert rotation failed — the old cert remains active, but the intended new cert was not loaded.

---

## 2. Prometheus Alert Rules

All alerts target a single weir instance (no `job` label filtering shown — operators should add `job="weir"` selectors to match their scrape config). Thresholds marked `[TUNE]` require operator input based on workload.

### Durability Alerts (Page-worthy)

| Alert Name | Severity | For | PromQL Condition | Meaning |
|------------|----------|-----|-----------------|---------|
| `WeirFsyncFailure` | critical | 0m (immediate) | `weir_wab_fsync_failures_total > 0` | An fdatasync call returned an error — durability hazard. Producers whose records were in the failed batch received Nack(InternalError). Data may be lost on Linux ("fsyncgate"). |
| `WeirFlusherPanic` | critical | 0m (immediate) | `weir_wab_flusher_panics_total > 0` | A WAB flusher thread panicked. Its shard is now offline: all records routed to it receive Nack(InternalError) until the daemon restarts. Self-healing via the 10-attempt panic respawn added in v0.4 (see `wab_flusher_panics` code comment), but any non-zero value requires investigation. |
| `WeirAckTimeout` | critical | 2m | `rate(weir_ack_timeout_total[5m]) > 0` | Pushes are timing out waiting for WAB ack — the flusher is wedged (slow fsync or lock contention) without having panicked yet. Precursor to a flusher panic. |
| `WeirFsyncLatencyHigh` | warning | 10m | `histogram_quantile(0.99, sum(rate(weir_wab_fsync_duration_seconds_bucket[5m])) by (le)) > 0.050` | p99 fsync latency sustained above 50 ms — 35x the SATA SSD baseline. Indicates storage pressure, disk queue saturation, or VM I/O throttling. |
| `WeirFsyncLatencyCritical` | critical | 5m | `histogram_quantile(0.99, sum(rate(weir_wab_fsync_duration_seconds_bucket[5m])) by (le)) > 0.200` | p99 fsync latency above 200 ms — approaching ack timeout territory. Ack timeouts and nacks are imminent. |

### Drain / Dead-Letter Alerts

| Alert Name | Severity | For | PromQL Condition | Meaning |
|------------|----------|-----|-----------------|---------|
| `WeirDrainBlocked` | critical | 1m | `weir_drain_state{state="blocked_dead_letter_full"} == 1` | Drain is fully paused — dead-letter directory is at capacity. No records are draining to the sink. WAB backlog will grow until operator intervenes. |
| `WeirDeadLetterBlockedDuration` | critical | 0m (gauge) | `weir_dead_letter_blocked_duration_seconds > 300` [TUNE] | Drain has been blocked for 5 minutes (default threshold). Tune to your operator response SLA. |
| `WeirDeadLetterGrowing` | warning | 30m | `increase(weir_dead_letter_bytes_on_disk[30m]) > 104857600` | Dead-letter directory grew by >100 MiB in 30 min, indicating sustained permanent rejection. |
| `WeirSinkDown` | critical | 5m | `weir_sink_health{state="down"} == 1` | Sink reports unhealthy (down state). Drain is in RetryingTransient with exponential backoff. Records are not flowing to the downstream. |
| `WeirSinkDegraded` | warning | 10m | `weir_sink_health{state="degraded"} == 1` | Sink reports degraded — partial failures but still committing some records. Investigate before it transitions to down. |
| `WeirDeadLettered` | warning | 5m | `rate(weir_sink_commit_records_total{outcome="dead_lettered"}[5m]) > 0` | Records are being permanently dead-lettered by the sink. Indicates a schema mismatch, auth failure, or misconfigured table that the sink considers permanent. |
| `WeirDrainRetrying` | warning | 15m | `weir_drain_state{state="retrying_transient"} == 1` | Drain has been in transient-retry mode for 15 minutes. If retries don't resolve, drain will exhaust `MAX_RETRIES` (3) and either recover or dead-letter. |

### Ingest / Backpressure Alerts

| Alert Name | Severity | For | PromQL Condition | Meaning |
|------------|----------|-----|-----------------|---------|
| `WeirNackRateHigh` | warning | 5m | `rate(weir_records_nack_total[5m]) > 0` | Any records are being nacked. Filter by reason for root cause. `internal_error` is the critical variant; protocol-error reasons indicate producer issues. |
| `WeirNackInternalError` | critical | 2m | `rate(weir_records_nack_total{reason="internal_error"}[5m]) > 0` | Internal-error nacks — flusher panic or ack timeout. Correlate with `WeirFlusherPanic` and `WeirAckTimeout`. |
| `WeirQueueDepthHigh` | warning | 5m | `weir_queue_depth > 52429` | Work queue above 80% of capacity (65 536). Producers will see latency elevation. If sustained, records may block. |
| `WeirQueueDepthSaturated` | critical | 2m | `weir_queue_depth >= 65536` | Work queue full. Producers are backpressured — new `push_timeout` calls return `InternalError` nacks. |
| `WeirRecordAcceptedAckGap` | warning | 5m | `sum(rate(weir_records_accepted_total[5m])) - sum(rate(weir_records_ack_total[5m])) > 100` [TUNE] | Sustained gap between accepted and acked records indicating the WAB pipeline is not keeping up. |

### Security / Connection Alerts

| Alert Name | Severity | For | PromQL Condition | Meaning |
|------------|----------|-----|-----------------|---------|
| `WeirUnauthorizedConnection` | warning | 0m (immediate) | `rate(weir_connection_rejected_peer_uid_total[5m]) > 0` | Connections rejected due to peer UID mismatch — attempted bypass of socket security or misconfigured producer. |
| `WeirSegmentQuarantined` | warning | 0m (immediate) | `rate(weir_recovery_segments_quarantined_total[5m]) > 0` | WAB segments quarantined during crash recovery — indicates data corruption. Examine quarantine directory. |
| `WeirUnexpectedSegmentMode` | warning | 0m (immediate) | `rate(weir_wab_unexpected_mode_total[5m]) > 0` | WAB segment files found with permissions != 0o600 — tampering or operator error. |
| `WeirTlsHandshakeFailures` | warning | 5m | `rate(weir_tls_handshake_failures_total[5m]) > 0` | TLS handshakes failing — unauthorized connection attempts or misconfigured clients. |
| `WeirTlsReloadFailed` | warning | 0m (immediate) | `rate(weir_tls_config_reloads_total{outcome="failed"}[5m]) > 0` | SIGHUP cert rotation failed — old cert still active, new cert not loaded. |
| `WeirIdleTimeoutSpike` | warning | 10m | `rate(weir_connection_idle_timeout_total[5m]) > 1` | Elevated idle connection timeouts — potential slowloris activity or buggy clients holding connection slots. |

---

## 3. Runbook — Per-Alert Entries

### WeirFsyncFailure — fsync returned an error

**What it means:** The kernel accepted the `write()` but `fdatasync()` returned an error. The kernel could not push buffered data to stable storage. Records in the affected batch received Nack(InternalError) and were not durably written. On Linux, a second fsync after EIO may appear to succeed while the data is actually lost ("fsyncgate" — see lwn.net/Articles/752063).

**Likely causes:**
- Storage device error (physical disk failure, RAID degradation)
- Out of disk space (no space left on device — EIO on ext4)
- VM I/O throttle exceeded (cloud providers may return EIO under extreme throttle)
- NFS/shared storage (unsupported — weir requires local block storage)

**Remediation:**
1. Examine daemon logs immediately for `shard_id` and the OS error string attached to the fsync failure.
2. Run `dmesg | grep -i "I/O error\|EXT4-fs error\|ata"` to check for block-layer errors.
3. Check disk space: `df -h /var/lib/weir`.
4. If disk full: free space or extend the volume, then restart the daemon. Records in the failed batch were already nacked — producers using at-least-once retry will resend them.
5. If physical device error: replace storage. Do not attempt data recovery from a failed-fsync segment — the data integrity is unknown. Treat affected records as potentially lost.
6. After resolving the storage issue, restart the daemon. Recovery will replay any sealed segments that have `.wab.sealed` files (these were not in the failed batch); the failed segment is truncated at the first corrupt record.

**Other metrics to check:** `weir_wab_bytes_on_disk` (sudden stop in growth means flusher stopped writing), `weir_records_nack_total{reason="internal_error"}` (quantifies how many records were nacked).

---

### WeirFlusherPanic — WAB flusher thread panicked

**What it means:** A WAB flusher thread terminated due to a Rust panic. The affected shard is offline. The panic respawn mechanism (capped at 10 attempts, added in v0.4 — `wab_flusher_panics_total` increments per respawn attempt) will attempt to restart the flusher automatically. If all 10 attempts exhaust, the shard remains offline and all records routed to it receive Nack(InternalError) permanently until the daemon is restarted.

**Likely causes:**
- OS-level resource exhaustion (file descriptor limit, OOM) causing an unwrap to panic
- Segment file corruption causing a panic in the write path
- A bug in the flusher code (check release notes for known panics)
- Disk full causing a panic in a write path that doesn't handle `ENOSPC` gracefully

**Remediation:**
1. Check logs for the `shard_id` and panic backtrace.
2. If panic is resource-related (OOM, ENOSPC): resolve the resource constraint, then restart the daemon. The respawn mechanism may have already recovered if the panic was transient.
3. If the respawn counter stopped incrementing and the shard recovered: investigate the root cause from logs to prevent recurrence, but no immediate restart required.
4. If the shard is permanently offline (respawn attempts exhausted or continuing): restart the daemon. Crash recovery will replay sealed segments from the failed shard.
5. Producers affected by the offline shard will have received Nack(InternalError) and should retry if they implement at-least-once semantics.
6. After restart, monitor `weir_wab_flusher_panics_total` — it resets to 0 on startup, so any increment after a fresh restart indicates a new panic.

**Other metrics to check:** `weir_wab_fsync_failures_total` (did a fsync failure cause the panic?), `weir_records_nack_total{reason="internal_error"}` (scope of impact), `weir_queue_depth` (backlog while shard was offline).

---

### WeirAckTimeout — flusher wedged without panicking

**What it means:** `push_timeout` fired before the WAB flusher returned an ack for a record. This is the pre-panic signal: the flusher thread is alive but unresponsive — most commonly because `fdatasync` is taking longer than the `ACK_TIMEOUT` deadline. It can also indicate lock contention in the flusher's internal state.

**Likely causes:**
- Disk I/O spike pushing fsync well beyond the ACK_TIMEOUT (check `weir_wab_fsync_duration_seconds` — if p99.9 is approaching or exceeding the timeout, this is expected under storage pressure)
- Storage throttle (cloud VM disk burst exhausted)
- Lock contention in the bridge/flusher (rare; indicates a scheduling pathology)

**Remediation:**
1. Immediately check `weir_wab_fsync_duration_seconds` p99.9. If fsync latency is the cause, address storage pressure (see `WeirFsyncLatencyCritical`).
2. If fsync latency is normal but ack timeouts are firing, the cause is likely scheduling or locking — check for CPU starvation (high load average, kernel preemption) or memory pressure causing page faults in the flusher.
3. If ack timeouts persist and `WeirFlusherPanic` has not fired: the flusher is alive but struggling. A graceful restart will clear the state without data loss (WAB segments are already on disk).
4. If `WeirFlusherPanic` fires shortly after `WeirAckTimeout`: follow the flusher panic runbook above.

**Other metrics to check:** `weir_wab_fsync_duration_seconds` (the primary correlated signal — Phase 3 finding), `weir_wab_flusher_panics_total`, `weir_queue_depth`.

---

### WeirFsyncLatencyHigh / WeirFsyncLatencyCritical — slow fsyncs

**What it means:** The kernel is taking longer than expected to flush WAB writes to stable storage. This is the single most important operational signal in weir's architecture (Phase 3 finding: fsync is ~89–99% of Sync/Batched tier latency). Elevated fsync latency directly elevates producer ack latency and queue depth.

**Likely causes:**
- Disk I/O queue saturation (competing workloads on the same storage device)
- Cloud VM disk burst budget exhausted (provisioned IOPS temporarily throttled)
- Shard count too high for a single SSD (multiple shards fighting for the same disk queue — see architecture documentation)
- Power-loss protection firmware pathology (some consumer SSDs have non-uniform fsync latency under load)
- Filesystem fragmentation or journal pressure (ext4 with large WAB directories)

**Remediation:**
1. Check `iostat -x 1` for `%util` and `await` on the WAB device. `%util` near 100% or `await` >20 ms confirms I/O saturation.
2. If competing workloads: isolate the WAB directory to a dedicated disk or mount point.
3. If cloud VM: check the cloud provider's disk metrics for throttling events; consider upgrading to provisioned IOPS storage (e.g. AWS io2 vs gp3).
4. If `shard_count` > 1 on a single SSD: reduce to 1 (multiple shards do not increase throughput on a single disk queue — they add overhead). See `shard_count` tuning notes.
5. If latency is within acceptable range for the storage medium (SATA SSD: 1–5 ms is normal): calibrate the alert threshold to your actual baseline.
6. The `Buffered` durability tier bypasses fsync entirely and continues operating at normal latency under storage pressure — consider using it for latency-insensitive records during an incident.

**Other metrics to check:** `weir_records_nack_total{reason="internal_error"}` (are slow fsyncs causing ack timeouts?), `weir_ack_timeout_total`, `weir_queue_depth` (backpressure indicator).

---

### WeirDrainBlocked — dead-letter directory full

**What it means:** The drain has entered `BlockedDeadLetterFull` state. All drain activity is paused — no records are flowing to the sink, the WAB backlog will grow, and eventually producers will see elevated queue depth and ack latency. The daemon wakes every `dead_letter_check_interval_secs` (default: 30 s) to rescan the dead-letter directory; it will unblock automatically if the directory size drops below `dead_letter_max_bytes`.

**Why it blocks entirely:** weir's design choice — the alternative (silently dropping dead-letter records) would lose data that the sink already classified as un-retriable. Blocking is intentional to force operator attention.

**Likely causes:**
- A sink schema change or auth failure causing sustained permanent record rejection, filling the dead-letter directory
- `dead_letter_max_bytes` configured too small for the expected rejection rate
- Operator failed to drain the dead-letter directory after a previous incident

**Remediation:**
1. Immediately examine `weir_sink_commit_records_total{outcome="dead_lettered"}` rate and logs to understand why records are being permanently rejected.
2. Fix the root cause of permanent rejection (schema mismatch, auth failure, table misconfiguration).
3. To unblock the drain without losing dead-letter data: free space by copying dead-letter files (`<wab_dir>/dead_letter/dl_NNNNNNNN.wab.sealed`) to external storage, then deleting the originals. The daemon's periodic rescan will detect the freed space within `dead_letter_check_interval_secs`.
4. To raise the cap temporarily while investigating: update `dead_letter_max_bytes` in config and restart the daemon. This unblocks the drain but allows more permanent data loss to accumulate.
5. After unblocking: process the dead-letter files using `weir`'s WAB reader tooling (planned for Phase 4) or write a custom reader using the WAB format spec at `docs/wab_format.md` to understand what was rejected and replay or discard records.

**Other metrics to check:** `weir_dead_letter_bytes_on_disk`, `weir_dead_letter_blocked_duration_seconds`, `weir_dead_letter_full_total` (how many distinct blocking events have occurred), `weir_sink_health`.

---

### WeirSinkDown — sink reporting unhealthy

**What it means:** The sink's health check returned `Down`. The drain has entered `RetryingTransient` with exponential backoff (base delay × 2ⁿ per attempt, max 3 retries before abandoning the segment). The sink health is re-checked per-segment and on a 30 s wall-clock interval.

**Likely causes:**
- Downstream database or HTTP endpoint is unavailable (connection refused, DNS failure)
- Network partition between weir host and sink
- Sink authentication failure (password rotated, credentials expired)
- Sink is overloaded and returning 5xx / 429

**Remediation:**
1. Check logs for the sink-supplied error reason (logged at `error` level when health transitions to `Down`).
2. Verify the downstream is reachable from the weir host: `curl`, `nc`, or equivalent.
3. If auth failure: rotate credentials and restart the daemon (no hot-reload for sink credentials outside TLS).
4. If downstream overloaded: the drain's exponential backoff buys time. WAB segments accumulate on disk; ensure `weir_wab_bytes_on_disk` growth is acceptable for the outage duration.
5. Monitor `weir_drain_state{state="retrying_transient"}` — if the drain exhausts `MAX_RETRIES` (3) without a successful commit, the segment is left on disk (not dead-lettered for transient errors) and the drain moves to the next segment. Left-behind segments are replayed on the next restart.

**Other metrics to check:** `weir_wab_bytes_on_disk` (WAB growing during outage), `weir_drain_state`, `weir_sink_commit_duration_seconds` (commit latency patterns before the outage).

---

### WeirDeadLettered — records being permanently rejected

**What it means:** `weir_sink_commit_records_total{outcome="dead_lettered"}` is incrementing. The sink is permanently rejecting records (as opposed to transient errors, which trigger retries). These records are written to the dead-letter directory and will not be re-sent to the sink without operator intervention.

**Likely causes:**
- Schema mismatch (sink table column type or length doesn't accept the record format)
- Authentication or authorization failure at the row level (PostgreSQL RLS, MySQL privilege check)
- Record content violates a database constraint not handled by the sink's idempotency mechanism
- HTTP sink receiving 4xx responses (excluding 408, 429) for permanently invalid records

**Remediation:**
1. Examine logs for the permanent error reason (logged at `warn` level with the sink-supplied message for each dead-lettered batch).
2. Fix the schema or configuration mismatch.
3. After fixing: dead-letter files can be replayed by re-processing them through weir or a direct sink writer. Files are standard WAB-format sealed segments (shard ID `0xFFFF`) readable with `weir_core::SegmentReader`.
4. Monitor `weir_dead_letter_bytes_on_disk` — if growing fast, check against `dead_letter_max_bytes` to anticipate `WeirDrainBlocked`.

**Other metrics to check:** `weir_sink_health`, `weir_dead_letter_bytes_on_disk`, `weir_drain_state`.

---

### WeirUnauthorizedConnection — peer UID rejection

**What it means:** The accept loop's `SO_PEERCRED` / `getpeereid` check rejected a connection because the connecting process's UID did not match the daemon's UID. The socket file is mode `0o600` (only the daemon's user can connect), so this indicates either an attempted bypass of the socket security model or a misconfigured producer.

**Likely causes:**
- A producer process running as a different UID than expected (e.g., container user mismatch)
- A probe or scan attempting to connect to the socket
- Operator manually connecting to the socket as root or another UID for debugging

**Remediation:**
1. Check logs for the rejected peer UID.
2. If it's a legitimate producer: fix the producer's user/group to match the daemon's UID. Check container `runAsUser` configuration.
3. If it's unknown: treat as a security event. Check who has access to the socket's parent directory (`ls -la $(dirname /run/weir/weir.sock)`). The parent directory should be mode `0o700` and owned by the daemon's user.
4. If it's an operator debugging session: use `sudo -u weir` to connect as the daemon's user.

**Other metrics to check:** `weir_tls_handshake_failures_total` (on TLS builds — related unauthorized-access signals), `weir_connection_idle_timeout_total` (combined with unauthorized attempts may indicate a scan).

---

### WeirSegmentQuarantined — corruption detected on recovery

**What it means:** During crash recovery, one or more WAB segments had CRC errors or unrecognized magic bytes and were moved to the quarantine directory (`<wab_dir>/quarantine/`). Records in quarantined segments could not be replayed and are potentially lost.

**Likely causes:**
- Torn write during a previous unclean shutdown (kernel did not flush buffered data after a power-loss or SIGKILL)
- Storage device returning corrupt data (failing disk)
- WAB directory on shared/network storage (unsupported — breaks the durability guarantees)
- Operator wrote to or modified WAB segment files

**Remediation:**
1. Examine logs for the quarantined segment filenames and the specific corruption detected (bad magic, CRC mismatch at byte offset N).
2. Copy quarantined files to safe external storage for forensic analysis before taking further action.
3. Check `weir_wab_fsync_failures_total` for the previous instance's lifetime — if fsync failures preceded the corruption, the corruption is explained by the fsyncgate scenario.
4. Examine the quarantined files with a WAB format reader (using the spec in `docs/wab_format.md`) to determine how many records were in the segment and recover any readable prefix.
5. Verify storage health: `smartctl -a /dev/sdX`, filesystem check.
6. If the WAB directory is on network storage: migrate to local block storage.

**Other metrics to check:** `weir_wab_unexpected_mode_total`, `weir_recovery_records_replayed_total` (how much was successfully replayed vs. quarantined).

---

### WeirTlsReloadFailed — SIGHUP cert rotation failed

**What it means:** A SIGHUP-triggered TLS certificate/key/CA reload failed. The daemon continues operating with the **previously loaded** TLS material, but the intended new certificate was not applied. Existing connections are unaffected; new connections will use the old certificate.

**Likely causes:**
- New certificate file not readable by the daemon user (permissions, path mismatch)
- New certificate has already expired or has a future not-before date
- Key/cert mismatch (private key doesn't match the new certificate)
- Cert file is a partial write (SIGHUP sent during a `cp` operation)

**Remediation:**
1. Check logs for the specific TLS parsing error (logged at `error` level on reload failure).
2. Verify the cert files are readable: `sudo -u weir openssl x509 -in /path/to/cert.pem -noout -dates`.
3. Verify key/cert pair: `openssl verify -CAfile ca.pem cert.pem`.
4. Ensure the cert file write is complete before sending SIGHUP — use atomic file replacement (`mv` after writing to a temp file) rather than in-place `cp` or `cat`.
5. After fixing the cert issue, send SIGHUP again to retry the reload.

**Other metrics to check:** `weir_tls_handshake_failures_total` (if the cert rotation was urgent — clients may be failing with the old cert), `weir_tls_config_reloads_total{outcome="ok"}` (confirms a successful reload after retry).

---

## 4. Deliverable Artifacts and Effort Estimate

The exploration above maps cleanly onto three committed artifacts.

### 4.1 `deploy/grafana/weir-dashboard.json`

A Grafana dashboard JSON provisioned via Grafana's dashboard provisioning API or the UI import workflow. Contains all panels from Section 1, organized into four collapsible rows.

**Implementation approach:**
- Start from a minimal Grafana JSON template (14 top-level fields: `panels`, `rows`, `title`, `uid`, `templating`, `time`, etc.).
- Each panel maps to one JSON object with `type`, `title`, `gridPos`, and `targets` (PromQL expressions).
- The `$__rate_interval` Grafana variable handles scrape-interval-aware rate windows automatically — no manual `[5m]` hardcoding needed.
- The dashboard should define a `job` template variable defaulting to `weir` so operators can scope to a specific instance in multi-instance deployments.
- Estimated effort: **2–3 days** (create all panels, tune grid layout, verify all PromQL against a real weir instance with prometheus scraping).

### 4.2 `deploy/prometheus/weir-alerts.yml`

A Prometheus alerting rule file in YAML, loadable via `rule_files:` in `prometheus.yml`. Contains all 20 alert rules from Section 2 grouped into three groups: `weir_durability`, `weir_drain`, `weir_ingest`.

Example structure:
```yaml
groups:
  - name: weir_durability
    rules:
      - alert: WeirFsyncFailure
        expr: weir_wab_fsync_failures_total > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "WAB fsync failure — durability hazard"
          description: "fsync returned an error on {{ $labels.instance }}. Data may be lost. Check logs for shard_id and OS error."
          runbook_url: "https://github.com/your-org/weir/blob/main/docs/explorations/phase4-observability.md#weirfsyncfailure"
```

**Implementation approach:**
- Write YAML by hand from Section 2 table; validate with `promtool check rules weir-alerts.yml`.
- Add `[TUNE]` placeholders as YAML comments with context for the operator.
- Estimated effort: **1 day** (transcription + validation + annotation writing).

### 4.3 `docs/operations/monitoring.md`

A standalone monitoring reference page cross-linked from `docs/README.md` and `docs/SUMMARY.md`. Contains:
- Prometheus scrape config snippet (`scrape_configs` entry for port 9185)
- Dashboard import instructions (Grafana UI or provisioning config)
- Alert rule installation instructions
- Abbreviated per-alert runbook (links to the full entries here)
- Operational thresholds table (what "normal" looks like per storage medium: NVMe vs SATA SSD vs cloud gp3)

**Implementation approach:**
- Primarily prose with embedded PromQL code blocks.
- References the `weir-alerts.yml` and `weir-dashboard.json` artifacts.
- Estimated effort: **1 day**.

### Total Effort Estimate

| Artifact | Effort |
|----------|--------|
| `weir-dashboard.json` | 2–3 days |
| `weir-alerts.yml` | 1 day |
| `monitoring.md` | 1 day |
| Integration testing (real Prometheus + Grafana against a running daemon) | 1 day |
| **Total** | **5–6 days** |

This is well-scoped for a single Phase 4 stream and can be developed in parallel with other 0.9.0 work.

---

## 5. Open Questions

**OQ-1: Alert thresholds for fsync latency.**
The `WeirFsyncLatencyHigh` threshold of 50 ms and `WeirFsyncLatencyCritical` at 200 ms are conservative placeholders. The right thresholds depend on the deployment's storage medium:
- NVMe: 5 ms / 20 ms would be more appropriate
- SATA SSD: 20 ms / 100 ms
- Cloud gp3 at baseline IOPS: 50 ms / 200 ms
Should the alert file ship with multiple threshold presets (via Prometheus recording rules or Grafana variable overrides), or should operators tune the YAML after installation? A `[TUNE]` annotation comment in the YAML may be sufficient.

**OQ-2: Multi-shard label aggregation.**
Currently all metrics aggregate across shards (no `shard_id` label). If a per-shard `shard_id` label is added to `weir_wab_fsync_duration_seconds` and `weir_wab_flusher_panics_total` in a future release, the fsync latency heatmap and flusher panic alerts become more precise (pinpointing which shard failed). This is worth tracking as a metrics enhancement request alongside the dashboard work.

**OQ-3: Drain replay tooling for dead-letter files.**
The runbooks for `WeirDeadLettered` and `WeirSegmentQuarantined` both reference a "WAB format reader" for processing dead-letter and quarantine files. Currently this requires writing a custom reader using `weir-core`'s `SegmentReader` API. A small CLI tool (`weir-inspect` or `weir-replay`) would make these runbook steps self-contained. This is a Phase 4 deliverable candidate but out of scope for the observability stream itself.

**OQ-4: Alertmanager routing recommendations.**
The alert file defines `severity: critical` vs `severity: warning` but does not define Alertmanager routing. Should the repository ship an example `alertmanager.yml` routing critical weir alerts to PagerDuty and warnings to Slack? This is a common ask but highly deployment-specific. A commented example in `monitoring.md` would cover most cases without prescribing a topology.

**OQ-5: bench-trace panel visibility.**
The per-stage breakdown panels (Panel 2.7) reference `weir_stage_*` metrics that only exist in `--features bench-trace` builds. The dashboard JSON needs a conditional strategy — either hide these panels in standard builds (they will return no data and show empty), or document them as a separate "performance debug" dashboard overlay. Grafana's `${__data.fields}` variables don't directly handle absent metrics well; the cleanest approach is a separate `weir-perf-debug-dashboard.json` for bench-trace builds.

**OQ-6: `weir_ack_timeout_total` ACK_TIMEOUT value.**
The `WeirAckTimeout` alert fires at `rate > 0` for 2 minutes. The sensitivity depends on `ACK_TIMEOUT` (a compile-time constant in `src/queue.rs` or similar — not surfaced in config). If `ACK_TIMEOUT` is short (e.g. 1 s), legitimate burst latency may fire this alert spuriously; if long (e.g. 30 s), ack timeouts happening at low rate over a long window need the alert's `for` duration tuned accordingly. The `ACK_TIMEOUT` value should be surfaced in the monitoring doc alongside the alert.

---

*End of exploration. All metric names verified against `crates/weir-server/src/metrics/mod.rs` at commit `cb1b3ba`.*
