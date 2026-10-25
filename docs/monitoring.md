# Monitoring weir

weir exposes Prometheus metrics at `/metrics`. This page is the operator's
companion: how to wire up monitoring, what each alert means and how to respond,
and a reference for every metric.

**The one thing to internalise:** weir is **fsync-bound**. `weir_wab_fsync_duration_seconds`
is the dominant latency signal in any durable-write deployment — fsync is
**~90–98%** of Durable-tier latency, the share rising as the disk gets slower
(measured: Mac NVMe ~150 µs, SATA SSD ~1.4 ms; see
`docs/benchmarks/phase3-results.md` and
`docs/benchmarks/snapshot-2026-06-13-comparison.md`). Read those two as the range
of what storage charges, not as a hardware ranking: they differ by **primitive as
well as by medium** — the Mac figure is macOS `F_BARRIERFSYNC`, a barrier rather
than a full flush, and the SATA one is an honest Linux `fdatasync`. Durable
throughput is set by your disk's fsync latency, full stop. The dashboard and
alerts are organised around making that obvious and actionable.

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

**See what an alert does before you need it in anger.** The rules are unit
tested — `promtool test rules deploy/prometheus/weir-alerts_test.yml` pins every
rule in both directions and runs in about a second, so you can read the suite to
see exactly which series state produces which alert.

The demo's opt-in `chaos` profile (`docker compose --profile chaos up`)
deliberately drives a permanently-failing sink, a full dead-letter dir, and
peer-UID rejections. **It will not make the shipped alerts go red, and this
document used to claim otherwise.** Every rule selects `job=~"weir"`, PromQL
matchers are fully anchored, and the chaos instance is scraped as
`job="weir-chaos"` (`deploy/monitoring/prometheus.yml`), so it matches no rule —
which is deliberate, so an idle chaos profile does not trip `WeirInstanceDown`
(`deploy/monitoring/README.md`). To watch a rule fire against live data, scrape
the failing instance under a job name the rules match. Note also that
`WeirSinkDown` is unreachable from that profile regardless: the failmock returns
400, and `sink/http.rs:645` maps any client error to `Degraded`, not `Down`.

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

#### WeirWabDirUnlocked
`weir_wab_dir_lock_held == 0`: this daemon is running **without** the exclusive
`flock` on `<wab_dir>/.weir.lock` that stops a second `weir-server` from using
the same `wab_dir`. A second daemon on the same directory recovers and drains
the first one's live segments, and the first one's later `Durable` acks become
false acks — the one failure on this page that is not "Nack, never a false
ack". Nothing is wrong yet; the guard is just not there.

The only steady state that reads 0 is `wab_dir_lock = "best-effort"`
(`--wab-dir-lock` / `WEIR_WAB_DIR_LOCK`) on a filesystem that cannot lock
(`ENOLCK`, `EOPNOTSUPP`, `ENOTSUP`, `ENOSYS`, `EINVAL`); the daemon also WARNs
about it on every start. The default, `"required"`, refuses to start there
instead. The other way to reach 0 — the integrity check proving another
instance holds or held the lock — exits the daemon with status 3 as soon as that
once-a-second check sees it, so the gauge is 0 only for the instant before
exit. That incident shows up as the process exiting with status 3 and an ERROR
beginning `CRITICAL: wab_dir ownership lost` (and as `WeirInstanceDown` if
nothing restarts it within a minute), not as this alert.

**Respond:** move `wab_dir` to a local filesystem that supports `flock` and
remove the `best-effort` setting. If that is not possible, make sure by other
means (orchestration, a single replica, no shared volume) that exactly one
daemon can ever run on this `wab_dir`, and silence this alert for the instance
deliberately rather than letting it fire unread.

A rising `weir_wab_dir_lock_integrity_failures_total` is the related signal for
a daemon that *does* hold the lock: something deleted or replaced the lock file
(the daemon re-creates and re-locks it), and while it was missing a second
daemon could have started.

### Readiness and conditions

#### WeirNotReady
The daemon reached `serving`, but `/readyz` has answered 503 for 5 minutes:
new connections are refused or pushes nack. Startup and shutdown are
excluded, because not-ready is their normal state; a daemon stuck in a startup
phase shows on `weir_startup_phase` and `/startupz` instead.
**Respond:** `curl -s 127.0.0.1:9185/readyz?verbose` (or `weir-ctl status`)
names the failing check. `wab_cap_rejecting`: delivery is behind and the WAB
reached `wab_max_bytes`; follow the drain and sink conditions, because raising
the cap only buys time. `no_live_shard` or `listener_down`: capture the logs,
then restart. See [Conditions](#conditions) for each code.

#### WeirConditionCritical
The daemon's own evaluator reports a critical condition that no dedicated rule
covers (`drain_stopped`, `drain_blocked_dead_letter_full` and
`fsync_failures_recent` have their own). The `code` label names it, and one
alert fires per code.
**Respond:** open that code's runbook under [Conditions](#conditions)
(`#cond-<code>`), and run `weir-ctl status` on the host: the admin view adds
the reason (panic message, error text) that the public `/status` leaves out.

### Drain / dead-letter

#### WeirDrainNotConfirming
Segments are entering the WAB and none are leaving it. **This is the only rule
here that can see a wedged drain**, so treat it as authoritative even when every
other drain signal looks healthy.

`weir_drain_state` and `weir_sink_health` are written *only* by the drain thread
(`drain/mod.rs`). When that thread wedges they freeze at their last value, so
`WeirDrainBlocked`, `WeirDrainStopped`, `WeirSinkDown` and `WeirSinkDegraded` are
all silent in exactly the incident they exist for, and `weir-readiness.sh` prints
`READY` throughout. This rule rests on `weir_wab_segments_total` instead — a
counter incremented by the WAB flusher (`sealed`) and by the drain's confirm path
(`confirmed`) — so neither side depends on the drain publishing its own health.

**Respond:** do not start from the gauges, they are lying by omission. Check
whether the drain thread is alive and what it is blocked on, then the sink.
`weir_drain_busy_seconds{kind}` answers the second question directly: like
`weir_wab_bytes_on_disk`, it is written by the main 5 s poll task rather than
the drain, so it stays live during a wedge, and the `kind` that is non-zero
names the sink call or local I/O operation the drain thread is inside. The
same poll task logs a stall verdict once the drain heartbeat freezes — see
[Drain liveness](#drain-liveness-reading-a-stalled-drain) for what each verdict
means and which one a restart actually fixes. `weir_wab_bytes_on_disk` is a
useful second opinion and should be climbing.

**Two shapes of this rule are wrong**, both documented in the rule's own comment.
A raw `sealed - confirmed` difference is blind after a restart-with-replay, since
recovery replays segments sealed by the previous process and `confirmed` can
restart far above `sealed` — measured at 417 segments of headroom, roughly
104 GiB at the default `wab_segment_max_bytes`. And `-` rather than `unless`
silently never fires on a daemon that has not yet confirmed anything, because the
`wab_segments` family is not pre-initialised and `{state="confirmed"}` does not
exist at all. Both cases are pinned in `weir-alerts_test.yml`.

**Blind spot:** an idle daemon. No seals means no left-hand series, so a drain
that is wedged while nothing is being produced does not fire — correctly, since
nothing is at risk, but it means this rule confirms a wedge rather than ruling
one out.

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
implementation, and a dead-letter open failure names its path. A segment the
drain panics on twice is parked (`weir_drain_segments_parked{reason="drain_panic"}`)
rather than retried, so one poison segment no longer spends the whole respawn
budget; several distinct poison segments still can. Set
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
is driven *only* by the health probe (`probe_health`), not by commit failures.
The probe runs in three places: on the `health_poll_interval_secs` cadence in the
`Draining` state; in `RetryingTransient`, after each retry backoff and before the
retry itself; and on every wake of `BlockedDeadLetterFull` (every
`dead_letter_check_interval_secs`). So during an outage `sink_health{down}` trails
the first failed commit by about one retry backoff (`sink_retry_base_delay_ms`,
doubling per retry, or a sink's `Retry-After`), not by a poll interval — while
`weir_drain_state{state="retrying_transient"}` flips on the first failed commit
itself. And `WeirSinkDown` holds for 5m on top of that. For prompt detection,
alert on the faster signals **in addition to** `WeirSinkDown`:
`weir_drain_state{state="retrying_transient"} == 1` (immediate on the first
transient failure) and `increase(weir_drain_segments_stranded_total[15m]) > 0`
(`WeirSegmentStranded`, fires once retries are exhausted) — see those entries.
A sink whose probe stays healthy while its commits fail (a POST-only outage)
never reaches `down` at all; only those two signals see it.

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
delivery for that segment is paused. Only a *sink* failure counts here: a segment
stranded by local I/O (reading the segment, or writing its dead-letter records)
counts in `weir_drain_local_io_strands_total{cause}` instead and does not fire
this alert — see `WeirDrainLocalIoStrands`.
**Respond:** fix the sink (correlate with `WeirSinkDown` / `weir_sink_health`).
Once the sink answers anything but `down` again, the drain **automatically
re-drains** stranded segments on a later health poll, after the segment's
cool-down (the backoff its last retry would have waited) has passed, and resumes
each one **past the records already delivered** in this run — watch
`weir_drain_segments_resumed_total` converge toward
`weir_drain_segments_stranded_total` and the alert clear. A daemon restart also
replays them, from the first record (the resume position is in memory only).
**Note: `stranded − resumed` is not a strict count of
currently-stuck segments** — both are event counters, and a segment that strands,
resumes, then re-strands (e.g. a POST-only outage where the HEAD probe stays
healthy) bumps both, so the difference can over- or under-state the live backlog.
For the actual stuck files, list them: `weir-ctl segments` shows the
sealed-but-undelivered files. If the gap persists, the sink isn't healthy yet (or is
recovering then re-failing). Raise `sink_max_retries` / `sink_retry_base_delay_ms` to ride out longer
outages before stranding. Distinct from `WeirDeadLettered` (*permanent* rejections).

#### WeirDrainLocalIoStrands
`increase(weir_drain_local_io_strands_total[15m]) > 0`: the drain exhausted
`sink_max_retries` on a segment because of a **local** failure, not a sink
failure, and set it aside. `cause="read"` means reading the sealed WAB segment
failed with a non-corruption error; `cause="write"` means the sink accepted the
batch but writing its rejected records into `dead_letter/` failed. These strands
used to count in `weir_drain_segments_stranded_total` and mark the sink down;
since the safety floor they count only here, so `WeirSegmentStranded` stays
quiet for them and this rule is the one that sees them. Warning, not critical:
the data is durable, nothing is false-acked, and the drain retries the segment
on its own.
**Respond:** investigate the WAB and dead-letter device, not the sink — kernel
I/O errors (`dmesg`), `ENOSPC` or a read-only remount, file permissions under
`wab_dir`, descriptor exhaustion (`EMFILE`/`ENFILE`). The drain's idle rescan
resumes the segment past its delivered prefix once its cool-down has passed, so
the alert clears by itself when the device recovers. A `write` strand keeps
cycling until the write lands and is never escalated. A `read` failure that
persists for 3 strand cycles and at least 10 minutes escalates: a
permission-class error (`EACCES`, `EPERM`, `EISDIR`, `ENOTDIR`, `ELOOP`) parks
the segment (`WeirSegmentParked`) and a media-class error quarantines it
(`WeirSegmentQuarantined`). Process-wide pressure (`EMFILE`, `ENFILE`,
`ENOMEM`, `ENOBUFS`, `EAGAIN`, `EINTR`) never escalates. The ERROR logged at
each strand (`drain: max retries exhausted reading the segment` / `... writing
dead-letter records`) names the segment path, and for a read the errno.

#### WeirSegmentParked
`weir_drain_segments_parked > 0` for 5m: the drain has **parked** one or more
sealed segments — left them on disk untouched and stopped re-queueing them for
the rest of this run. The records were acked and are durable, but **they are
not delivered until the daemon restarts**; no rescan picks a parked segment back
up. The gauge holds until that restart, so this alert does too. Critical: acked
data is stuck behind an operator action. The `reason` label says which action:

- `local_read_failed` — a permission-class read error that persisted for 3
  strand cycles and at least 10 minutes. Fix the segment file's permissions,
  ownership or type (the ERROR logged at park time names the path).
- `unsupported_version` — the segment was written in a newer WAB format than
  this binary reads, typically after a rollback. Upgrade weir. It is parked, not
  quarantined, so the rollback does not push the newer segments into manual
  recovery.
- `drain_panic` — the drain panicked twice while delivering this segment
  (`weir_drain_panics_total` moved too). Fix the sink, or the record it trips
  on, first; restarting unchanged replays the same segment into the same panic.

**Respond:** fix the cause for the reason shown, then restart the daemon. A
restart replays every parked segment from its first record, so delivery is
at-least-once for whatever part of it the sink had already taken.

#### WeirDeadLettered
The sink **permanently** rejected records (e.g. a 4xx). They are written to the
dead-letter directory for manual handling — not retried.
**Respond:** inspect the dead-letter files and the sink's rejection reason
(a schema mismatch, auth failure, malformed payload). Fix the upstream cause,
then reprocess with `weir-ctl dl requeue` — it re-submits the dead-lettered
records back through the daemon's socket and deletes each segment once all its
records are re-accepted. Re-delivery is at-least-once (a record may be delivered
more than once if a requeue run is interrupted). The HTTP sink's per-record
idempotency key is derived from where the record lands in the WAB (segment +
index), not just its payload bytes, so it dedupes a retry within one delivery
attempt but not a duplicate from a separate, later `dl requeue` run — that
writes the record into a new WAB entry with a different key. `weir-ctl dl`
also offers `list` (inspect) and `drop` (discard) for when the records aren't
worth reprocessing.

#### WeirDrainPausedLong
An operator paused delivery with `weir-ctl drain pause` over an hour ago, and
it is still paused. Ingest keeps buffering, so the WAB grows until
`wab_max_bytes` starts nacking pushes, or without bound when the cap is 0.
**Respond:** if the pause is still wanted, silence the alert for the planned
window. Otherwise `weir-ctl drain resume`. A restart also resumes delivery:
a pause lives in memory only.

#### WeirOldestUndeliveredAge
The oldest sealed segment the sink has not confirmed was created over an hour
ago. That is an upper bound on the oldest record's age: it is the segment's
creation time.
**Respond:** `weir-ctl status` tells stuck from slow. Stuck:
`delivery_stalled`, `sink_down`, `sink_failing`, `drain_paused` or
`segments_stranded`; follow that condition's runbook. Slow: only
`delivery_lagging`, with commits succeeding, means ingest outruns the sink;
compare `rate(weir_sink_commit_records_total[5m])` with the ingest rate and add
sink capacity. `[TUNE]` the hour to your delivery objective.

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
shift latency-tolerant traffic to the `Buffered` tier. `[TUNE]` the
threshold to your `batch_size × shard_count` and burst profile.

#### WeirDrainBackpressure
WAB flushers have spent the last ten minutes repeatedly waiting to hand sealed
segments to the drain, whose 256-slot channel is full. The acks of the batch
behind each wait cannot resolve until it ends, so producers see `ACK_TIMEOUT`
nacks for records that are already durable: a delivery problem that looks
like a storage one.
**Respond:** check sink commit latency (`weir_sink_commit_duration_seconds`)
and the sink and drain conditions in `weir-ctl status`. A paused drain
(`drain_paused`) produces exactly this. Producers' retries of the nacked
records will be delivered twice, which at-least-once allows.

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

#### WeirAdminUnavailable
`admin_socket_enabled` is on, but the daemon could not bind the admin socket.
The reason is on `/status` under `admin.reason_code`: `parent_insecure` (the
parent directory is group- or world-writable, or owned by a uid other than the
daemon's or root; the daemon refuses rather than let another user plant a
fake control socket there), `path_too_long`, `in_use` (another live socket
already answers at that path) or `bind_failed`.
**Respond:** make the parent directory private (`chmod go-w <dir>`, and
`chown <daemon user> <dir>` if another user owns it), or set
`admin_socket_path` to a private directory, then restart. Until then
`weir-ctl drain` and `weir-ctl reload` do not work; `kill -HUP` still reloads
TLS, and `weir-ctl status` falls back to HTTP and disk. To run without the
admin socket on purpose, set `admin_socket_enabled = false`; that does not
fire this alert.

### Integrity / tamper evidence

`weir-ctl attest` hash-chains **sealed** WAB segments so "was this segment
edited after it was written?" becomes an answerable question. It runs outside
the daemon entirely — no cost on the ack path, no `FORMAT_VERSION` bump, no
touch to the frozen conformance vectors. Three subcommands:

```
weir-ctl attest seal   --wab-dir /var/lib/weir/wab [--shard N]   # chain any sealed segment lacking a sidecar
weir-ctl attest verify --wab-dir /var/lib/weir/wab [--shard N]   # recompute the chain and compare
weir-ctl attest head   --wab-dir /var/lib/weir/wab               # print the newest head per shard, for anchoring
```

`seal` writes a `<segment>.wab.sealed.attest` sidecar per sealed segment,
chaining each record's already-frozen `RecordId` into a running SHA-256 and
linking the segment's start hash to the previous segment's head. `verify`
re-reads every byte and recomputes the chain from nothing — seconds on a
256 MiB segment, so it is an audit operation, not a health check, and must
never be wired into a readiness probe.

**Records in an active, unsealed segment are not chained at all** — only
`seal` puts anything into the chain, and it only ever sees sealed segments.
That window is bounded by the seal cadence:
[`wab_segment_max_bytes`](operations/configuration.md#wab_segment_max_bytes)
and
[`wab_segment_max_age_secs`](operations/configuration.md#wab_segment_max_age_secs)
are what close it. An operator who needs the window tighter lowers those, at
the usual cost of smaller/more-frequent segments.

**A `.attest` sidecar sitting next to the segment it protects is not evidence
by itself.** Anyone able to rewrite the segment can recompute a sidecar that
matches it, in the same motion. The chain only becomes evidence once its head
has been observed somewhere the editor does not control — in practice, the
line `attest seal` prints to its own stdout: `weir.attest.head` (shard,
segment name, record count, the 64-hex head).

**Be precise about what that line is.** It is a plain `println!` to a CLI
process's stdout — not a `tracing` event, not timestamped, not leveled, and
not shipped anywhere by itself. Nothing daemon-side is involved: the daemon
never runs `seal` and never sees this line. It becomes an anchor only once
**the operator** captures that stdout and ships it off-host — before anyone
with write access to the WAB directory could edit both the segment and its
sidecar to match. A `.attest` file with no independent, off-host record of
what its head used to say is a number, not an audit trail.

**Nothing here runs automatically or lives in the daemon — including `seal`
itself.** `seal` is the command that does the actual work: it is the only
one that writes `.attest` sidecars and the only one that prints the anchor
line. `verify` only ever checks what `seal` has already produced. Wiring up
`verify` alone (a mistake this page previously invited) means every segment
reports `MISSING_SIDECAR`, `weir_attest_verify_failures` sits at `0` forever,
and the feature has attested **nothing** — a clean-looking dashboard for a
WAB nobody has ever chained. Both commands need a schedule, and `seal`'s
needs its stdout captured, not just its exit code checked.

A systemd timer pair that runs both, with `seal`'s stdout landing in the
journal (forward that to your log pipeline the way you already ship the
daemon's own journal output):

```ini
# /etc/systemd/system/weir-attest-seal.service
[Unit]
Description=weir-attest: chain newly sealed WAB segments

[Service]
Type=oneshot
User=weir
ExecStart=/usr/local/bin/weir-ctl attest seal --wab-dir /var/lib/weir/wab
# StandardOutput=journal is what makes the weir.attest.head anchor line reach
# your log pipeline at all. Without this (or an equivalent `| systemd-cat` /
# `| logger` redirect), the line is printed once and discarded the moment
# this process exits — the chain would have no off-host anchor.
StandardOutput=journal
StandardError=journal
```

```ini
# /etc/systemd/system/weir-attest-seal.timer
[Unit]
Description=Run weir-attest seal periodically

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/weir-attest-verify.service
[Unit]
Description=weir-attest: verify the sealed WAB chain

[Service]
Type=oneshot
User=weir
ExecStart=/usr/local/bin/weir-ctl attest verify --wab-dir /var/lib/weir/wab \
  --metrics-file /var/lib/node_exporter/textfile_collector/weir_attest.prom
StandardOutput=journal
StandardError=journal
```

```ini
# /etc/systemd/system/weir-attest-verify.timer
[Unit]
Description=Run weir-attest verify periodically

[Timer]
OnBootSec=10min
OnUnitActiveSec=15min

[Install]
WantedBy=timers.target
```

Offset `verify`'s cadence after `seal`'s (as above), not tighter than it —
`verify` against segments `seal` has not reached yet just reports them as
`MISSING_SIDECAR`, but there is no reason to burn the full-reread cost (§
above) checking segments nothing has chained yet. The cron equivalent, for a
host not on systemd — with the same requirement to explicitly capture
stdout, since cron only mails it if `MAILTO` is configured and mail delivery
actually works, and `logger` is the portable way to land it in syslog/
journald regardless of that:

```cron
*/5  * * * *  weir  /usr/local/bin/weir-ctl attest seal   --wab-dir /var/lib/weir/wab 2>&1 | logger -t weir-attest-seal
*/15 * * * *  weir  /usr/local/bin/weir-ctl attest verify --wab-dir /var/lib/weir/wab --metrics-file /var/lib/node_exporter/textfile_collector/weir_attest.prom 2>&1 | logger -t weir-attest-verify
```

Its exit codes distinguish the failure mode for that job: `0` every segment
verified, `1` at least one segment's chain diverged from what was recomputed,
or its recorded link to its predecessor did not match reality (tampering, or
corruption past what CRC32 catches), `2` at least one segment could not even
be checked (missing/unreadable segment, undecodable sidecar). Script the
cron job to alert differently on `1` than on `2` — treat `2` as "unknown,"
not "clean." And watch `weir_attest_missing_sidecar` (below) alongside the
exit code: a nonzero, non-falling value there over time means `seal` isn't
running or isn't keeping up — the one failure mode the three-outcome exit
code above cannot surface, because `verify` against a WAB directory nobody
has ever sealed exits `0` too.

#### WeirAttestVerifyFailed
`weir_attest_verify_failures` does **not** come from the daemon — the
daemon is not involved in this feature at all. It's written by a cron/timer
running `weir-ctl attest verify --metrics-file`, via the node_exporter
textfile collector. A firing alert means either a sealed WAB segment no
longer matches its recorded hash chain, or a segment's recorded link to its
predecessor no longer matches what that predecessor actually produced (a
**broken link** — reported as `BROKEN_LINK` in `verify`'s text output —
which is what deleting or substituting a whole segment out of a chain looks
like). Reclaiming the *oldest* segments is not this: retention removes them
continuously, the oldest survivor is reported as a chain origin instead, and
`weir_attest_chain_origins` is where that shows up. A broken link always
names a boundary between two segments that are both still on disk.

**This is an integrity incident, not a durability one.** The records were
acked and fsynced correctly at the time; the bytes on disk (or the set of
segments present) have since stopped matching what was chained. Do **not**
delete anything.

**Respond:** compare the chain head against what was captured off-host from
`seal`'s stdout (the `weir.attest.head` line — see *Integrity / tamper
evidence* above for what that line is and is not), then run
`weir-ctl attest verify --wab-dir <dir>` to see which segment diverged, or
which segment boundary's link broke. Exit codes distinguish the failure
mode: `0` every segment verified with no broken links, `1` at least one
segment diverged or one link broke (tampered), `2` at least one segment
could not even be checked (unreadable segment or undecodable sidecar) —
treat `2` as "unknown," not "clean."

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

## Drain liveness: reading a stalled drain

The drain thread cannot report its own wedge — the gauges it writes freeze with
it (see [WeirDrainNotConfirming](#weirdrainnotconfirming)). So the daemon's 5 s
poll task watches it from outside, through two signals the drain only stores to:

- **A heartbeat**, bumped at the top of the drain loop, on every slice of the two
  deliberate waits (a retry backoff and the dead-letter-full block), after every
  sink commit, and every 1,024 records read. A drain that is waiting, or working
  through one large segment, keeps it moving.
- **A busy signal**: which operation the drain thread is inside, and since when.
  The poll task publishes it as `weir_drain_busy_seconds{kind}` on every tick,
  before its WAB-size scan, so a failing scan cannot silence it.

An idle drain only re-enters its loop once per `health_poll_interval_secs`, so
the heartbeat is allowed that long to stand still. Once it has been frozen for
`health_poll_interval_secs` + 15 s (45 s at the defaults), the poll task logs
one verdict, checked top to bottom:

| Condition | Verdict | Log | What to do |
|---|---|---|---|
| `weir_drain_state{state="stopped"}` is 1 | `Stopped` | WARN, "…and drain_state reports `stopped` — the drain thread is GONE, not wedged" | Nothing is wedged and there is no sink call to hunt: the supervisor exhausted its respawn budget (look for the earlier "drain thread panicked too many times" ERROR) or the daemon is shutting down. Restart. See [WeirDrainStopped](#weirdrainstopped). |
| busy in `sink_commit` or `sink_probe` for at most `commit_timeout` + 15 s | `SlowSinkCall` | INFO, "drain is inside a slow sink call…" | Nothing, unless it persists. The sink is slow and the timeout still bounds it; check the sink's latency. |
| busy in `segment_read`, `local_fsync`, `dead_letter_write`, `quarantine_move` or `dir_scan`, for **any** duration | `SlowLocalIo` | WARN, "drain has been inside one local I/O operation…" | Check the WAB device (I/O latency, a failing or saturated disk, a stalled network filesystem). **Do not restart for this**: a restart re-delivers from record 0 and leaves the disk exactly as slow. |
| busy in `sink_commit` or `sink_probe` for more than `commit_timeout` + 15 s | `Wedged` | WARN, "drain heartbeat has not advanced in over Ns — the drain thread may be wedged…" | The sink is blocking the drain's OS thread (a synchronous driver, `std::fs`, a spin loop) instead of yielding, so no timeout can fire. A restart is the only recovery; the sink implementation is the bug. |
| nothing busy | `Wedged` | same as above | The drain thread is stuck somewhere no busy guard covers. Restart, and capture a thread dump first if you can. |

`commit_timeout` is `max(2 × sink_timeout_secs, 60 s)` — 60 s at the defaults,
so a sink call reads as `Wedged` once it has been busy for more than 75 s.

A repeat of the same verdict, or a milder one, is logged at most once every
5 minutes. An **escalation** is logged at once: a sink that blocks the thread
always reads first as `SlowSinkCall`, and its `Wedged` WARN is not held back
behind that INFO.

No shipped alert reads `weir_drain_busy_seconds`, because what counts as "too
slow" for a local operation depends on your disk. If you add one, alert on the
local kinds separately from the sink kinds — they need opposite responses.

---

## Metric reference

All metrics are prefixed `weir_`. Counters carry a `_total` suffix in the
exposition; histograms expose `_bucket` / `_sum` / `_count`.

### Ingest

> **Changed in 2.0.0 — dashboards need a one-line edit.** The `tier` label was
> `sync`/`batched`/`buffered` and is now `durable`/`buffered`. `sync` and
> `batched` were separate series for identical behaviour; they are one series
> named `durable`. Any panel or alert selecting `tier="sync"` or
> `tier="batched"` will silently return no data until updated — panels go blank
> rather than erroring, so grep your dashboards rather than waiting to notice.

| Metric | Type | Meaning |
|---|---|---|
| `weir_records_accepted_total{tier}` | counter | Records admitted for processing, by durability tier (`durable`/`buffered`). |
| `weir_records_ack_total{tier}` | counter | Records durably acked to the producer, by durability tier (`durable`/`buffered`). The gap `accepted − ack` is in-flight or failed (Nacked) work; it is only ~0, and the amortization ratio only meaningful, while `weir_records_nack_total` stays flat. |
| `weir_records_nack_total{tier,reason}` | counter | Records Nacked (not durably accepted), by durability tier and `reason`. Should be ~0. `reason` is one of `bad_magic`, `version_mismatch`, `bad_header_crc`, `payload_too_large`, `bad_payload_crc`, `internal_error`, `empty_payload`, `unknown_message`, `reserved_flags_set` (the wire `NackReason` variant names, lowercased). `internal_error` is the only *transient* reason (queue saturation / ack timeout / write-fsync error / the [`wab_max_bytes`](operations/configuration.md#wab_max_bytes) cap rejecting — connection stays open, producer retries); the rest are permanent protocol/payload errors that close the connection. The cap is the one cause with its own counter — `weir_wab_cap_rejections_total` — so subtracting it from `internal_error` separates "the WAB is full" from the other three. |
| `weir_batch_records` | histogram | Records carried by one `PushBatch` frame. Only observed for the optional batch extension, so it stays empty on a daemon no client batches against. Its shape is the question "are producers actually batching?" — a distribution pinned near 1 means the batch path is being paid for without being used. |
| `weir_batch_partial_total` | counter | `PushBatch` frames answered with at least one record not durable. Should be ~0. A partial batch is not a protocol error: the frame was valid and the connection stays open, and the clear bits are the same at-least-once signal a single-record `Nack(InternalError)` carries. Rising values track the same underlying causes as `internal_error` — queue saturation, ack timeout, a write or fsync that did not succeed, the `wab_max_bytes` cap — so read it alongside `weir_records_nack_total{reason="internal_error"}` rather than on its own. |
| `weir_accept_latency_seconds` | histogram | Socket-accept → connection-handler spawn latency (independent of fsync). |
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
| `weir_wab_fsync_failures_total` | counter | fsync returned an error. **Must be 0.** The failing batch is nacked, and the active segment is *retired* so no later batch can be acked over the hole. Nothing at runtime rescans an unsealed `.wab`, so any already-acked records left in that segment (up to `wab_segment_max_bytes`, 256 MiB by default) are **stranded until the daemon restarts** and crash recovery seals them. Stranded, not lost — but clear the storage fault and restart to release them. |
| `weir_wab_group_commit_records` | histogram | Records covered by one WAB group fsync — the fsync amortisation factor. **1.0 means every record paid for its own fsync**, which is the condition that makes a round-trip-bound producer slow for a reason batching cannot fix, and the condition under which adding concurrency helps far more than adding batching. Read it together with `weir_wab_fsync_duration_seconds`: the two multiply into the per-record durable cost. |
| `weir_wab_flusher_panics_total` | counter | Flusher thread panics. **Must be 0.** |
| `weir_drain_panics_total` | counter | weir-drain thread panics caught and respawned by its supervisor. **Must be 0**; sustained values indicate a sink/drain logic bug, and exhausting the respawn budget stops delivery. |
| `weir_wab_segments_total{state}` | counter | Segment lifecycle transitions: `open` → `sealed` → `confirmed`; `quarantined` on corruption. A segment is counted `sealed` whichever way it was sealed — rotation, idle-seal, shutdown-seal, **or crash recovery** (recovery finalises an unsealed `.wab` and renames it, which is a real transition and is counted). A mid-file-corrupt segment counts **both** `quarantined` (the preserved forensic copy) and `sealed` (the truncated valid prefix, which is still delivered). **Backlog caveat across a restart** — see the note below. |
| `weir_wab_bytes_on_disk` | gauge | Bytes used by **live** WAB shard segments: the open active segment (`.wab`) **plus** sealed segments awaiting drain (`.wab.sealed`). It scans on a 5 s cadence, so it trails real-time by up to that interval. It is **not** a total-disk-usage gauge: it excludes drained-marker files (`.wab.confirmed`) and the `dead_letter/` (own gauge: `weir_dead_letter_bytes_on_disk`) and `quarantine/` subdirs, and it doesn't measure the filesystem's free space — for the "disk filling up" signal use a node/host filesystem alert on the partition holding `wab_dir` (see *Capacity*). |
| `weir_wab_record_logical_bytes_total` | counter | Cumulative record payload bytes **as accepted from producers**, before any compression. Equal to `weir_wab_record_stored_bytes_total` when `wab_compression = "none"`. Counts payload bytes only — not the 8-byte per-record framing, the segment header, or the footer — so it is not a file-size measure. |
| `weir_wab_record_stored_bytes_total` | counter | Cumulative record payload bytes **as written to disk**, after compression. Divide the logical counter by this one for the live compression ratio (see *Compression ratio* below). Bumped only after a successful write, so a rejected record moves neither counter. |
| `weir_wab_unexpected_mode_total` | counter | Segment file found with unexpected permissions (tampering guard). |
| `weir_wab_cap_rejections_total` | counter | Pushes Nacked because live WAB bytes exceeded [`wab_max_bytes`](operations/configuration.md#wab_max_bytes). These surface to clients as `NackReason::InternalError` — **the same byte queue saturation uses** — so this counter is the **only** way to distinguish a cap rejection from queue-saturation `InternalError` Nacks. Zero when `wab_max_bytes` is unset (the default). |
| `weir_recovery_records_replayed_total` | counter | Records replayed from sealed-but-unconfirmed segments on startup. |
| `weir_recovery_segments_quarantined_total` | counter | Corrupt segments quarantined during **active**-segment recovery (bad header, or mid-file corruption with a copied tail). Covers every segment quarantine: active-segment recovery (bad header, or mid-file corruption with a copied tail), sealed-segment replay when a `.confirmed` sidecar fails verification, and drain-time quarantine. It moves in lockstep with `weir_wab_segments{state="quarantined"}`. The one thing it deliberately does **not** count is the `.confirmed` **sidecar file** itself when it is quarantined alongside its segment — that would double-count a single incident. `weir_quarantine_bytes_on_disk` is the signal for how much is sitting there. Recover with `weir-ctl quarantine requeue` — see [the runbook](operations/quarantine-recovery.md). |
| `weir_recovery_segments_failed_total` | counter | Segments whose active-segment recovery (`recover_segment`) failed **and left the segment at its original path**, so nothing will ever reach it. **Deliberately excludes quarantined segments** — a header-level quarantine (bad magic / unknown version / too-short header) moves the file and is counted by `weir_recovery_segments_quarantined_total` instead, because it is handled, not abandoned. (Such a segment is listed by `weir-ctl quarantine list`, though `inspect` cannot parse it — the header is what's corrupt.) **Does NOT fire for the mid-file-corruption case** this toolchain exists for: recovery returns `Ok` there, having sealed and delivered the valid prefix. Non-zero therefore means a genuine I/O failure left a segment stranded — check the startup logs for the path and cause. |
| `weir_quarantine_bytes_on_disk` | gauge | Total bytes under `<wab_dir>/quarantine/`. **The one signal that moves on every quarantine event**, including the `.confirmed`-sidecar case neither counter above catches. Survives a restart (unlike the counters), refreshed on the same cadence as `weir_dead_letter_bytes_on_disk`. Recover with `weir-ctl quarantine requeue`. |
| `weir_recovery_quarantine_copy_failed_total` | counter | Mid-file-corrupt segments whose quarantine copy failed (disk full / read-only / inode exhaustion); recovery left the segment un-truncated to preserve acked-durable tail records and will retry. **Non-zero means recovery is stuck on that segment — clear the disk/read-only state and restart.** |
| `weir_ack_timeout_total` | counter | Acks that never fired within the timeout (wedged flusher). |
| `weir_wab_dir_lock_held` | gauge | 1 while this daemon holds the exclusive `flock` on `<wab_dir>/.weir.lock`, so no second `weir-server` can recover or drain its live segments. **0 means it runs without that guard**: `wab_dir_lock = "best-effort"` on a filesystem that cannot lock. It also reads 0 for the instant before an exit with status 3, when the daemon has proven another instance owns the `wab_dir`. Starts at 0 and is set before the metrics endpoint binds, so a scrape never shows a guard that is not there. **Alert on 0** — the shipped [`WeirWabDirUnlocked`](#weirwabdirunlocked) rule. |
| `weir_wab_dir_lock_integrity_failures_total` | counter | Once-a-second integrity checks of `<wab_dir>/.weir.lock` that found the file deleted or replaced (the daemon re-created and re-locked it, and logs an ERROR saying so), or that failed with an errno (retried a second later). **Must be 0.** Any increase means something is deleting or replacing the lock file of a running daemon, and while it was missing a second `weir-server` could have started on this `wab_dir`. Proven loss to another instance is not counted here: the daemon exits with status 3 instead. |
| `weir_stage_queue_seconds`, `weir_stage_bridge_wait_seconds`, `weir_stage_write_seconds`, `weir_stage_total_seconds` | histogram | Per-stage latency decomposition (`bench-trace` builds only — diagnostic): queue + coalesce wait, bridge hop + flusher receive, record write before fsync, and enqueue → ack end to end. |

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

> **Changed after 4.0.1 — `weir_drain_segments_stranded_total` now counts sink
> failures only.** A segment stranded because the drain could not *read* it, or
> could not write its sink-rejected records into `dead_letter/`, used to count
> there too, and to mark the sink down. It now counts in
> `weir_drain_local_io_strands_total{cause}` and leaves the sink's health alone.
> On a host whose WAB device misbehaves, expect `stranded` (and
> `WeirSegmentStranded`) to go quieter and the new counter to carry what moved;
> `WeirDrainLocalIoStrands` alerts on it, and `WeirSegmentParked` on segments
> the drain has set aside until a restart.
> The rest of this table's drain-ledger rows are new in the same release; every
> one of their series exists at 0 from boot.

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
| `weir_drain_segments_stranded_total` | counter | Segments left on disk after exhausting `sink_max_retries` **transient sink** failures. Local I/O strands are not counted here (see the note above and `weir_drain_local_io_strands_total`). The data is durable; delivery is paused. **Alert on `increase(weir_drain_segments_stranded_total[15m]) > 0`** and correlate with `weir_sink_health{state="down"}`. They are re-drained automatically, past the records already delivered, once their cool-down has passed and the sink answers (see below), or from the first record on restart. This is an **event counter, not a live gauge of distinct stuck segments**: the same segment can strand, auto-resume on the next healthy probe, and re-strand — each strand increments it — so it can exceed the number of distinct stuck segments. That happens notably in a **POST-only outage**: deliveries (`commit`) keep failing while the HEAD health probe stays healthy, so each recovery edge re-queues the segment, it re-fails, and it strands again. Distinct from `weir_dead_letter_full_total` (permanent rejections). |
| `weir_drain_segments_resumed_total` | counter | Stranded segments re-queued for delivery by the drain's rescan, which runs on a sink health **recovery** (down→up) and on every health poll that finds the drain idle with the sink not `down`. Convergence with `weir_drain_segments_stranded_total` means an outage's backlog has been picked back up; a persistent gap means segments are still stranded (sink not healthy yet). Segments resumed after a local I/O strand count here too, so `resumed` can run ahead of `stranded`. The count can over-state at a recovery edge (a copy still in the drain channel is counted again), but a segment is never *processed* twice: both copies pass the drain's admission check first. |
| `weir_drain_local_io_strands_total{cause}` | counter | Segments set aside after exhausting `sink_max_retries` on a **local** I/O failure rather than a sink failure. `cause="read"`: reading the sealed segment failed with a non-corruption error. `cause="write"`: writing sink-rejected records into `dead_letter/` failed after the sink commit succeeded. Unlike `weir_drain_segments_stranded_total` these do not mark the sink down; the idle rescan (a health poll that finds the drain idle and the sink not `down`) resumes them past their delivered prefix once their cool-down has passed. **Investigate the WAB and dead-letter device** (I/O errors, `ENOSPC`, permissions, descriptor exhaustion), not the sink. A read failure that persists for 3 strand cycles and at least 10 minutes escalates: a permission-class error parks the segment (`weir_drain_segments_parked{reason="local_read_failed"}`), a media-class error quarantines it. Process-wide pressure (`EMFILE`, `ENFILE`, `ENOMEM`, `ENOBUFS`, `EAGAIN`, `EINTR`) never escalates. |
| `weir_drain_segments_parked{reason}` | gauge | Segments the drain has parked: left on disk untouched, not re-queued for the rest of this run, and replayed from their first record on the next start. `reason="local_read_failed"`: a permission-class read error (`EACCES`, `EPERM`, `EISDIR`, `ENOTDIR`, `ELOOP`) that persisted for 3 strand cycles and at least 10 minutes — fix the file's permissions or type, then restart. `reason="unsupported_version"`: the segment was written in a newer WAB format than this binary reads — upgrade weir; it is parked rather than quarantined so a rollback does not push the newer segments into manual recovery. `reason="drain_panic"`: the drain panicked twice while delivering this segment — fix the sink (or the record it trips on) before restarting. **Non-zero means acked data is not being delivered until a restart**; the ERROR at park time names the path. |
| `weir_drain_segments_disposition_pending{kind}` | gauge | Segments the drain has finished with that still owe a local step, retried on every health poll and **never re-delivered** while they wait. `kind="confirm"`: a delivered segment whose `.confirmed` sidecar write or delete has not succeeded yet. `kind="quarantine"`: a segment whose move into `quarantine/` has not succeeded yet. Should be 0, or briefly non-zero; a value that stays up means the WAB device is full or read-only — the matching failure counters below say which step. |
| `weir_drain_confirm_failures_total{stage}` | counter | Failed steps confirming a delivered segment. `stage="sidecar"`: the atomic `.confirmed` write (temp file, fsync, rename, directory fsync). `stage="delete"`: removing the segment after a durable sidecar. Each failure is retried on the next health poll without re-delivering the segment (`weir_drain_segments_disposition_pending{kind="confirm"}`). A rising `sidecar` count usually means the WAB device is full or read-only. |
| `weir_drain_quarantine_failures_total` | counter | Segments the drain could not move into `quarantine/` (corrupt, truncated or persistently unreadable segments whose readable prefix was already delivered). Counted **once per segment**; the move is then retried on every health poll (`weir_drain_segments_disposition_pending{kind="quarantine"}`), the bytes stay in place, and the prefix is not re-delivered. Usually a full or read-only WAB device, or something other than a directory at `<wab_dir>/quarantine`. |
| `weir_drain_dead_letter_write_failures_total` | counter | Failed attempts to write sink-rejected records into `dead_letter/` after the sink commit succeeded. The drain keeps the rejected records in memory and retries **only the write**, so the accepted records are not committed again; the segment stays on disk until the write lands. A segment that exhausts its retries this way counts in `weir_drain_local_io_strands_total{cause="write"}` and keeps cycling (it is never parked), so it self-heals once space frees up. Usually a full or read-only WAB device (`ENOSPC`, `EROFS`, `EACCES`). |
| `weir_drain_channel_depth` | gauge | Sealed segments waiting in the bounded flusher-to-drain channel (capacity `weir_drain_channel_capacity`). A recent snapshot, written by each flusher after a hand-off and by the drain on every wait slice, published every 500 ms. Sustained at capacity means the drain is the bottleneck: flushers block at rotation, and producers can see ACK_TIMEOUT NACKs for records that are already durable. Check `weir_wab_handoff_blocked_seconds` and sink latency before suspecting storage. |
| `weir_drain_channel_capacity` | gauge | Capacity of the flusher-to-drain channel (256). Use it for a `depth / capacity` panel. |
| `weir_drain_busy_seconds{kind}` | gauge | Seconds the drain thread has spent inside its **current** operation. `kind` is `sink_commit` or `sink_probe` (calls into the sink) or `segment_read`, `local_fsync`, `dead_letter_write`, `quarantine_move`, `dir_scan` (local I/O on the WAB device; `dir_scan` includes the rescan's orphan-file cleanup fsync). Only the current kind is non-zero, and all read 0 between operations. Written by the 5 s poll task, not the drain, so it **stays live while the drain is wedged** — sampled every 5 s, so operations shorter than that rarely show. A large local value means a slow disk, which a restart does not fix; a `sink_commit` value past `commit_timeout` means a sink blocking the drain thread. See [Drain liveness](#drain-liveness-reading-a-stalled-drain). |

### System / security
| Metric | Type | Meaning |
|---|---|---|
| `weir_connection_rejected_peer_uid_total` | counter | Connections rejected by the `SO_PEERCRED` UID check. |
| `weir_accept_resource_exhaustion_total` | counter | `accept(2)` failures from resource exhaustion (EMFILE/ENFILE/ENOBUFS/ENOMEM); the accept loop backs off on each. A rising value means the daemon is near an fd/memory limit. |
| `weir_connection_idle_timeout_total` | counter | Connections dropped for read-idle (slowloris guard). |
| `weir_connections_aborted_at_shutdown_total` | counter | Connections force-closed at shutdown after the grace period. |
| `weir_tls_handshake_failures_total` | counter | (tls) Mutual-TLS handshake failures. |
| `weir_tls_config_reloads_total{outcome}` | counter | (tls) SIGHUP TLS cert/key/CA reload attempts, by `outcome` (`ok` / `failed`). **Alert on `outcome="failed"`** — a failed reload means the daemon keeps serving the old certificate. Unchanged by `weir_config_reloads_total` below, which counts the same reloads again among every other SIGHUP. |
| `weir_config_reloads_total{trigger,outcome}` | counter | Every config reload request, on every build. `trigger` is `sighup`, or `admin` (the admin socket's `reload` verb, which runs the same reload). `outcome` is `ok` (something was reloaded — today only TLS material), `failed` (a reload was attempted and rejected; the previous config stays in force), `noop` (nothing reloadable: the config is read once at startup, so a daemon without TLS answers every SIGHUP this way and keeps running), or `dropped` (the request never ran: the 8-deep reload queue was full, as in a SIGHUP flood, or the reload loop had stopped). A SIGHUP that arrives before the handler is installed, during startup, is ignored and not counted. |
| `weir_http_requests_dropped_total{reason}` | counter | Connections to the metrics/health listener (`metrics_bind:metrics_port`) closed without a response. `reason` is `head_timeout` (no complete request head within 1 s: a stalled or slowloris peer), `head_pool_full` (all 64 head slots busy: a connection flood), `metrics_pool_full` (more concurrent scrapes than [`metrics_max_connections`](operations/configuration.md#metrics_max_connections); Prometheus retries on its next interval) or `probe_pool_full` (more than 4 concurrent `/livez`, `/readyz`, `/startupz` or `/status` requests). Should be ~0. A rising `head_timeout` or `head_pool_full` on a listener bound beyond loopback means someone is holding connections open: restrict the port to the scraper and the kubelet. |
| `weir_admin_requests_total{verb,outcome}` | counter | Requests on the admin Unix socket. `verb` is the wire verb (`status`, `reload`, `drain.pause`, `drain.resume`, `drain.retry_stranded`), `_connect` for a connection refused before any request, or `_invalid` for a line that is not a valid request. `outcome` is `ok`, `error` (the exact error code is in the daemon log), `refused` (the peer is neither the daemon's uid nor root — **investigate every one**: the socket is mode 0600 in a private directory, so a refusal means something loosened that) or `busy` (more than 4 concurrent admin connections). |
| `weir_admin_socket_up` | gauge | 1 while the admin socket is bound and served. 0 when it is disabled (`admin_socket_enabled = false`), unavailable (the derived default path could not be bound; the `admin_unavailable` condition and an ERROR at startup say why), or closed during exit. |

### Status, lifecycle and conditions

Added by Foundations. The conditions behind `weir_condition_active` get their own section on this page, one `cond-<code>` heading each (the runbook link every condition carries) *(written from PR 8)*. Families marked *(written from PR n)* exist at 0 from boot and move once the named part of Foundations is in the build.

| Metric | Type | Meaning |
|---|---|---|
| `weir_build_info{version,wire_version}` | gauge | Always 1; the labels carry the daemon and wire-protocol versions. Join on it to label dashboards by version. |
| `weir_start_time_seconds` | gauge | Unix time the process started. `time() - weir_start_time_seconds` is uptime; a reset means a restart. |
| `weir_startup_phase{phase}` | gauge | One-hot lifecycle phase: `initializing`, `recovering`, `handoff`, `binding`, `serving`, `shutting_down`. A daemon stuck in `recovering` is replaying a large or slow WAB. |
| `weir_ready` | gauge | 1 when the daemon can accept ingest: serving, not shutting down, not cap-rejecting, at least one live shard, no listener down. Refreshed every 5 s. Sink health never gates it. |
| `weir_live` | gauge | 1 unless a check selected in `livez_checks` fails. With the default empty list it is always 1 while the process answers. |
| `weir_condition_active{code,severity}` | gauge | 1 while the condition is active; 0 when inactive **or unevaluated** (missing evidence). One pre-created series per code. Alert on `severity="critical"`. |
| `weir_wab_shard_up{shard}` | gauge | 1 while the shard's flusher is up or respawning. 0 means new pushes for that shard nack; see `shard_offline`. |
| `weir_wab_handoff_blocked_seconds{shard}` | gauge | Seconds the shard's flusher has been blocked handing a sealed segment to a full drain channel. Non-zero means delivery is stalling ingest (acks wait), not slow storage. *(written from PR 3)* |
| `weir_worker_up{worker}` | gauge | 1 while the worker thread runs. 0 after startup means the shards it owns cannot accept records. |
| `weir_wab_cap_rejecting` | gauge | 1 while `wab_max_bytes` is rejecting pushes. Since Foundations it clears on its own once the WAB drains below 90% of the cap, without traffic. |
| `weir_wab_undelivered_segments` | gauge | Sealed segments not yet confirmed delivered, from a read-only 5 s scan. |
| `weir_wab_undelivered_records` | gauge | Records in those segments, from their footers. |
| `weir_wab_oldest_undelivered_age_seconds` | gauge | Upper bound on the age of the oldest undelivered record (from its segment header); 0 when nothing is undelivered. |
| `weir_wab_active_segment_age_seconds{shard}` | gauge | Age of the shard's open segment. With both seal timers 0 a quiet shard's records wait here until rotation or shutdown. |
| `weir_drain_pending_segments{queue}` | gauge | Segments queued in the drain: `live` (from the channel) and `backlog` (startup replay and rescans). *(written from PR 2)* |
| `weir_drain_replay_remaining_segments` | gauge | Startup replay segments not yet delivered. *(written from PR 2)* |
| `weir_drain_heartbeat_age_seconds` | gauge | Seconds since the drain heartbeat last moved. Read with `weir_drain_busy_seconds`. |
| `weir_drain_commit_in_flight_seconds` | gauge | Seconds the current sink commit has been in flight; 0 when none. *(written from PR 2)* |
| `weir_drain_paused` | gauge | 1 while an operator has paused delivery. Ingest keeps buffering. *(written from PR 2)* |
| `weir_drain_generation` | gauge | Drain thread starts in this process (1 + respawns). *(written from PR 2)* |
| `weir_sink_last_success_timestamp_seconds` | gauge | Unix time of the last successful sink commit; 0 before the first. *(written from PR 2)* |
| `weir_sink_last_failure_timestamp_seconds{kind}` | gauge | Unix time of the last failed commit, by `kind`: `transient`, `permanent`, `timeout`, `contract_violation`. *(written from PR 2)* |
| `weir_sink_consecutive_failures` | gauge | Commits failed in a row since the last success. *(written from PR 2)* |
| `weir_config_wab_max_bytes` | gauge | Effective `wab_max_bytes` (0 = no cap). |
| `weir_config_dead_letter_max_bytes` | gauge | Effective `dead_letter_max_bytes`. |
| `weir_config_wab_segment_max_bytes` | gauge | Effective `wab_segment_max_bytes`. |
| `weir_config_segment_max_age_seconds` | gauge | Effective `wab_segment_max_age_secs` (0 = off). |
| `weir_config_segment_max_lifetime_seconds` | gauge | Effective `wab_segment_max_lifetime_secs` (0 = off). |
| `weir_config_sink_max_retries` | gauge | Effective `sink_max_retries`. |
| `weir_config_shutdown_timeout_seconds` | gauge | Effective `shutdown_timeout_secs`. |
| `weir_config_drain_shutdown_budget_seconds` | gauge | Effective `drain_shutdown_budget_secs`: how long the drain keeps delivering after SIGTERM once everything is sealed. |
| `weir_drain_shutdown_deadline_seconds` | gauge | Unix time at which the current bounded shutdown abandons the drain's in-flight work; 0 when not shutting down. `weir_drain_shutdown_deadline_seconds - time()` is the budget left. |
| `weir_records_nack_internal_total{cause}` | counter | Transient `InternalError` nacks by cause: `queue_full`, `write_failed`, `sender_dropped`, `ack_timeout`, `wab_cap`, `coordinate`, `ownership_lost`. The wire reason is the same for all of them; this is how to tell them apart. |
| `weir_connections_refused_no_live_shard_total` | counter | New connections refused because no shard could accept records. *(written from PR 3)* |
| `weir_drain_segments_abandoned_total` | counter | Segments left partly delivered when the drain thread exited on a stop: at the shutdown deadline, or while the segment was held by a pause or blocked on a full dead letter. They stay sealed and unconfirmed and are re-delivered from record 0 on the next start. *(written from PR 2)* |
| `weir_wab_segments_not_handed_off_total{reason}` | counter | Sealed segments a flusher could not hand to the drain, by `reason`: `budget` (shutdown hand-off budget spent) or `disconnected` (drain gone). Durable; delivered on restart. *(written from PR 3)* |

### Integrity / tamper evidence (weir-attest)

> **Unlike every other row on this page, these do not come from the daemon's
> `/metrics`.** They are written by a cron-run `weir-ctl attest verify
> --metrics-file <path>` onto the node_exporter textfile collector — see
> [Integrity / tamper evidence](#integrity--tamper-evidence) above for the
> full picture, **including that `verify` alone attests nothing: `seal` must
> be scheduled too.** All four are gauges, overwritten wholesale on each run,
> not counters: they carry no `_total` suffix on purpose.

| Metric | Type | Meaning |
|---|---|---|
| `weir_attest_segments` | gauge | Sealed segments examined by the last `weir-ctl attest verify` run. |
| `weir_attest_verify_failures` | gauge | Segments that diverged, or whose recorded link to their predecessor did not match reality (a broken link — see above), or that could not be verified in the last run. **Must be 0** — a firing alert here is an integrity incident, not a durability one. See [`WeirAttestVerifyFailed`](#weirattestverifyfailed). |
| `weir_attest_chain_origins` | gauge | Segments observed with no chain predecessor in the last run — the first segment of a shard, the oldest to survive **retention** (`confirm_and_delete` reclaims drained sealed segments continuously, so this is the steady-state case, not a rare one), one whose predecessor was quarantined, or one whose sidecar declares itself an origin. **Never counted as a failure.** Verify cannot tell a reclaimed predecessor from a deleted one — after either, the predecessor is simply gone — so an origin is not evidence of tampering and an *unexpected* origin is; only your own record of a shard's history distinguishes them. Alert on this **changing unexpectedly** (a jump with no reclamation or quarantine behind it), not on it being nonzero. |
| `weir_attest_missing_sidecar` | gauge | Sealed segments with no `.attest` sidecar in the last run — `seal` has never chained them. **Not** counted in `weir_attest_verify_failures` (a missing sidecar is not tampering by itself — and since the *next* segment's `prev_head` commits to this one's content, verify recomputes the head of a sidecar-less segment and still checks that link, so deleting a sidecar to hide an edit breaks the chain rather than erasing it) — but a nonzero, non-falling value here over time means `seal` isn't running or isn't keeping up, which the other three metrics cannot tell you: a WAB directory nobody has ever sealed reports `verify_failures 0` forever. |

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

