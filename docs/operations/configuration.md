# Configuration reference

> **TL;DR** — weir reads config from CLI flags, env vars, an optional
> TOML file, and built-in defaults, in that precedence order. Every
> option has a sensible default; the minimum viable config is
> `wab_dir` + the parent of `socket_path` existing and being writable
> by the daemon's user. The full reference below covers every option
> with its default, range, what it controls, and when to tune it.

## Configuration sources

Weir merges four layers in this precedence order (first match wins):

1. **CLI flags** — `--socket-path`, `--wab-dir`, `--batch-size`, etc.
   Useful for one-off runs and overriding the file for tests.
2. **Environment variables** — `WEIR_SOCKET_PATH`, `WEIR_WAB_DIR`,
   `WEIR_BATCH_SIZE`, etc. The `WEIR_` prefix + screaming-snake-case
   field name. Useful in container orchestrators (Kubernetes, ECS,
   systemd `Environment=`).
3. **TOML file** — default path `/etc/weir/weir.toml`, overridable via
   `--config <path>`. A missing file is **not** an error; the loader
   silently falls through to env + defaults. Unknown keys produce a
   `warn!` log but don't fail startup.
4. **Built-in defaults** — every option has one; running `weir-server`
   with no flags, no env vars, and no config file will start the
   daemon with the defaults documented here.

The merge happens once at startup. Config is not reloaded on SIGHUP
(see [Limitations](#limitations) below).

### TOML file structure

All options live under `[server]`:

```toml
[server]
socket_path = "/run/weir/weir.sock"
wab_dir     = "/var/lib/weir/wab"
batch_size  = 256
# ... etc
```

The reference example at `deploy/docker/weir.toml.example` shows every
option with its default value and the matching `WEIR_*` env var name.

## Option reference

Each option below lists: **type**, **default**, **valid range**,
**CLI flag**, **env var**, **TOML key**, **what it controls**, and
**when to tune**.

### Paths

#### `socket_path`

- **Type**: absolute path
- **Default**: `/run/weir/weir.sock`
- **Validation**: must be absolute, no `..` components, no null bytes.
  The parent directory must exist; weir does not create it.
- **CLI**: `--socket-path <path>`
- **Env**: `WEIR_SOCKET_PATH`
- **TOML**: `socket_path`

The Unix domain socket file the daemon listens on. Producers connect
here.

**Operational notes**:

- The parent directory **must be writable only by the daemon's user**
  (typically mode `0o700`). The bind sequence relies on this — see
  [socket-bind security analysis](../security/socket-bind.md) for the
  threat model.
- The socket file itself is always created with mode `0o600` via a
  hardened bind sequence (`bind_hardened` in
  `crates/weir-server/src/socket/mod.rs`).
- A stale socket at the path from a previous unclean shutdown is
  detected and replaced atomically. A non-socket file at the path
  causes startup to refuse.

**When to change the default**: production deployments using a
non-standard runtime directory (e.g. `/var/run/weir/`), or per-instance
socket paths when running multiple daemons on one host.

---

#### `wab_dir`

- **Type**: absolute path to an existing directory
- **Default**: `/var/lib/weir/wab`
- **Validation**: must be absolute, must already exist,
  `canonicalize()`-able, no `..` components, no null bytes.
- **CLI**: `--wab-dir <path>`
- **Env**: `WEIR_WAB_DIR`
- **TOML**: `wab_dir`

The write-ahead buffer directory. WAB segments and the dead-letter
sub-directory live here.

**Operational notes**:

- The daemon does not create the WAB directory (Postgres model — the
  operator creates the data directory at install time). Pre-create
  with:
  ```bash
  mkdir -p /var/lib/weir/wab
  chown weir:weir /var/lib/weir/wab
  chmod 0700 /var/lib/weir/wab
  ```
- Subdirectories: `<wab_dir>/00000000/` per shard, `<wab_dir>/dead_letter/`
  for permanently-rejected records, `<wab_dir>/quarantine/` for
  corrupted segments quarantined during crash recovery.
- Segment files are created mode `0o600`. At startup, every
  `.wab` / `.wab.sealed` / `.wab.confirmed` file is audited; any with
  permissions ≠ `0o600` increments
  `weir_wab_unexpected_mode_total` and logs a warning.

**Disk sizing**: depends on `batch_size`, `batch_deadline_ms`, sink
throughput, and `dead_letter_max_bytes`. For typical workloads,
provisioning ~5× the dead-letter cap is a safe starting point. The
WAB is rotated and reclaimed as the sink confirms records, so steady-
state size is bounded by transient backlog, not by total throughput.

---

### Sharding and worker pool

#### `shard_count`

- **Type**: usize
- **Default**: `1`
- **Range**: 1–256
- **CLI**: `--shard-count <n>`
- **Env**: `WEIR_SHARD_COUNT`
- **TOML**: `shard_count`

Number of WAB shards. Each shard has its own segment file, its own
flusher thread, and its own bridge thread. Records are routed to
shards by the worker pool (currently round-robin; shard-aware routing
is future work).

**When to tune**: increase only if fsync throughput is your bottleneck
and you have parallel disk bandwidth. On a single SSD, multiple shards
do not increase fsync throughput — they fight for the same disk queue.
On NVMe with high parallel write depth, 2–4 shards may improve aggregate
throughput. Default `1` is correct for most deployments.

---

#### `worker_count`

- **Type**: usize
- **Default**: `2`
- **Range**: 1–64
- **CLI**: `--worker-count <n>`
- **Env**: `WEIR_WORKER_COUNT`
- **TOML**: `worker_count`

Number of worker threads pulling from the global queue and batching
records into per-shard bridge channels. Workers are stateless and
pinned at startup; they exist primarily to absorb tokio
`spawn_blocking` slots without each spawn-blocking task fighting for a
queue lock.

**When to tune**: scale with `max_connections / 32` as a starting
point. The bottleneck is rarely worker count itself; if records
appear to back up at the worker layer (rising `weir_queue_depth`
without a corresponding rise in WAB fsync latency), worker count is
the right knob.

---

### Batching

The batch tuning data is in [batch-tuning.md](../benchmarks/batch-tuning.md);
an operator-facing tuning **guide** is planned for Phase 2. Defaults below
are the sweet spot found by the empirical sweep.

#### `batch_size`

- **Type**: usize
- **Default**: `256`
- **Range**: 1–100,000
- **CLI**: `--batch-size <n>`
- **Env**: `WEIR_BATCH_SIZE`
- **TOML**: `batch_size`

Maximum records per fsync batch. When the bridge thread has
accumulated this many records or `batch_deadline_ms` elapses,
whichever comes first, the batch is fsynced and the per-record acks
are returned.

**Effect**: larger batches amortise fsync cost across more records
(higher throughput) but increase tail latency for the first record in
each batch (waiting for siblings).

**When to tune**: the default `(256, 1ms)` is a balanced point for
mixed workloads. Pure-throughput workloads can push this to 1000+;
strict-latency workloads down to 64 with `batch_deadline_ms = 1`.
See [batch-tuning.md](../benchmarks/batch-tuning.md) for the empirical
landscape.

---

#### `batch_deadline_ms`

- **Type**: u64 (milliseconds)
- **Default**: `1`
- **Range**: 1–60,000
- **CLI**: `--batch-deadline-ms <n>`
- **Env**: `WEIR_BATCH_DEADLINE_MS`
- **TOML**: `batch_deadline_ms`

Maximum time the bridge thread waits to fill a batch before flushing
what it has. Caps tail latency in low-traffic regimes where
`batch_size` would not be reached for a long time.

**Effect**: lower values reduce p99 latency at low load; higher values
reduce CPU wakeups but increase tail latency on the first record after
quiet periods.

**When to tune**: leave at `1` unless you're running an extremely
high-volume, latency-insensitive workload where you want to amortise
the timer cost itself. See
[batch-tuning.md](../benchmarks/batch-tuning.md).

---

### Connection limits

#### `max_connections`

- **Type**: usize
- **Default**: `256`
- **Range**: 1–512
- **CLI**: `--max-connections <n>`
- **Env**: `WEIR_MAX_CONNECTIONS`
- **TOML**: `max_connections`

Maximum concurrent producer connections. Enforced by a
`tokio::sync::Semaphore`; connections beyond the cap are dropped
immediately (no Nack, the stream is closed).

**Hard ceiling**: weir uses `spawn_blocking` to push to the worker
queue, so `max_connections` must be `<=` the tokio runtime's blocking
thread pool size (default 512). Increasing the runtime's pool size
beyond 512 requires custom runtime construction, which weir does not
currently expose.

**When to tune**: production deployments behind a load balancer
typically run with the default. Single-tenant deployments with very
few producers can lower this to free resources; high-fanout deployments
can raise it up to the hard ceiling.

---

#### `connection_read_timeout_secs`

- **Type**: u64 (seconds)
- **Default**: `30`
- **Range**: 1–600
- **CLI**: `--connection-read-timeout-secs <n>`
- **Env**: `WEIR_CONNECTION_READ_TIMEOUT_SECS`
- **TOML**: `connection_read_timeout_secs`

How long a connection handler may sit in `read_exact` waiting for the
next byte before being dropped. **Slowloris guard**: without this, a
silent or extremely slow client could hold a semaphore permit
indefinitely, denying service to legitimate clients.

**What triggers the timeout**: any of the three `read_exact` sites in
the frame parser (header read, payload read, CRC read) blocked for
longer than this value. Drops are silent (no Nack sent — the client
isn't reading anyway) and increment
`weir_connection_idle_timeout_total`.

**When to tune**: lower for high-throughput / short-frame deployments
where 30s of silence indicates a problem (5–10s is typical). Raise for
deployments with intermittent producers that legitimately go quiet
between sends (60–300s).

**Operational note**: alert on
`rate(weir_connection_idle_timeout_total[5m]) > 0` to detect potential
slowloris activity or buggy clients. The full alert-recipe doc is
planned for Phase 2.

---

### Payload limits

#### `max_payload_bytes`

- **Type**: usize
- **Default**: `16777216` (16 MiB)
- **Range**: 1 to `MAX_PAYLOAD_HARD_CAP` (16 MiB, compile-time)
- **CLI**: `--max-payload-bytes <n>`
- **Env**: `WEIR_MAX_PAYLOAD_BYTES`
- **TOML**: `max_payload_bytes`

Maximum size of a single record payload. The hard cap of 16 MiB is
compiled into `weir-core` (`MAX_PAYLOAD_HARD_CAP`) and cannot be
exceeded at runtime; raising the config value above the hard cap is a
startup error.

**Effect**: records larger than this value are rejected with
`Nack(PayloadTooLarge)` before any heap allocation occurs.

**When to tune**: lower to tighten the DoS surface in environments
where you know record sizes are bounded (e.g. structured log records
≤ 64 KiB). Leave at the default for general workloads.

---

### Metrics endpoint

#### `metrics_port`

- **Type**: u16
- **Default**: `9185`
- **Range**: 1–65535
- **CLI**: `--metrics-port <n>`
- **Env**: `WEIR_METRICS_PORT`
- **TOML**: `metrics_port`

TCP port for the Prometheus `/metrics` HTTP endpoint.

**Bind address**: the metrics server binds to `0.0.0.0:{metrics_port}`
(not localhost). This is intentional — it makes the endpoint
accessible from container hosts and sidecars. Restrict access via
firewall rules or a `--publish 127.0.0.1:9185:9185` port mapping in
Docker; **do not** rely on the bind address as a security boundary.

**When to tune**: change if 9185 conflicts with another service, or
to allow multiple weir instances on one host.

---

### Shutdown

#### `shutdown_timeout_secs`

- **Type**: u64 (seconds)
- **Default**: `30`
- **Range**: 1+ (no upper cap)
- **CLI**: `--shutdown-timeout-secs <n>`
- **Env**: `WEIR_SHUTDOWN_TIMEOUT_SECS`
- **TOML**: `shutdown_timeout_secs`

After receiving SIGTERM (or Ctrl-C), how long the daemon waits for
in-flight connections to finish before forcibly closing them.

**Effect**: clean shutdown lets connected producers complete their
current Push frame and receive an Ack. Aborted shutdown drops the
connection without an Ack — the producer will not know whether the
record was committed (it may have been, since the WAB write may
already be on disk).

**When to tune**: deployments with long-running batched producers
should raise this to match their batch flush interval. Container
orchestrators usually have their own termination-grace setting
(Kubernetes `terminationGracePeriodSeconds`, ECS `stopTimeout`) —
weir's `shutdown_timeout_secs` should be set ≤ the orchestrator
grace minus a few seconds, so the daemon's own cleanup finishes
before the orchestrator SIGKILLs.

---

### Dead-letter queue

When the sink permanently rejects a record (transient retries
exhausted, or sink classifies as permanent), the record is appended
to a dead-letter segment under `<wab_dir>/dead_letter/`. The two
options below cap that directory's growth.

#### `dead_letter_max_bytes`

- **Type**: u64 (bytes)
- **Default**: `1073741824` (1 GiB)
- **Range**: 1+ (a warning is logged below 1 MiB)
- **CLI**: `--dead-letter-max-bytes <n>`
- **Env**: `WEIR_DEAD_LETTER_MAX_BYTES`
- **TOML**: `dead_letter_max_bytes`

Maximum total size of the dead-letter directory. When a new
dead-letter write would push the directory over this cap, the drain
transitions to `BlockedDeadLetterFull` and pauses **all** drain
activity (including for healthy records) until an operator removes
files or raises the cap.

**Why drain blocks entirely**: the alternative (drop the new
dead-letter record) silently loses data that the sink already
classified as un-retriable. Blocking surfaces the problem in metrics
(`weir_drain_state{state="blocked_dead_letter_full"} = 1`) and forces
operator attention.

**When to tune**: bigger for deployments where the operator response
loop is slow (alerts → human → mitigation can be 15+ minutes); smaller
when the dead-letter directory shares a disk with the WAB and you
want a hard cap before WAB writes fail.

**Operational note**: alert on `weir_drain_state{state="blocked_dead_letter_full"} == 1`.
Alert-recipe details land with the Phase 2 observability doc.

---

#### `dead_letter_check_interval_secs`

- **Type**: u64 (seconds)
- **Default**: `30`
- **Range**: 1–3600
- **CLI**: `--dead-letter-check-interval-secs <n>`
- **Env**: `WEIR_DEAD_LETTER_CHECK_INTERVAL_SECS`
- **TOML**: `dead_letter_check_interval_secs`

While in `BlockedDeadLetterFull`, how often the drain wakes to
re-scan the dead-letter directory size. The re-scan catches operator-
initiated deletions (lower disk usage → unblock) and external growth
(some other process writing into the dir → reject the unblock).

**When to tune**: shorter if you want faster recovery after operator
intervention; longer if the dead-letter directory is on slow storage
and rescans are expensive (each rescan is a `readdir` + per-file
`stat`).

---

### Sink selection

Weir ships with two built-in sinks. The `noop` sink is the default; the
`http` sink POSTs each record to a configurable URL with transient /
permanent error classification.

#### `sink_type`

- **Type**: string (`"noop"` or `"http"`)
- **Default**: `"noop"`
- **CLI**: `--sink-type <value>`
- **Env**: `WEIR_SINK_TYPE`
- **TOML**: `sink_type`

Which built-in sink to run.

- `"noop"` — accepts every record, forwards nothing. Use for soak-testing
  the daemon pipeline without a downstream, or as a known-good sink in
  integration tests.
- `"http"` — POSTs records to `sink_url`. See classification rules below.

**When to change**: set to `"http"` once a real downstream is available;
leave at `"noop"` until then.

---

#### `sink_url`

- **Type**: URL string
- **Default**: none (required when `sink_type = "http"`)
- **Validation**: parsed at startup; invalid URLs fail fast.
- **CLI**: `--sink-url <url>`
- **Env**: `WEIR_SINK_URL`
- **TOML**: `sink_url`

The HTTP endpoint that receives one POST per record. The body is the raw
payload bytes; `Content-Type` is `application/octet-stream`. The endpoint
is expected to return:

- **2xx** for accepted records → committed
- **4xx (except 408, 429)** for rejected records → dead-lettered
- **408, 429, 5xx** for retryable failures → drain retries the whole
  segment with exponential backoff (up to `MAX_RETRIES`)

Network-layer failures (connect refused, DNS failure, timeout) are
treated as transient.

**Idempotency**: the drain guarantees at-least-once delivery per
segment, so the endpoint **must** handle duplicates gracefully
(idempotency key, upsert, dedup). This is documented in the `Sink`
trait and applies to every sink, but is especially relevant for HTTP
endpoints where retries cross a network boundary.

---

#### `sink_timeout_secs`

- **Type**: u64 (seconds)
- **Default**: `10`
- **Range**: 1–300
- **CLI**: `--sink-timeout-secs <n>`
- **Env**: `WEIR_SINK_TIMEOUT_SECS`
- **TOML**: `sink_timeout_secs`

Per-request timeout for the HTTP sink. Applies to the whole request
(connect + send + receive). On timeout, the request is classified as a
transient transport error and the drain retries.

**When to tune**: lower if the endpoint is local / sub-millisecond and a
stuck request shouldn't block other records; higher for slow endpoints
that legitimately take seconds (some logging ingesters do).

---

#### `sink_max_batch_size`

- **Type**: usize
- **Default**: `100`
- **Range**: 1–10000
- **CLI**: `--sink-max-batch-size <n>`
- **Env**: `WEIR_SINK_MAX_BATCH_SIZE`
- **TOML**: `sink_max_batch_size`

Maximum records the drain hands to a single `Sink::commit` call. The
HTTP sink iterates records inside `commit` (one POST per record), so
this also caps the longest contiguous run of POSTs before the drain
re-checks its shutdown signal and dead-letter state.

**When to tune**: lower for endpoints that prefer many small calls; the
default 100 is a balanced point for most deployments.

---

#### `WEIR_SINK_BEARER_TOKEN` (env-only)

- **Type**: string
- **Default**: none
- **Env**: `WEIR_SINK_BEARER_TOKEN`
- **TOML**: deliberately **not** supported

Optional bearer token sent as `Authorization: Bearer <token>` with each
HTTP-sink request.

**Why env-only**: bearer tokens are secrets. Putting them in a config
file invites accidental commits to git, inclusion in container image
layers, and exposure via `cat /etc/weir/weir.toml` to anyone who can
read the file. Env vars don't have those failure modes by default.
Containers should source the token from a secrets manager (Kubernetes
`Secret`, AWS Secrets Manager, HashiCorp Vault) into the env at start.

The token is also redacted from `HttpSinkConfig`'s `Debug` impl so it
never reaches a log line via accidental `?config` interpolation.

---

### Logging

#### `log_level`

- **Type**: string
- **Default**: `"info"`
- **Valid values**: `trace`, `debug`, `info`, `warn`, `error`, or any
  `tracing-subscriber::EnvFilter` directive (e.g.
  `"weir_server=debug,info"` to set per-module levels).
- **CLI**: `--log-level <level>`
- **Env**: `WEIR_LOG_LEVEL` (also `RUST_LOG`, which `EnvFilter` reads
  natively as a fallback)
- **TOML**: `log_level`

The `tracing-subscriber` `EnvFilter` directive used to initialise the
logging subscriber.

**Effect**: `info` is the production default — startup, shutdown,
recovery events, and warnings are visible; per-record activity is
not. `debug` adds per-connection events and queue activity; `trace`
adds per-frame parser events.

**When to tune**: `debug` while diagnosing a problem; back to `info`
once resolved. `trace` is verbose enough to materially impact
throughput; don't leave it on in production.

---

## Example minimal config

The smallest useful TOML (everything else defaults):

```toml
[server]
wab_dir = "/var/lib/weir/wab"
```

The daemon will:

- Listen on `/run/weir/weir.sock`
- Use `/var/lib/weir/wab` for WAB segments
- Serve metrics on `0.0.0.0:9185`
- Use the (256, 1ms) batch defaults
- Cap connections at 256, payload at 16 MiB
- Drop idle connections after 30s
- Use the `noop` sink (accepts all records, forwards nothing)
- Run with log level `info`

For container deployments, the same config can come entirely from env
vars with no file:

```bash
WEIR_WAB_DIR=/var/lib/weir/wab \
weir-server
```

## Example production config

A more typical production TOML, with comments explaining each deviation:

```toml
[server]
# Standard paths.
socket_path = "/run/weir/weir.sock"
wab_dir     = "/var/lib/weir/wab"

# Two shards on this NVMe; measured 1.4× throughput gain over single shard.
shard_count  = 2
worker_count = 4

# Latency-tuned: small batches, tight deadline.
batch_size        = 64
batch_deadline_ms = 1

# Match the LB connection cap.
max_connections              = 384
connection_read_timeout_secs = 10

# This deployment uses structured log records; cap accordingly to limit DoS surface.
max_payload_bytes = 1048576  # 1 MiB

# Metrics on a non-standard port to avoid colliding with node_exporter.
metrics_port = 9186

# Long-running batch producers; match the orchestrator's grace period (45s).
shutdown_timeout_secs = 40

# Bigger dead-letter cap; operator response loop can be 30+ minutes.
dead_letter_max_bytes           = 5_368_709_120  # 5 GiB
dead_letter_check_interval_secs = 60

# Forward records to an internal ingest endpoint via HTTP.
# Bearer token comes from $WEIR_SINK_BEARER_TOKEN in the unit's
# Environment= file, not from this config.
sink_type           = "http"
sink_url            = "https://ingest.internal.example.com/weir"
sink_timeout_secs   = 5
sink_max_batch_size = 200

log_level = "info"
```

## Limitations

- **No hot reload**: config is read once at startup. SIGHUP is not
  handled. Configuration changes require a restart.
- **No per-shard tuning**: `batch_size` and `batch_deadline_ms` apply
  uniformly to every shard. Workloads where shards have very different
  load profiles cannot tune them independently.
- **No CLI override of `[server]` table name**: the TOML file must use
  `[server]` exactly; this is not configurable.

## See also

- [Quickstart](../getting-started/quickstart.md) — the fastest path to a
  running daemon, with a minimal config inline.
- *Tuning guide* — planned for Phase 2; operator-facing guide on
  picking values for your workload (vs the data dump in batch-tuning).
- *Observability* — planned for Phase 2; metrics catalogue with alert
  thresholds and Grafana dashboard JSON.
- [Batch tuning data](../benchmarks/batch-tuning.md) — the empirical
  sweep behind the `(256, 1ms)` default.
- [`deploy/docker/weir.toml.example`](https://github.com/miki-przygoda/weir/blob/main/deploy/docker/weir.toml.example)
  — every option with its default and matching env var, kept in sync
  with this reference.
