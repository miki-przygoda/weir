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

#### `peer_uid_check`

- **Type**: bool
- **Default**: `true`
- **CLI**: `--peer-uid-check <true|false>`
- **Env**: `WEIR_PEER_UID_CHECK`
- **TOML**: `peer_uid_check`

The application-level access control on the Unix socket path. When
enabled (the default), the accept loop reads each peer's effective uid
(`SO_PEERCRED` on Linux, `getpeereid` on macOS) and **refuses any
connection whose peer euid does not match the daemon's** — closing the
socket before any frame is read. It is **fail-closed**: if the peer
credential lookup itself fails, the connection is refused. Every refusal
increments `weir_connection_rejected_peer_uid_total`.

This is the narrow application-level complement to the socket file's
`0o600` permissions: even if the socket's permissions or parent directory
are looser than intended, a same-host process running as a different user
cannot connect. It applies only to the Unix transport; the TCP + mutual-TLS
listener authenticates clients by certificate instead.

**When to change the default**: set to `false` only when a trusted helper
must connect as a *different* uid than the daemon (e.g. a sidecar under a
separate service account) and you are deliberately relying on socket
permissions alone. Leaving it on is strongly recommended.

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
- Subdirectories: `<wab_dir>/shard_NN/` per shard (zero-padded index,
  e.g. `shard_00`), `<wab_dir>/dead_letter/` for permanently-rejected
  records, `<wab_dir>/quarantine/` for corrupted segments quarantined
  during crash recovery.
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

Number of WAB shards. Each shard has its own segment file and its own
flusher thread. Each connection is pinned to a shard round-robin at
accept time (`connection_counter % shard_count`); every record on that
connection goes to the same shard. Per-record shard-aware routing is
future work.

**When to tune**: increase only if fsync throughput is your bottleneck
and you have parallel disk bandwidth. On a single SSD, multiple shards
do not increase fsync throughput — they fight for the same disk queue.
On NVMe with high parallel write depth, 2–4 shards may improve aggregate
throughput. Default `1` is correct for most deployments.

The daemon emits a startup advisory if `shard_count` / `worker_count`
looks unusual for the host's core count. See
[Agent count vs cores](../benchmarks/agent-count-tuning.md) for the
empirical sweep and the heuristic the advisory is based on; the rule
of thumb is `max(2, cores - 2) / 2`.

---

#### `worker_count`

- **Type**: usize
- **Default**: `min(shard_count, 64)` — the resolved `shard_count`, capped at the field max
- **Range**: 1–64
- **CLI**: `--worker-count <n>`
- **Env**: `WEIR_WORKER_COUNT`
- **TOML**: `worker_count`

Number of worker threads pulling from the global queue and batching
records into per-shard batch channels. Workers are stateless and
pinned at startup; they exist primarily to absorb tokio
`spawn_blocking` slots without each spawn-blocking task fighting for a
queue lock.

The default was changed in 0.9.0 from the hard-coded `2` to `shard_count`.
With the old default, the standard single-shard config had `worker_count=2`
but only one shard, so worker 1 was permanently idle. Defaulting to
`shard_count` removes the idle worker and keeps the balanced invariant
(`worker_count == shard_count`) out of the box.

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

Maximum records per fsync batch. When the flusher thread has
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

Maximum time the flusher thread waits to fill a batch before flushing
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

#### `wab_segment_max_bytes`

- **Type**: u64 (bytes)
- **Default**: `268435456` (256 MiB)
- **Range**: 4096 – 4294967296 (4 KiB – 4 GiB)
- **CLI**: `--wab-segment-max-bytes <n>`
- **Env**: `WEIR_WAB_SEGMENT_MAX_BYTES`
- **TOML**: `wab_segment_max_bytes`

The size threshold at which the WAB flusher seals the active segment
and opens a fresh one. The sealed segment is forwarded to the drain
for sink commit; until a segment seals, its records are durable on
disk but invisible to the sink.

**Effect**: smaller values trigger more frequent drain → sink activity
and faster failure isolation (a corrupt segment only affects records
in that window); larger values reduce the per-rotation overhead and
batch more records into each `Sink::commit` call. The 256 MiB default
balances both for production workloads.

**When to tune**: lower for storage-constrained deployments (e.g.
embedded boxes with ≤ 1 GiB available for WAB), or in tests that
need to demonstrate sink-side behaviour without pushing 256 MiB of
data. Higher if you have spare disk and want to amortise the seal
cost over more records.

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

**Memory sizing**: in-flight payload buffers are sized to the actual
frame, but there is no global byte budget across connections, so the
worst-case transient receive memory is `max_connections × max_payload_bytes`
(with the defaults, 256 × 16 MiB ≈ 4 GiB if every connection streams a
max-size payload at once). On memory-constrained hosts, lower
`max_payload_bytes` and/or `max_connections` accordingly. See the
[threat model](../security/threat-model.md).

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

**When to tune**: change if 9185 conflicts with another service, or
to allow multiple weir instances on one host.

#### `metrics_bind`

- **Type**: IpAddr
- **Default**: `127.0.0.1` (localhost only)
- **CLI**: `--metrics-bind <addr>`
- **Env**: `WEIR_METRICS_BIND`
- **TOML**: `metrics_bind`

Address the metrics server binds to. The default is localhost-only, so
the endpoint is not exposed off-box without an explicit decision. To
scrape from another host, sidecar, or container, set `0.0.0.0` (or a
specific interface) and restrict access via firewall rules or a
`--publish 127.0.0.1:9185:9185` port mapping in Docker. A `0.0.0.0`
bind is **not** a security boundary — `/metrics` is unauthenticated.

#### `metrics_max_connections`

- **Type**: usize
- **Default**: `8`
- **Range**: 1–1024
- **CLI**: `--metrics-max-connections <n>`
- **Env**: `WEIR_METRICS_MAX_CONNECTIONS`
- **TOML**: `metrics_max_connections`

Maximum number of concurrent scrape connections to the `/metrics`
endpoint. Bounds the resource cost of a misbehaving or hostile
scraper; the default of 8 is generous for normal Prometheus scraping.

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

Weir ships with five built-in sinks. The **default build** compiles in `noop`,
`http`, `mysql`, and `postgres`; `clickhouse` is compiled in only when you build
with the opt-in `clickhouse-sink` Cargo feature (a default binary rejects
`sink_type = "clickhouse"` with a clear "built without it" error):

| `sink_type` | What it does | When to use |
|------------|--------------|------|
| `"noop"` | accepts every record, forwards nothing | soak-testing the daemon pipeline; integration tests |
| `"http"` | POSTs each record to `sink_url`; up to `sink_http_concurrency` POSTs in flight per batch | endpoints that already accept POST bodies |
| `"mysql"` | writes a whole batch with one multi-row `INSERT` | the IOPS-compression downstream: N records → 1 statement |
| `"postgres"` | Postgres counterpart to `"mysql"`; multi-row INSERT with `ON CONFLICT DO NOTHING` | same IOPS-compression story when the downstream is Postgres |
| `"clickhouse"` | one HTTP `INSERT … FORMAT RowBinary` per batch with a sha256 `insert_deduplication_token` (**requires the `clickhouse-sink` build feature**) | bulk inserts into ClickHouse with replay-safe dedup (see the ClickHouse sink section below) |

#### `sink_type`

- **Type**: string (`"noop"`, `"http"`, `"mysql"`, `"postgres"`, or `"clickhouse"`)
- **Default**: `"noop"`
- **CLI**: `--sink-type <value>`
- **Env**: `WEIR_SINK_TYPE`
- **TOML**: `sink_type`

**When to change**: set to `"http"`, `"mysql"`, `"postgres"`, or
`"clickhouse"` once a real downstream is available; leave at `"noop"`
until then.

---

#### `sink_url`

- **Type**: URL string
- **Default**: none (required when `sink_type` is `"http"`, `"mysql"`, `"postgres"`, or `"clickhouse"`)
- **Validation**: parsed at startup; invalid URLs fail fast.
- **CLI**: `--sink-url <url>`
- **Env**: `WEIR_SINK_URL`
- **TOML**: `sink_url`

For `sink_type = "http"`: the endpoint that receives one POST per
record. For `sink_type = "mysql"`: the `mysql://user:password@host:port/db`
connection URL. For `sink_type = "postgres"`: the
`postgres://user:password@host:port/database` connection URL. When the URL
carries credentials, **prefer setting it via `WEIR_SINK_URL` rather than the
TOML file** so the password is not written to disk — `sink_url` is accepted in
TOML, but a credential-bearing URL placed there is stored in plaintext.

For HTTP, the body is the raw payload bytes; `Content-Type` is
`application/octet-stream`. The endpoint is expected to return:

- **2xx** for accepted records → committed
- **4xx (except 408, 429)** for rejected records → dead-lettered
- **408, 429, 5xx** for retryable failures → drain retries the whole
  segment with exponential backoff (up to `MAX_RETRIES`)

Network-layer failures (connect refused, DNS failure, timeout) are
treated as transient.

**Retry-After honoring**: when the endpoint returns a transient
status (408, 429, or 5xx) with a `Retry-After: <seconds>` header,
the drain uses that value as the next retry delay instead of its
exponential-backoff default. Only the delay-seconds form is parsed
in v0; HTTP-date form is silently ignored (no header parsed → drain
uses its default). The delay is capped at 5 minutes regardless of
header value so a misbehaving endpoint can't stall the drain.

**Idempotency**: the drain guarantees at-least-once delivery per
segment, so the endpoint **must** handle duplicates gracefully.
weir helps by sending `Idempotency-Key: sha256:<hex>` by default
(see `sink_send_idempotency_key` below); endpoints that prefer
their own dedup scheme can disable the header.

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
HTTP sink sends one POST per record inside `commit` (up to
`sink_http_concurrency` in flight), so this also caps the longest run
of POSTs before the drain re-checks its shutdown signal and
dead-letter state.

**When to tune**: lower for endpoints that prefer many small calls; the
default 100 is a balanced point for most deployments.

---

#### `sink_send_idempotency_key`

- **Type**: bool
- **Default**: `true`
- **CLI**: `--sink-send-idempotency-key <bool>`
- **Env**: `WEIR_SINK_SEND_IDEMPOTENCY_KEY`
- **TOML**: `sink_send_idempotency_key`

Whether to send `Idempotency-Key: sha256:<lowercase-hex>` with each HTTP
sink request.

**Why it matters**: the drain's at-least-once delivery contract means
records can be re-POSTed if a transient failure mid-batch causes the
drain to retry the whole segment. The endpoint needs to dedupe to
avoid double-writes. The key is a pure hash of the payload, so the
same payload always produces the same key — exactly the property
endpoint-side dedup needs.

**Format**: `sha256:` prefix + 64-char lowercase hex digest. The
prefix lets endpoints distinguish weir's scheme from other key sources
and lets us swap algorithms in future (e.g. `blake3:...`) without
breaking parsers.

**When to disable**: only if the endpoint can't tolerate the extra
header (strict CORS, header allow-lists). In that case the endpoint
must implement its own dedup — usually by hashing the body server-side.

---

#### `sink_http_concurrency`

- **Type**: usize
- **Default**: `8`
- **Range**: 1–1024
- **CLI**: `--sink-http-concurrency <n>`
- **Env**: `WEIR_SINK_HTTP_CONCURRENCY`
- **TOML**: `sink_http_concurrency`

How many per-record POSTs the HTTP sink keeps in flight per `commit`
batch. The drain runs on a single thread, but the POSTs are async, so
up to this many overlap their network round-trips — collapsing a
segment's serial latency (1000 records × one RTT each would otherwise
serialise to ~1000 RTTs). The protocol is unchanged: still one POST per
record, each with its own `Idempotency-Key`, and dead-lettering stays
per-record. Results are committed in submission order.

**When to tune**: raise it for high-latency endpoints where the RTT
dominates; lower it (or set `1` for fully serial) for endpoints that
rate-limit aggressively or can't handle concurrent connections.

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

### MySQL sink

The MySQL sink writes a whole batch with one multi-row `INSERT` statement.
This is the IOPS-compression sink: N records pushed into the daemon
become one prepared statement on one server-side commit.

**Schema contract**: the sink does not auto-create the target table.
Provision it before pointing weir at the database. The minimal table:

```sql
CREATE TABLE weir_records (
  id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  payload VARBINARY(16384) NOT NULL,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_payload (payload(255))
);
```

The `UNIQUE` constraint pairs with the default `sink_mysql_insert_mode =
"ignore"`: at-least-once retries that re-insert a payload are silently
dropped by the server, no consumer-side dedup required.

#### `sink_mysql_table`

- **Type**: identifier (`[A-Za-z_][A-Za-z0-9_]{0,63}`)
- **Default**: `"weir_records"`
- **CLI**: `--sink-mysql-table <name>`
- **Env**: `WEIR_SINK_MYSQL_TABLE`
- **TOML**: `sink_mysql_table`

Target table for the multi-row INSERT. Validated at startup; characters
outside the allowed identifier set fail fast (the sink does not escape
identifiers — it validates them, which is a strict subset of MySQL's
backtick-quoted form and leaves zero SQL-injection surface).

#### `sink_mysql_column`

- **Type**: identifier (same rules as `sink_mysql_table`)
- **Default**: `"payload"`
- **CLI**: `--sink-mysql-column <name>`
- **Env**: `WEIR_SINK_MYSQL_COLUMN`
- **TOML**: `sink_mysql_column`

Column that receives the payload bytes. Must be a `VARBINARY` or `BLOB`
column wide enough to hold the largest payload weir accepts (capped
elsewhere by `max_payload_bytes`).

#### `sink_mysql_insert_mode`

- **Type**: string (`"ignore"` or `"plain"`)
- **Default**: `"ignore"`
- **CLI**: `--sink-mysql-insert-mode <mode>`
- **Env**: `WEIR_SINK_MYSQL_INSERT_MODE`
- **TOML**: `sink_mysql_insert_mode`

How to phrase the INSERT statement.

- `"ignore"` → `INSERT IGNORE INTO ...`. Duplicate-key errors are
  silently dropped by the server. The recommended default: pair with a
  `UNIQUE` constraint on the payload (or a hash of it) so crash-recovery
  retries are idempotent without consumer-side dedup.
- `"plain"` → `INSERT INTO ...`. Duplicates surface as `ER_DUP_ENTRY`
  (code 1062) and are classified as transient — the drain retries the
  segment. Use only if duplicate rows in the target table are tolerable.

#### Error classification

| MySQL error | Classification | What weir does |
|------------|----------------|----------------|
| connection / pool / IO failures | transient | retry the segment |
| 1205 `ER_LOCK_WAIT_TIMEOUT` | transient | retry |
| 1213 `ER_LOCK_DEADLOCK` | transient | retry |
| 1290 `ER_OPTION_PREVENTS_STATEMENT` (e.g. `--read-only`) | transient | retry |
| 1317 `ER_QUERY_INTERRUPTED` | transient | retry |
| 1062 `ER_DUP_ENTRY` (Plain mode only) | transient | retry |
| 1064 `ER_PARSE_ERROR`, 1146 `ER_NO_SUCH_TABLE`, 1054 `ER_BAD_FIELD_ERROR`, 1045/1044 access denied | permanent | dead-letter the batch with the server-supplied message |
| anything else | permanent | dead-letter the batch |

#### Credentials and TLS

The MySQL URL contains credentials. **Always** supply it via
`WEIR_SINK_URL` rather than the TOML file. The `MySqlSinkConfig` Debug
impl redacts the password before logging, but a TOML file on disk has
no such protection. weir uses rustls — no system OpenSSL dependency —
so TLS to a managed database "just works" with `mysql+tls://...` URLs.

---

### Sink: Postgres (`sink_type = "postgres"`)

The Postgres sink is the direct counterpart to the MySQL sink — same
multi-row INSERT shape, `ON CONFLICT DO NOTHING` in place of
`INSERT IGNORE` for idempotency, SQLSTATE-based error classification in
place of MySQL error codes. Driver is `tokio-postgres` with a small
connection pool (`deadpool-postgres`, max 4 connections).

#### Reference schema

```sql
CREATE TABLE weir_records (
    id BIGSERIAL PRIMARY KEY,
    payload BYTEA NOT NULL,
    payload_sha256 BYTEA GENERATED ALWAYS AS (sha256(payload)) STORED,
    UNIQUE (payload_sha256)
);
```

The `UNIQUE (payload_sha256)` constraint pairs with the default
`sink_postgres_insert_mode = "on_conflict_do_nothing"` so crash-recovery
retries are idempotent: duplicate inserts are silently dropped by the
server, no consumer-side dedup required.

#### `sink_postgres_table`

- **Type**: identifier (`[A-Za-z_][A-Za-z0-9_]{0,62}`, ≤ 63 chars to fit
  Postgres's `NAMEDATALEN - 1` limit)
- **Default**: `"weir_records"`
- **Validation**: at startup; rejected configurations include illegal
  characters, leading digits, length > 63.
- **CLI**: `--sink-postgres-table <name>`
- **Env**: `WEIR_SINK_POSTGRES_TABLE`
- **TOML**: `sink_postgres_table`

The strict identifier rule means the sink builds SQL via `format!` with
no escaping logic — there is no SQL injection vector through this knob.

---

#### `sink_postgres_column`

- **Type**: identifier (same rules as `sink_postgres_table`)
- **Default**: `"payload"`
- **CLI**: `--sink-postgres-column <name>`
- **Env**: `WEIR_SINK_POSTGRES_COLUMN`
- **TOML**: `sink_postgres_column`

---

#### `sink_postgres_insert_mode`

- **Type**: string (`"on_conflict_do_nothing"` or `"plain"`)
- **Default**: `"on_conflict_do_nothing"`
- **CLI**: `--sink-postgres-insert-mode <mode>`
- **Env**: `WEIR_SINK_POSTGRES_INSERT_MODE`
- **TOML**: `sink_postgres_insert_mode`

| Mode | INSERT phrasing | Idempotent under crash recovery? |
|------|------------------|----------------------------------|
| `on_conflict_do_nothing` (default) | `INSERT INTO t (col) VALUES ($1), ($2), … ON CONFLICT DO NOTHING` | yes, if the table has a `UNIQUE` constraint |
| `plain` | `INSERT INTO t (col) VALUES ($1), ($2), …` | no — duplicates surface as SQLSTATE `23505` and trigger a transient-retry loop until the operator removes the dup manually |

#### Error classification

| Postgres SQLSTATE | Classification | What weir does |
|--------------------|----------------|----------------|
| connection / pool / IO failures | transient | retry the segment |
| `40P01` `deadlock_detected` | transient | retry |
| `55P03` `lock_not_available` | transient | retry |
| `57014` `query_canceled` | transient | retry |
| `57P01` `admin_shutdown`, `57P02` `crash_shutdown`, `57P03` `cannot_connect_now` | transient | retry |
| `23505` `unique_violation` (Plain mode only) | transient | retry |
| `42P01` `undefined_table`, `42703` `undefined_column`, `42601` `syntax_error`, `28P01` `invalid_password` | permanent | dead-letter the batch with the server-supplied message |
| anything else | permanent | dead-letter the batch |

#### Credentials and TLS

The Postgres URL contains credentials. **Always** supply it via
`WEIR_SINK_URL` rather than the TOML file. The `PostgresSinkConfig`
Debug impl redacts the password before logging.

**TLS support: opt-in via `?sslmode=require` in the URL.** The Postgres
sink uses `tokio-postgres-rustls` (webpki-roots, aws-lc-rs) when the
URL's `sslmode` query parameter is `require`, and `NoTls` otherwise.
Opt-in semantics rather than auto-detect: an upgrade does not silently
enable TLS on a previously-cleartext deployment.

```toml
# Cleartext (default; safe for private networks):
WEIR_SINK_URL=postgres://user:pw@10.0.0.5:5432/weir

# TLS required:
WEIR_SINK_URL=postgres://user:pw@db.example.com:5432/weir?sslmode=require
```

Cert verification uses the Mozilla root store bundled via
`webpki-roots` — no host-system CA configuration required. The aws-lc-rs
crypto provider is explicitly selected (the same one weir uses for
mysql+tls and for outbound reqwest TLS), keeping the binary's
code-signing surface at one provider.

For cleartext deployments without operator control over `sslmode` (e.g.
a managed Postgres URL that omits it), the tokio-postgres default
`sslmode=prefer` is treated as cleartext — `sslmode=require` is the
explicit opt-in. A TLS-terminating proxy (stunnel, ghostunnel,
pgbouncer with TLS upstream) remains a valid option for deployments
that need certificate-pinning or client-auth — neither is wired up
by the in-process connector yet.

---

### Sink: ClickHouse (`sink_type = "clickhouse"`)

Requires the `clickhouse-sink` build feature. The sink sends one HTTP
`INSERT INTO {database}.{table} ({column}) FORMAT RowBinary` request per
batch to ClickHouse's HTTP interface (default `:8123`), with the batch
encoded as length-prefixed bytes into a single `String` column. N records →
one request → one ClickHouse block — the same IOPS compression as the SQL
sinks. `sink_url` carries `http://[user:password@]host:8123` (credentials
sent as HTTP basic auth, redacted in logs).

#### `sink_clickhouse_database`

- **Type**: string · **Default**: `default`
- **CLI**: `--sink-clickhouse-database` · **Env**: `WEIR_SINK_CLICKHOUSE_DATABASE` · **TOML**: `sink_clickhouse_database`

#### `sink_clickhouse_table`

- **Type**: string · **Default**: `weir_records`
- **CLI**: `--sink-clickhouse-table` · **Env**: `WEIR_SINK_CLICKHOUSE_TABLE` · **TOML**: `sink_clickhouse_table`

#### `sink_clickhouse_column`

- **Type**: string · **Default**: `payload`
- **CLI**: `--sink-clickhouse-column` · **Env**: `WEIR_SINK_CLICKHOUSE_COLUMN` · **TOML**: `sink_clickhouse_column`

#### Idempotency (dedup token)

The drain is at-least-once per segment, so a crash mid-commit replays the
batch. ClickHouse has no `ON CONFLICT`; instead the sink sends a
deterministic `insert_deduplication_token = sha256(batch)` on each insert,
so a replayed byte-identical batch is deduplicated by ClickHouse **provided
the target table uses a dedup-capable engine** — a `Replicated*MergeTree`
(where `insert_deduplicate` is on by default) or a `MergeTree` with
`non_replicated_deduplication_window` set. Mind the dedup window (default
last ~100 blocks). If the engine isn't dedup-capable, the token is harmless
and dedup falls back to your table design.

Reference schema:

```sql
CREATE TABLE weir_records (payload String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;
```

#### Error classification

| ClickHouse response | Classification | What weir does |
|---------------------|----------------|----------------|
| connect / DNS / network reset | transient | retry the segment |
| HTTP 5xx | transient | retry |
| request timeout (`sink_timeout_secs`) | timeout (transient) | retry |
| HTTP 4xx (bad query, auth, unknown table) | permanent | dead-letter the batch |

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
- Serve metrics on `127.0.0.1:9185` (localhost only; set `metrics_bind` to expose)
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

## TCP + mutual TLS

> **Requires `--features tls`** — the `tls` feature on `weir-server` (and
> `weir-client`) is **off by default**. A build without `--features tls` has
> no TCP listener and the five keys below are not recognised. Enabling
> `tcp_bind` without building with the feature is a startup error.

TLS is **mandatory** on the TCP path. Setting `tcp_bind` without a valid TLS
configuration (or without building with `--features tls`) is a **fatal startup
error** — weir never exposes a plaintext TCP socket. The three cert-path keys
are **required** whenever `tcp_bind` is set; omitting any one of them is a
startup error.

The Unix and TCP listeners share **one** global connection semaphore sized
`max_connections`. The total concurrent connections across **both** transports
is bounded by `max_connections`, not 2×. Tune accordingly.

For a full operator guide including CA setup, cert rotation, and monitoring,
see [TCP + mutual TLS](tcp-mtls.md).

#### `tcp_bind`

- **Type**: socket address, e.g. `0.0.0.0:7100` or `[::]:7100`
- **Default**: none (TCP listener disabled; Unix socket only)
- **CLI**: `--tcp-bind <addr>`
- **Env**: `WEIR_TCP_BIND`
- **TOML**: `tcp_bind`

The address and port for the TCP listener. When unset (the default), the
daemon operates Unix-socket-only and the four TLS keys below are ignored.
When set, all four TLS keys become required; missing any one is a startup
error.

**Operational notes**:

- Bind to `0.0.0.0:7100` to accept from any interface, or to a specific
  interface address to limit exposure. Restrict inbound access with host
  firewall rules independent of the bind address.
- The TCP listener runs concurrently with the Unix socket and feeds the
  same pipeline.
- The connection cap is shared with the Unix listener; see `max_connections`.

---

#### `tls_cert_path`

- **Type**: absolute path to a PEM-encoded TLS server certificate (may be a
  full chain)
- **Default**: none (required when `tcp_bind` is set)
- **CLI**: `--tls-cert <path>`
- **Env**: `WEIR_TLS_CERT`
- **TOML**: `tls_cert_path`

Path to the server's TLS certificate file. The file must be readable by the
daemon at startup and on every SIGHUP reload.

**When to tune**: update the path when rotating to a new certificate chain.
After updating, send SIGHUP to reload TLS material without dropping
connections (see SIGHUP cert rotation in [TCP + mutual TLS](tcp-mtls.md)).

---

#### `tls_key_path`

- **Type**: absolute path to the PEM-encoded private key for `tls_cert_path`
- **Default**: none (required when `tcp_bind` is set)
- **CLI**: `--tls-key <path>`
- **Env**: `WEIR_TLS_KEY`
- **TOML**: `tls_key_path`

Path to the private key that pairs with `tls_cert_path`.

**Operational notes**:

- Restrict this file to mode `0o400` owned by the daemon's user. The daemon
  reads it at startup and on SIGHUP; no other process should be able to read
  it.
- Supply via env var (`WEIR_TLS_KEY`) in container environments where secrets
  management injects a file path at runtime.

---

#### `tls_client_ca_path`

- **Type**: absolute path to a PEM-encoded CA certificate
- **Default**: none (required when `tcp_bind` is set)
- **CLI**: `--tls-client-ca <path>`
- **Env**: `WEIR_TLS_CLIENT_CA`
- **TOML**: `tls_client_ca_path`

Path to the Certificate Authority that signs client certificates. Every TCP
client must present a certificate signed by this CA during the TLS handshake.
Anonymous or cert-less clients are rejected at the handshake level
(`weir_tls_handshake_failures_total{reason="no_client_cert"}`).

The **trust model is CA-issuance**: issuing a client cert from this CA is the
act of authorizing that producer. To revoke a client, rotate the CA (CRL/OCSP
are out of scope; see [TCP + mutual TLS](tcp-mtls.md) for the rationale).

**When to tune**: when rotating the client CA as part of a revocation or
re-keying procedure. Update the path and send SIGHUP.

---

#### `tls_handshake_timeout_secs`

- **Type**: u64 (seconds)
- **Default**: `10`
- **Range**: 1+ (no upper cap)
- **CLI**: `--tls-handshake-timeout-secs <n>`
- **Env**: `WEIR_TLS_HANDSHAKE_TIMEOUT_SECS`
- **TOML**: `tls_handshake_timeout_secs`

Maximum time the daemon waits for a TLS handshake to complete on a new TCP
connection. **Slowloris guard for the TLS path**: a TCP client that opens a
connection but stalls during the handshake holds a semaphore permit (from
`max_connections`) for at most this many seconds before being dropped. Drops
increment `weir_tls_handshake_failures_total{reason="timeout"}`.

The semaphore permit is acquired **before** the handshake begins, so a flood
of stalled TCP connections is bounded by `max_connections` regardless of
handshake progress.

**When to tune**: lower for high-throughput deployments where a 10s stall
indicates a problem; raise if clients are on high-latency links where
legitimate handshakes approach the default. The existing per-frame
`connection_read_timeout_secs` continues to apply after the handshake
completes.

---

### TLS metrics

Two metric families are added by the `tls` feature:

| Metric | Type | Labels | What it tracks |
|--------|------|--------|----------------|
| `weir_tls_handshake_failures_total` | counter | `reason` ∈ {`no_client_cert`, `bad_cert`, `timeout`, `other`} | TLS handshakes that failed before a connection was established |
| `weir_tls_config_reloads_total` | counter | `outcome` ∈ {`ok`, `failed`} | SIGHUP-triggered TLS cert/key/CA reload attempts |

Alert on `rate(weir_tls_handshake_failures_total[5m]) > 0` (especially
`reason="no_client_cert"` or `reason="bad_cert"`) to detect unauthorised
connection attempts. Alert on
`weir_tls_config_reloads_total{outcome="failed"}` to detect cert rotation
failures.

---

## Limitations

- **No hot reload (except TLS material)**: config is read once at startup.
  SIGHUP reloads **TLS cert/key/CA only** — all other configuration changes
  require a daemon restart.
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
