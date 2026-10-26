# Weir threat model

This document describes the trust boundaries, threat model, design
assumptions, and explicit non-goals of the weir daemon. It is the place
the operator should look first when deploying weir, and the place
contributors should consult before adding a new attack surface.

## Trust boundaries

### Unix socket path

Weir's access control for local producers has two layers. The first is the
**Unix domain socket file's permissions**: the daemon binds the socket at
`socket_path` with mode 0o600. The second — and the primary
application-level control — is the **default-on peer-uid check**
(`peer_uid_check`, default `true`): on every accept the daemon reads the
peer's effective uid (`SO_PEERCRED` on Linux, `getpeereid` on macOS) and
**refuses any connection whose peer euid does not match the daemon's own**,
closing the socket before a single frame is read. It is fail-closed — if
the credential lookup fails, the connection is refused — and each refusal
increments `weir_connection_rejected_peer_uid_total`.

This means:

- With `peer_uid_check` on (the default), the set of clients trusted to push
  records = exactly the single OS uid equal to the daemon's effective uid.
  Root is **not** implicitly trusted: a process running as root cannot push
  to a daemon running as a non-root user, because root's euid (0) does not
  match the daemon's. (If the daemon itself runs as root, then root is the
  trusted uid — but running as root is discouraged; see the assumptions
  below.)
- The admin socket deliberately also admits root (euid 0) besides the
  daemon's own uid: `sudo weir-ctl drain pause` is an expected way to call
  it. This diverges from the producer rule above on purpose. Root can already
  read and rewrite the WAB and signal the daemon, so admitting it grants
  nothing new.
- With `peer_uid_check` disabled, the access control falls back to the
  socket file's permissions alone, so the trusted set becomes any OS user
  who can `connect(2)` to the socket path.
- A separate daemon-group model is not currently supported. If you need
  group-readable access, set the parent directory mode appropriately and
  the daemon socket mode to 0o660 in a future config option (not yet
  implemented).
- Beyond the peer-uid check there is no per-client application-level
  authentication on the Unix path: a connection that passes the uid check is
  treated as fully authorised, and payloads are opaque bytes.

### Admin socket

The admin socket (`weir-ctl status`, `reload`, `drain pause|resume|retry-stranded`)
is a second Unix socket, separate from the producer socket:

- **Path and mode.** It binds at mode 0o600 at a path derived from `wab_dir`
  (`<parent of wab_dir>/<name of wab_dir>.admin.sock`, so
  `/var/lib/weir/wab.admin.sock` by default), never on the producer socket's
  volume. `admin_socket_path` overrides it.
- **Private parent required.** The bind refuses a parent directory that is
  group- or world-writable, or owned by a non-root user other than the
  daemon's, because anyone who can create names there could bind the path
  first and impersonate the control plane. An explicit path refused this
  way stops startup; the derived default only disables the admin socket
  (ERROR log, `admin_unavailable` condition).
- **Peer check, always on.** On every accept the peer's effective uid must
  be the daemon's or 0. This is independent of `peer_uid_check`, which
  governs only the producer socket, and fail-closed: if the peer
  credentials cannot be read, the connection is refused. The check runs
  before a byte is read. A refused peer gets one error line with code
  `forbidden` that carries no daemon identity, then EOF; the refusal counts
  as `weir_admin_requests_total{verb="_connect",outcome="refused"}` and logs
  a WARN naming only the peer's numeric uid (or that it had no credentials).
- **Bounded framing.** One JSON object per line, protocol v1. A line may be
  at most 64 KiB, enforced while reading, so a longer one is never buffered;
  each request must arrive within 5 s, an idle connection is closed after
  10 s, each reply write is bounded at 5 s, a connection serves at most 64
  requests, and at most 4 connections are served at once.
- **Identity.** Every reply to an authorized peer carries the daemon's
  identity (pid, `wab_dir`, `socket_path`, the `wab_dir` (dev, ino), start
  time), so `weir-ctl` can tell which daemon answered.
- **No secrets.** The `status` reply's document is built from the typed
  config view, which classifies bearer tokens, S3 secrets and URL passwords
  as never shown; no verb reads or returns them.

### TCP + mutual TLS path (optional, `--features tls`)

When `tcp_bind` is configured, remote producers connect over **mandatory
mutual TLS** (rustls, ring provider). The trust model differs from the
Unix path:

- **Client cert required.** Every TCP client must present a certificate
  signed by the configured CA (`tls_client_ca_path`). Anonymous or cert-less
  clients are rejected at the TLS handshake.
- **Trust model: CA issuance.** Issuing a client certificate from the
  configured CA is the act of authorising a producer. There is no
  per-CN/SAN allowlist — CA-signed cert = authorised producer.
- **Plaintext TCP is never exposed.** Setting `tcp_bind` without a valid TLS
  configuration is a fatal startup error; weir never downgrades to cleartext.
- **Shared connection cap.** The Unix and TCP listeners draw from one shared
  semaphore (`max_connections`); the total concurrent connections across both
  transports is bounded by `max_connections`, not 2×.
- **Handshake-slowloris bounded.** `tls_handshake_timeout_secs` (default 10s)
  caps how long a TCP client may stall during the handshake while holding a
  connection permit.
- **Revocation by CA rotation.** CRL and OCSP are out of scope (v2). To
  revoke a client cert, rotate the CA: issue a new CA, re-issue all certs
  from it, update `tls_client_ca_path`, send SIGHUP (or run `weir-ctl reload`,
  the same reload path). The old CA is
  immediately distrusted.

See [TCP + mutual TLS](../operations/tcp-mtls.md) for the operator guide.

## In scope

Threats the daemon defends against:

| Threat | Defense |
|---|---|
| Local producer running as a different uid | Default-on peer-uid check (`peer_uid_check`): the accept loop refuses any connection whose peer euid (`SO_PEERCRED`/`getpeereid`) does not match the daemon's, fail-closed, before any frame is read. Increments `weir_connection_rejected_peer_uid_total`. The application-level complement to the socket's 0o600 mode. |
| Malformed wire frames causing OOM or DoS | Strict validation order: magic → version → header CRC → header fields (message_type/durability/flags) → payload cap → allocation → payload CRC. No allocation occurs before bounds checks. See `crates/weir-core/src/envelope.rs` and `crates/weir-server/src/socket/connection.rs`. |
| Oversized payloads | `max_payload_bytes` config cap (≤ `MAX_PAYLOAD_HARD_CAP = 16 MiB`) checked before allocation. |
| A batch frame declaring more records than it carries | `max_batch_records` config cap (≤ `MAX_BATCH_RECORDS_HARD_CAP = 2048`) checked against the **declared** count before anything is sized by it, and each record's declared length checked against the per-record cap before it is added to the cursor. `record_count` is a u16, so without the cap a ~320 KiB frame could declare 65,535 records — 99.998% of the daemon's global queue capacity, all of it aimed at the single partition the connection is pinned to. The payload CRC has already passed by the time the body is parsed and contributes nothing here: a hostile peer computes a perfectly valid CRC over a body declaring 65,535 records in three bytes. Two conformance vectors carry no record data at all, precisely so that reversing this check order fails them. |
| A batch reply that answers a different batch | `AckBatch` echoes the record count, and a client must check it against what it sent. `ceil(N/8)` is not injective — N of 1017 through 1024 all yield 128 bitmap bytes, a 131-byte payload — so length alone cannot detect a desync, and a bitmap carries no other self-identifying field. |
| Connection flood | `max_connections` semaphore (default 256, max 512). Connections beyond cap are dropped immediately. |
| Network peer reading the metrics, health and status listener | Read-only and unauthenticated by design, bound to 127.0.0.1 by default. `/livez`, `/readyz`, `/startupz` and `/status` match their exact paths, accept only GET and HEAD, read no body and change no state; every other path is the metrics exposition. `/status` serves `PublicStatusDoc`, a separate type from the admin document with no free-text field: no panic, I/O or sink error text, probe reason, reload detail, uid, pid, absolute path, or sink URL, host or bucket. Free text cannot be serialized into it, and the `?verbose` probe lines print only a condition code and its template-rendered summary. A sentinel test, a merge gate, plants a marker in every admin-only string and checks that none reaches `/status` or the verbose probes. |
| Network peer starving the health probes | Every connection first takes one of 64 head slots and must send a complete request head (at most 4096 bytes) within 1 s, or it is closed (`weir_http_requests_dropped_total{reason="head_timeout"}`). Only then does it take a permit from its route's own pool: scrapes from `metrics_max_connections`, the four health routes from a separate pool of 4. A scrape flood cannot starve kubelet probes, and a probe flood cannot starve Prometheus. Writes are bounded at 5 s. **Residual:** a peer on the network path that keeps all 64 head slots busy makes probes fail, not merely wait: a probe arriving while every head slot is taken is closed at once (`weir_http_requests_dropped_total{reason="head_pool_full"}`). Each slot frees within 1 s, so the peer must keep reopening connections to hold the pool; liveness `failureThreshold: 6` absorbs about a minute of it. Loopback binding or a NetworkPolicy restricting the port to kubelet and Prometheus is the boundary. |
| Aggregate memory amplification (many large payloads at once) | Each in-flight payload buffer starts as a bounded first slice (`PAYLOAD_PREALLOC_CAP` = 64 KiB); the remainder of the client-declared length is reserved only after the client has actually delivered that first slice (fixed in 2.0.3 — before, the full declared `payload_len` was allocated before a single body byte arrived, so a client could reserve up to `max_payload_bytes` per connection for free just by lying in the header). Concurrency is bounded by `max_connections`, so worst-case transient receive memory is `max_connections × max_payload_bytes`, but reaching it now requires genuinely streaming that much data — the free-of-data-transfer floor is only `max_connections × PAYLOAD_PREALLOC_CAP` (≈ 16 MiB with the 256-connection default). With the defaults (256 × 16 MiB) the streamed worst case is still **≈ 4 GiB** if every connection simultaneously streams a max-size payload. There is **no global byte budget** across connections in 1.0 — size `max_connections` and `max_payload_bytes` for the host's RAM, and lower `max_payload_bytes` (it caps individual records) if you do not need 16 MiB records. A cross-connection memory budget with admission control is tracked for a post-1.0 release. |
| Slowloris / stalled clients | `connection_read_timeout_secs` (default 30s, range 1–600) wraps every `read_exact` in `tokio::time::timeout`. Idle connections are dropped and the counter `weir_connection_idle_timeout_total` increments. |
| Queue saturation | `QUEUE_PUSH_TIMEOUT` (5s) caps the time a connection blocks waiting for a queue slot. Excess is nacked, not held. |
| Socket bind path TOCTOU | `bind_hardened` (see `docs/security/socket-bind.md`) uses dirfd + AT_SYMLINK_NOFOLLOW + umask-tightened bind to eliminate the post-bind chmod vulnerability. |
| Symlink attacks on WAB files | Segment creation uses `O_CREAT \| O_EXCL \| O_NOFOLLOW` with explicit mode 0o600. Recovery uses `O_NOFOLLOW` on **both** passes — the read/validation open and the truncate/seal open — so a symlink swapped in at the segment path is refused in either case. |
| Stale socket files | A connect-only liveness probe decides: a socket nobody is listening on (a SIGKILLed predecessor's) is unlinked through the pinned parent dirfd and replaced; a live socket, or one whose state the probe cannot tell, is refused and left in place. Never removes anything that isn't a socket. See [`socket-bind.md`](socket-bind.md#stale-socket-removal-and-the-liveness-probe). The admin socket follows the same probe-then-unlink rule, and the same own-inode-only removal at exit. |
| Second daemon on the same `wab_dir` | `<wab_dir>/.weir.lock` is taken with an exclusive `flock` before recovery, preflight or anything else reads the WAB, and held for the life of the process. A second instance refuses to start with `wab_dir … is locked by another weir-server (pid … on host …, started …, version …, socket …)` and exit status 1. Contention is never overridable. See [Single ownership](#single-ownership-of-wab_dir-and-socket_path). |
| Second daemon on the same `socket_path` | Preflight probes `socket_path` before recovery and refuses a live socket (`socket_path '…' is in use by a running process; refusing to remove it.`); the late bind repeats the probe. A live socket is never unlinked, and at shutdown a daemon removes the socket file only if it is still the inode it bound. |
| Another instance taking over a running daemon's `wab_dir` | A once-a-second integrity check re-verifies the held lock. On proof that another instance holds it, every new push is Nacked `InternalError` before it reaches the queue and the daemon exits with status **3** without sealing. See [Single ownership](#single-ownership-of-wab_dir-and-socket_path), including the residual window. |
| Local process reaching the admin socket | Two threats. **An unauthorized local process driving it:** the socket is 0o600 in a parent the bind requires to be private, and every accept checks that the peer euid is the daemon's or root, fail-closed, before a byte is read; a refusal gets one `forbidden` line with no daemon identity, counts as `weir_admin_requests_total{verb="_connect",outcome="refused"}` and logs a WARN. An authorized peer is still bounded: 64 KiB lines enforced while reading, 5 s per request, 10 s idle, 64 requests per connection, 4 connections. No verb reaches a secret: the `status` document's config view excludes bearer tokens, S3 secrets and URL passwords by type. **A squatter impersonating it:** the bind refuses a non-private parent and a live socket at the path, and `weir-ctl` authenticates the server before trusting a reply, by the socket's `lstat` mode, the parent's mode, the peer uid and the `wab_dir` (dev, ino) the reply names. See [The admin socket](socket-bind.md#the-admin-socket). |
| A stray SIGHUP (`systemctl reload`, a log-rotation script, a terminal hang-up) | SIGHUP is set to ignored at the top of `main`, before the config is read, and once the runtime is up it is a counted reload request (`weir_config_reloads_total{trigger="sighup"}`). It no longer terminates the daemon. Before this, any daemon without a TLS listener died on SIGHUP and skipped the seal and drain. |
| WAB segment tampering (mode tightening) | `audit_segment_modes` runs at startup and warns on any `.wab`/`.wab.sealed`/`.wab.confirmed` file whose permissions are not 0o600. Increments `weir_wab_unexpected_mode_total`. |
| Bad CRCs (header or payload) | Reject with `Nack(BadHeaderCrc \| BadPayloadCrc)` and close the connection; counter incremented. |
| Stuck pipeline on shutdown | Shutdown is bounded. Listeners close at once (the socket file is unlinked, inode-checked); a connection in the middle of a frame finishes it and gets its ack, for at most `shutdown_timeout_secs`; an idle connection is closed at once. Each flusher waits at most 1 s to hand its last sealed segment to the drain, and the drain gets `drain_shutdown_budget_secs` before it abandons in-flight work, unconfirmed, for replay. The runtime outlives the worker and flusher joins, so `/metrics`, the probes and the admin `status` verb answer throughout, and the joins are never skipped. A drain stuck in a sink call that never yields is left behind: the process exits 0 and the kernel releases the `wab_dir` lock only once it is gone. A second SIGTERM shortens the connection drain and the drain wait, never the joins. |
| Unauthenticated remote TCP producers (`--features tls`) | Mutual TLS with CA-signed client cert required. Anonymous clients rejected at handshake; increments `weir_tls_handshake_failures_total{reason="no_client_cert"}`. |
| TLS-handshake slowloris on TCP path (`--features tls`) | `tls_handshake_timeout_secs` (default 10s) bounds handshake duration. The connection cap semaphore permit is held across the handshake, so a flood of stalled TCP connections is bounded by `max_connections`. Increments `weir_tls_handshake_failures_total{reason="timeout"}`. |
| Plaintext TCP fallback | Setting `tcp_bind` without a valid TLS config is a fatal startup error. The daemon never opens a plaintext TCP socket. |

## Single ownership of `wab_dir` and `socket_path`

The WAB's crash-safety argument assumes one writer per shard. A second
`weir-server` on the same `wab_dir` breaks it: the second instance's startup
recovery seals the first one's **active** segment, its drain delivers,
confirms and deletes it, and the first daemon then acks Durable records into
an unlinked inode. Those acks are false. In 4.0.x and earlier nothing stopped
this, and a second instance with only a different metrics port would also
unlink the first one's live socket. The daemon now enforces single ownership itself.

### What is guaranteed

**`wab_dir` is exclusively locked before anything reads it.** Startup order
is: SIGHUP ignored → config → `weir starting` → **lock** → **preflight** →
runtime (signal and reload handlers, the metrics/health listener, the admin
socket) → recovery → replay hand-off → pipeline → producer listeners. The lock is `flock(LOCK_EX | LOCK_NB)` on
`<wab_dir>/.weir.lock` (`crates/weir-server/src/wab/lock.rs`):

- The file is opened `O_NOFOLLOW`, created at mode 0600, and must be a regular
  file with exactly one hard link, owned by the daemon's uid. A symlink
  (`ELOOP`), a directory, a FIFO, a device, a hard link (which the record
  write would otherwise truncate through) or a file another uid owns (whose
  owner could loosen it and hold the lock while the daemon is down) refuses
  startup before the `flock`, and nothing is written to it. A file of ours
  with a mode other than 0600 is set to 0600 once the lock is held, with a
  WARN.
- The holder record read back from the file is untrusted: only the first
  4 KiB is parsed, control and bidi formatting characters are stripped, and a
  value over 256 bytes is dropped, so a forged record cannot put escape
  sequences on a terminal through the refusal message.
- The holder record (`pid`, `host`, `started_unix`, `boot_id` on Linux,
  `version`, `socket_path`, `metrics`) is written **only after** the lock is
  held, so a refused contender never truncates the owner's record. The record
  is informational; the lock is the guard.
- The lock is never released explicitly and the file is never unlinked. The
  kernel drops the lock at process exit, which comes after the flusher and
  drain joins and also covers an early error return.
- Contention always refuses (exit status 1). `wab_dir_lock` (TOML),
  `--wab-dir-lock` or `WEIR_WAB_DIR_LOCK` only decide what happens on a
  filesystem that **cannot** lock (`ENOLCK`, `EOPNOTSUPP`, `ENOTSUP`, `ENOSYS`,
  `EINVAL`): `"required"` (the default) refuses to start, and `"best-effort"`
  WARNs on every start, sets `weir_wab_dir_lock_held` to 0 and runs with **no**
  guard. Any other error refuses, whatever the policy. Examples: a lock file
  owned by another user (the message names its uid and the `chown` that fixes
  it), `EROFS`, and `EPERM` from a seccomp filter. On a mount that maps owners
  (NFS `root_squash`, CIFS or FUSE `uid=`) even a file the daemon just created
  reports another owner and is refused; that fails closed.
- `flock` locks the inode in the local kernel, so bind mounts, overlayfs,
  symlinked paths and containers on one host that share a volume all conflict
  correctly.
- `weir-ctl` never calls `flock`, not even a momentary probe, because a probe
  could make a starting daemon fail with "locked by". The offline tools keep
  working beside a live daemon. Every WAB walker already ignores the lock
  file, because it is not a directory.

**A live socket is never stolen.** Preflight runs before recovery. It checks
the socket path's format, refuses a non-socket entry at the path, and runs a
connect-only liveness probe that refuses a live socket, or one the probe
cannot classify. It then test-binds the metrics address and, with
`tcp_bind`, loads the TLS material and test-binds the TCP address. Every
refusal exits 1 before the WAB is read. In 4.0.x and earlier these checks
failed only after recovery, replay and the drain had started. Under a restarting
supervisor, a refused duplicate then re-delivered its oldest unconfirmed
segment on every restart. The late bind probes again and unlinks only a
stale socket. At shutdown the daemon removes the socket file only if the
path still names the inode it bound, so it does not delete a successor's
socket. Mechanism: [`socket-bind.md`](socket-bind.md#stale-socket-removal-and-the-liveness-probe).

**Proven ownership loss fails closed.** From right after acquisition (through
preflight, recovery and the startup replay, not only once the runtime is up),
a separate thread checks the lock every second. It compares the `(dev, ino)` at the lock
path with the held fd and re-issues `flock` on the held fd. The verdicts:

| Verdict | What the daemon does |
|---|---|
| Intact | Nothing. |
| File deleted (re-created with `O_EXCL`, so a file planted meanwhile is never adopted), or replaced by one nobody holds whose record does not name another daemon | Re-creates or re-locks it. ERROR log ending `Never delete the lock file of a running daemon`, and `weir_wab_dir_lock_integrity_failures_total` +1. Keeps running. |
| Another open file description holds the lock (on the held fd, or on the re-created file), or the replacement nobody holds carries another daemon's record (that daemon held the directory and exited; its record is left in place). Both are checked before the hard-link and owner checks, which only gate writing this daemon's record, so a second daemon running as another uid is still caught | **Proof that a second writer owns the directory.** The daemon raises an in-memory flag, so every new push on the Unix and TCP listeners is Nacked `InternalError` before it reaches the queue (`weir_records_nack_total{reason="internal_error"}`, and `weir_records_nack_internal_total{cause="ownership_lost"}`). It then sets `weir_wab_dir_lock_held` to 0, logs `CRITICAL: wab_dir ownership lost: …` naming the other holder, and exits with status **3** at once. It does not seal, because the segments are the other daemon's now, and there is no grace period, because a grace only lengthens the window of knowing false acks. |
| Any other error | ERROR log and `weir_wab_dir_lock_integrity_failures_total` +1. Retried on the next tick. |

Exit status 3 is distinct from 1 (refused to start). Under the shipped
systemd unit (`Restart=on-failure`, `RestartSec=5`), every restart then
refuses with "locked by" until the other instance stops. That is safe and
loud.

### Residual windows

- **Deleting `.weir.lock` under a running daemon.** From the deletion until
  the running daemon's next integrity check (at most about 1 s), a second
  daemon that starts can create and lock a fresh file and run recovery on the
  first daemon's active segments. Durable records the first daemon acks
  between that recovery and its own exit can be false acks. Records it had
  already fsynced into files the second daemon recovers are delivered by the
  second daemon. The integrity check runs on its own thread from right after
  the lock is taken, so the window is the same during a long recovery (when
  the producer socket is not bound yet, so a duplicate with its own metrics
  port would pass preflight): proven loss there exits 3 before anything is
  served. Detection cannot close this window. The only
  defence is **never delete `.weir.lock`**. It is a dotfile, so `rm -rf wab/*`
  does not match it under default shell globbing. Alert on any increase of
  `weir_wab_dir_lock_integrity_failures_total`.
- **Network filesystems.** NFS mounted with `nolock` grants local-only locks
  *silently*: two hosts can both "hold" the lock. With lockd or NFSv4, Linux
  emulates `flock` with whole-file POSIX locks. Closing *any* fd to the file
  anywhere in the process drops such a lock, which is why the daemon opens
  the lock file once and never re-opens it. A lease expiry can also drop the
  lock unseen. The held-fd re-flock catches that only once another client
  has taken the lock. The WAB on a network filesystem is already unsupported
  ([platform-support.md](../platform-support.md)); the lock may be granted
  there, but it is not a supported guard.
- **`wab_dir_lock = "best-effort"`.** On a filesystem that cannot lock, this
  runs with no single-ownership guard at all. `weir_wab_dir_lock_held` reads 0
  for the life of the process, and the integrity check does not run.
- **The socket probe is a real connection.** Probing a live daemon connects
  to it. That bumps the live daemon's connection metrics. Under
  `peer_uid_check`, a probe from a different uid is refused and logged there
  (`refusing connection: peer uid does not match daemon uid`,
  `weir_connection_rejected_peer_uid_total`). An unexplained peer-uid
  rejection next to a refused start is most likely the probe.
- **A daemon that is shutting down still holds the `wab_dir` lock.** At the
  first SIGTERM it unlinks its socket and closes its listeners, so a new
  instance's probe finds nothing live; it refuses with "locked by" instead,
  until the old process exits, up to
  `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` seconds later.
  Under systemd the old process has exited before the new one starts;
  elsewhere, wait for it to exit.
- **Preflight test-binds and releases.** Preflight does not hold the metrics
  and TCP addresses. A process that binds them after preflight still makes
  the authoritative bind fail later, after recovery. Preflight narrows that
  failure to a race; it does not remove it.
- **Socket identity off Linux.** On Linux the bound socket inode is pinned
  with an `O_PATH` fd, so `(dev, ino)` is a sound identity at shutdown.
  Elsewhere the identity is best-effort; see
  [`socket-bind.md`](socket-bind.md#own-inode-only-removal-at-shutdown).

## Tamper evidence — operator-run, not a daemon defense

**This is not a threat the daemon defends against.** Everything in the table
above runs inside the daemon process, unattended. What follows is a separate
tool the operator must schedule and anchor themselves — it is listed on its
own so it is never mistaken for one of the guarantees above.

`weir-attest` hash-chains every **sealed** WAB segment (`weir-ctl attest
seal|verify|head`) over each record's already-frozen `RecordId`, linked to
the previous segment's chain head. Catches: offline tampering (a segment
copied to backup, archive, or another host and edited there), a different UID
on the same host, an edit made on a shared or network filesystem where
`weir-wab`'s own guarantees are already documented not to hold, and silent
corruption that happens to pass CRC32 (a 32-bit checksum recomputes cleanly
in microseconds after an edit; a SHA-256 chain does not). The honest one-line
description is "prove a sealed buffer was not edited after the fact" — and it
holds only if the operator has actually wired up the cron/timer and the
off-host log shipping described in
[monitoring.md](../monitoring.md#integrity--tamper-evidence). A `.attest`
sidecar nobody ever ran `verify` against, or whose head was never observed
off-host, is not evidence of anything — it is inert until an operator makes
it otherwise.

## Out of scope (current non-goals)

Things weir does **not** defend against in the current version. Some are
deliberate architectural choices; others are future work.

| Out of scope | Why / what to do instead |
|---|---|
| Remote producers (unauthenticated) | TCP listener requires mutual TLS (CA-signed client cert). Anonymous TCP connections are rejected at the handshake. Plaintext TCP is never exposed. See [TCP + mutual TLS](../operations/tcp-mtls.md). |
| Per-client authentication | Possession of socket open access = full producer authority. Filesystem permissions are the access control. |
| Privilege drop | Daemon runs as whatever user invoked it. There is no built-in `setuid`/`setgid` to a dedicated `weir` user. Operators are expected to launch under the desired user (e.g. via systemd `User=weir`). |
| Linux capabilities / seccomp | Not applied by the binary itself. Sandbox it externally: the shipped `deploy/systemd/weir.service` sets `NoNewPrivileges=true`, `ProtectSystem=strict`, `PrivateTmp=true`, `SystemCallFilter=@system-service` and `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6` (AF_INET/AF_INET6 are needed: the metrics/health listener is TCP, and so are most sinks). Add `CapabilityBoundingSet=` and similar as your deployment allows. |
| Encrypted at-rest WAB | WAB segments are plaintext on disk. Use filesystem-level encryption (LUKS / dm-crypt) if payloads are sensitive. |
| Non-repudiation / signed audit log | The hash chain described above ("Tamper evidence — operator-run, not a daemon defense") is not a signature. It proves internal consistency and, via an off-host anchor, consistency with what was already observed — it proves nothing about *who* wrote a record, and signing remains a possible follow-up, not something shipped. It also does not defend against a process running as the daemon UID: that is already out of scope above and unchanged here — such a process can rewrite the segment, the `.attest` sidecar, and the anchor's own log line in one motion. **Detection only, never prevention:** nothing stops the write, and a chain whose head was never observed off-host is not evidence of anything. |
| Sink-side authorization | Sinks (HTTP / MySQL / Postgres / ClickHouse / S3, plus the built-in `noop`) carry their own credentials to the downstream system; weir does not pass through producer identity. Authorization to the downstream is the sink's / operator's responsibility. The S3 sink additionally carries its own **hand-rolled SigV4 signer** and resolves credentials from static config, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, IRSA or an EC2 instance role (`crates/weir-sink-s3/src/creds.rs`); the workspace is deliberately ring-only and CI fails if `aws-lc-sys` enters the tree. |
| Deleting `.weir.lock` under a running daemon | The guard detects it within about 1 s, but cannot close the window in between: see [Residual windows](#residual-windows). Never delete the lock file of a running daemon. |
| A WAB on a network filesystem | The `flock` may be granted there with weaker semantics and is not a supported guard. See [Residual windows](#residual-windows). |
| Resource accounting per-client | All connections share the same `max_connections` budget. No per-uid quotas. A misbehaving client can consume up to the global cap. |
| Authentication on the metrics/health listener | None, deliberately: probes and scrapers must reach it without credentials. It is bound to 127.0.0.1 by default. Binding it to `0.0.0.0` (as in Kubernetes) exposes counters, condition codes, summaries and the public config subset to anyone who can reach the port; restrict it with a NetworkPolicy or firewall. It never exposes free text, paths, uids or sink endpoints, and it cannot change state. |

## Operator-side assumptions

The hardening above assumes the operator has:

1. **Created `wab_dir` with appropriate ownership and mode 0o700**, on a
   local filesystem that supports `flock`. The daemon does not create the
   WAB directory (PostgreSQL model). It does validate at startup that the
   directory exists and is canonicalisable, and it creates
   `<wab_dir>/.weir.lock` inside it. Never delete that file while a daemon
   runs (see [Residual windows](#residual-windows)).
2. **Created the parent of `socket_path` (e.g. `/run/weir/`) writable
   only by the daemon's user.** This closes the residual race window in
   the bind sequence (see `docs/security/socket-bind.md`). If the parent
   is world-writable, the bind hardening's late-swap check becomes
   defeatable by a sufficiently fast attacker.
3. **Launched the daemon as a non-root user**, ideally a dedicated
   `weir` system user. The daemon does not drop privileges; whatever uid
   starts it is the uid it serves under.
4. **Tightened the process sandbox externally** if required: systemd
   unit options, AppArmor / SELinux profiles, container `--cap-drop`,
   etc.

## Threat actors considered

- **Local unprivileged user (different uid)**: blocked from connecting
  by socket file mode 0o600. Cannot read the WAB (parent dir 0o700).
  Cannot win the bind-time TOCTOU window unless the socket's parent
  directory is writable.
- **Network peer reaching the metrics/health port** (when `metrics_bind`
  is not loopback): can read the metric exposition and the public
  `/status` document — counts, ages, condition codes and summaries, shard
  and drain state, the public config numbers — but never free text,
  paths, uids, the pid or sink endpoints, because the public type has no
  field for them. Can attempt probe starvation, bounded by the 1 s head
  deadline, the 64-slot head pool and the separate probe pool. Cannot
  change any state: no route mutates, and the admin socket is a Unix
  socket on a private path, not on this port.
- **Operator-misconfigured environment** (e.g. loose umask, world-
  writable parent, stale socket file owned by another user, a second
  instance pointed at a running daemon's `wab_dir` or `socket_path`): the
  `wab_dir` lock, preflight and `bind_hardened` make most of these refuse
  to start, before recovery, rather than silently accept; the rest (e.g. loose umask) are
  defended against by the startup `umask(0o077)` and bind-time
  `umask(0o177)`.
- **Buggy or malicious producer connected to the socket**: bounded by
  per-connection caps (payload size, read timeout) and global caps
  (max_connections, queue capacity). Cannot exhaust memory or block the
  daemon indefinitely; can only consume their share of the connection
  budget.
- **Producer or sidecar sharing the socket volume**: no admin access —
  the admin socket is not on that volume, and a same-uid producer in another
  container cannot reach `/var/lib/weir`.
- **Compromised process running as the daemon uid**: out of scope.
  Equivalent to the daemon itself. Defense is OS-level isolation. This does
  not extend to producer containers that share the daemon's uid only so that
  `peer_uid_check` admits them: they reach the producer socket, never the
  admin socket or the WAB, as long as the `wab_dir` parent is not mounted
  into them.

## Reporting

Found something? Please open a private security advisory on the
repository, or contact the maintainer directly. Do not file a public
issue for a verified vulnerability before the fix has shipped.
