# Weir threat model

This document describes the trust boundaries, threat model, design
assumptions, and explicit non-goals of the weir daemon. It is the place
the operator should look first when deploying weir, and the place
contributors should consult before adding a new attack surface.

## Trust boundaries

### Unix socket path

Weir's access control for local producers is the **Unix domain socket file's
permissions**. The daemon binds the socket at `socket_path` with mode 0o600.
Anyone who can `connect(2)` to that path can push records.

This means:

- The set of clients trusted to push records = the set of OS users who can
  open the socket file = the daemon's own uid (and root).
- A separate daemon-group model is not currently supported. If you need
  group-readable access, set the parent directory mode appropriately and
  the daemon socket mode to 0o660 in a future config option (not yet
  implemented).
- No application-level authentication on the Unix path. Every connected
  client is treated as fully authorised.

### TCP + mutual TLS path (optional, `--features tls`)

When `tcp_bind` is configured, remote producers connect over **mandatory
mutual TLS** (rustls, aws-lc-rs provider). The trust model differs from the
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
  from it, update `tls_client_ca_path`, send SIGHUP. The old CA is
  immediately distrusted.

See [TCP + mutual TLS](../operations/tcp-mtls.md) for the operator guide.

## In scope

Threats the daemon defends against:

| Threat | Defense |
|---|---|
| Malformed wire frames causing OOM or DoS | Strict validation order: magic → version → header CRC → payload cap → allocation → payload CRC. No allocation occurs before bounds checks. See `crates/weir-core/src/envelope.rs` and `crates/weir-server/src/socket/connection.rs`. |
| Oversized payloads | `max_payload_bytes` config cap (≤ `MAX_PAYLOAD_HARD_CAP = 16 MiB`) checked before allocation. |
| Connection flood | `max_connections` semaphore (default 256, max 512). Connections beyond cap are dropped immediately. |
| Slowloris / stalled clients | `connection_read_timeout_secs` (default 30s, range 1–600) wraps every `read_exact` in `tokio::time::timeout`. Idle connections are dropped and the counter `weir_connection_idle_timeout_total` increments. |
| Queue saturation | `QUEUE_PUSH_TIMEOUT` (5s) caps the time a connection blocks waiting for a queue slot. Excess is nacked, not held. |
| Socket bind path TOCTOU | `bind_hardened` (see `docs/security/socket-bind.md`) uses dirfd + AT_SYMLINK_NOFOLLOW + umask-tightened bind to eliminate the post-bind chmod vulnerability. |
| Symlink attacks on WAB files | Segment creation uses `O_CREAT \| O_EXCL \| O_NOFOLLOW` with explicit mode 0o600. Recovery reopens with `O_NOFOLLOW` before truncate. |
| Stale socket files | Detected and replaced atomically; refuses to remove anything that isn't a socket. |
| WAB segment tampering (mode tightening) | `audit_segment_modes` runs at startup and warns on any `.wab`/`.wab.sealed`/`.wab.confirmed` file whose permissions are not 0o600. Increments `weir_wab_unexpected_mode_total`. |
| Bad CRCs (header or payload) | Reject with `Nack(BadHeaderCrc \| BadPayloadCrc)` and close the connection; counter incremented. |
| Stuck pipeline on shutdown | SIGTERM handler unwinds the pipeline in dependency order; the runtime is dropped before joining threads so background tasks holding queue sender clones don't deadlock the join. |
| Unauthenticated remote TCP producers (`--features tls`) | Mutual TLS with CA-signed client cert required. Anonymous clients rejected at handshake; increments `weir_tls_handshake_failures_total{reason="no_client_cert"}`. |
| TLS-handshake slowloris on TCP path (`--features tls`) | `tls_handshake_timeout_secs` (default 10s) bounds handshake duration. The connection cap semaphore permit is held across the handshake, so a flood of stalled TCP connections is bounded by `max_connections`. Increments `weir_tls_handshake_failures_total{reason="timeout"}`. |
| Plaintext TCP fallback | Setting `tcp_bind` without a valid TLS config is a fatal startup error. The daemon never opens a plaintext TCP socket. |

## Out of scope (current non-goals)

Things weir does **not** defend against in the current version. Some are
deliberate architectural choices; others are future work.

| Out of scope | Why / what to do instead |
|---|---|
| Remote producers (unauthenticated) | TCP listener requires mutual TLS (CA-signed client cert). Anonymous TCP connections are rejected at the handshake. Plaintext TCP is never exposed. See [TCP + mutual TLS](../operations/tcp-mtls.md). |
| Per-client authentication | Possession of socket open access = full producer authority. Filesystem permissions are the access control. |
| Privilege drop | Daemon runs as whatever user invoked it. There is no built-in `setuid`/`setgid` to a dedicated `weir` user. Operators are expected to launch under the desired user (e.g. via systemd `User=weir`). |
| Linux capabilities / seccomp | Not applied. Operators should sandbox the binary externally if needed (systemd `CapabilityBoundingSet=`, `RestrictAddressFamilies=AF_UNIX`, `PrivateTmp=yes`, `ProtectSystem=strict`, etc.). |
| Encrypted at-rest WAB | WAB segments are plaintext on disk. Use filesystem-level encryption (LUKS / dm-crypt) if payloads are sensitive. |
| Signed audit log | Records are written verbatim; no per-record signature or hash chain beyond the per-record CRC. CRC catches accidental corruption, not deliberate tampering. |
| Sink-side authorization | Sinks (HTTP / MySQL / Postgres / ClickHouse, plus the built-in `noop`) carry their own credentials to the downstream system; weir does not pass through producer identity. Authorization to the downstream is the sink's / operator's responsibility. |
| Concurrent-write protection on socket path | Two daemon instances pointed at the same `socket_path` will race. Operator's responsibility (one daemon per socket). |
| Resource accounting per-client | All connections share the same `max_connections` budget. No per-uid quotas. A misbehaving client can consume up to the global cap. |

## Operator-side assumptions

The hardening above assumes the operator has:

1. **Created `wab_dir` with appropriate ownership and mode 0o700.** The
   daemon does not create the WAB directory (PostgreSQL model). It does
   validate at startup that the directory exists and is canonicalisable.
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
- **Operator-misconfigured environment** (e.g. loose umask, world-
  writable parent, stale socket file owned by another user): the
  daemon's startup checks and `bind_hardened` make most of these refuse
  to start rather than silently accept; the rest (e.g. loose umask) are
  defended against by the startup `umask(0o077)` and bind-time
  `umask(0o177)`.
- **Buggy or malicious producer connected to the socket**: bounded by
  per-connection caps (payload size, read timeout) and global caps
  (max_connections, queue capacity). Cannot exhaust memory or block the
  daemon indefinitely; can only consume their share of the connection
  budget.
- **Compromised process running as the daemon uid**: out of scope.
  Equivalent to the daemon itself. Defense is OS-level isolation.

## Reporting

Found something? Please open a private security advisory on the
repository, or contact the maintainer directly. Do not file a public
issue for a verified vulnerability before the fix has shipped.
