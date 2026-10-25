# Container hardening

This document covers the security-relevant choices in `deploy/docker/Dockerfile`
and how operators should layer additional hardening when running weir in
production.

## Image-level choices

### Base image tags

- Builder: `rust:1-slim-bookworm` — the latest 1.x Rust on Debian slim.
- Runtime: `debian:bookworm-slim` — minimal Debian, no extras.

Both are pinned to major-version+distro tags rather than to digests. For
production, **pin by digest** (e.g. via Renovate / Dependabot) so rebuilds
are bit-for-bit reproducible and a compromised registry tag cannot silently
ship a different image. Example:

```dockerfile
FROM rust:1-slim-bookworm@sha256:abc123... AS builder
FROM debian:bookworm-slim@sha256:def456...
```

### Reproducible Cargo builds (`--locked`)

Both `cargo build` invocations use `--locked`. Without it, a transient
lockfile-resolution issue could silently pull a newer transitive dependency
than `Cargo.lock` specifies — a real supply-chain risk for a daemon that
handles attacker-controlled wire traffic. `--locked` makes Cargo refuse to
update the lockfile during the build, so the dep set is exactly what the
repository's `Cargo.lock` records.

### TCP + mutual TLS listener (`--features tls`)

Both `cargo build` invocations also pass `--features tls`, so the shipped
image includes the TCP+mTLS listener, not just the Unix socket. The
listener stays inert unless an operator sets `tcp_bind` and the three TLS
cert-path config keys — see [TCP + mutual TLS](../operations/tcp-mtls.md)
for setup. Building from source without `--features tls` (e.g. a custom
image) produces a Unix-socket-only binary and rejects a configured
`tcp_bind` at startup instead of opening it.

### Non-root daemon user

The runtime stage creates a system user `weir` with:

- **Pinned UID/GID 10001**: pinning matters for bind-mounted volumes. If
  the host's `/var/lib/weir` is owned by uid 10001, the container must run
  as the same uid to access it. Without a pin the uid drifts across
  rebuilds and breaks existing volume mounts.
- **`--system`**: skips home directory, sets a system-range uid (would be
  in the 100-999 range, but we override to 10001 because the container
  base image's `useradd` defaults to a system-uid floor that may not match
  the host's expectations).
- **`--no-create-home`**: daemon needs no home directory.
- **`--shell /usr/sbin/nologin`**: prevents interactive shell access if
  someone manages to `docker exec` as the weir user.

The `USER weir` directive at the bottom of the Dockerfile means the
daemon runs as uid 10001 from the start, never as root in the container.

### Producer-sidecar uid alignment (peer-uid check)

The daemon enforces a peer-credential check on every accepted connection:
`peer_uid_check` defaults to **`true`** (`crates/weir-server/src/config/mod.rs`),
and the accept loop (`crates/weir-server/src/socket/mod.rs`) reads each peer's
effective uid via `SO_PEERCRED` (Linux) / `getpeereid` (macOS) and **refuses any
connection whose uid does not match the daemon's own effective uid** (it also
fails closed if the credential lookup itself errors). This is defense-in-depth
on top of the socket file's mode bits.

Because the shipped image runs as **uid 10001** (`deploy/docker/Dockerfile`),
this becomes a concrete gotcha the moment a *producer* container shares the
socket with the daemon — e.g. a sidecar in the same Kubernetes pod, or another
container, mounting `/run/weir` via an `emptyDir` (or a host `hostPath`) volume.
The producer connects over that shared socket, so its uid is checked against the
daemon's. You have two options:

1. **Align the producer's uid (recommended).** Run the producer pod/container
   with `runAsUser: 10001` so its connecting uid matches the daemon's. The
   peer-uid check stays on and continues to act as an access-control layer.

   ```yaml
   # producer container in the same pod as the weir daemon
   securityContext:
     runAsUser: 10001    # match the weir image's uid so peer_uid_check passes
   ```

2. **Disable the check.** Set `WEIR_PEER_UID_CHECK=false`
   (`crates/weir-server/src/config/env.rs`) on the daemon. Connections from any
   uid are then accepted.

   **Trade-off:** disabling drops the peer-credential layer entirely — the
   socket file's directory/mode permissions become the *only* thing gating who
   may connect. Prefer option 1 unless producers legitimately run under a
   different uid AND you have deliberately chosen the socket directory's
   permissions as the trust boundary.

### Filesystem permissions baked into the image

The startup `chmod 0o700` on `/run/weir` and `/var/lib/weir/wab` matches
the operational assumption documented in
[`socket-bind.md`](socket-bind.md): the bind-time TOCTOU race window is
closed by parent-directory permissions.

The daemon's own runtime hardening (process umask 0o077, `bind_hardened`'s
nested umask 0o177) is belt-and-braces on top of these directory perms.

### Signal handling (`STOPSIGNAL SIGTERM`)

Made explicit so orchestrators that override Docker's default
(e.g. Kubernetes' `terminationGracePeriodSeconds` flow, which sends a
configurable signal) still get the clean shutdown path. Shutdown is
bounded: listeners close at once (the socket file is unlinked and `/readyz`
answers 503), an idle connection is closed at once and a connection in the
middle of a frame finishes it and gets its ack (for at most
`shutdown_timeout_secs`), the active segments are sealed and fsynced, and the
drain gets `drain_shutdown_budget_secs` before the rest is left sealed for
replay. Give the container `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`
seconds (45 s at the defaults) before the SIGKILL: `docker stop -t 45`,
compose `stop_grace_period: 45s` (the shipped compose file sets it),
Kubernetes `terminationGracePeriodSeconds: 45`. Docker's own default is 10 s.

**SIGHUP never stops the daemon.** `main` sets SIGHUP to ignored before it
reads the config. Once the runtime is up, each SIGHUP is a reload request
counted in `weir_config_reloads_total{trigger="sighup"}`. With `tcp_bind`
set, it reloads the TLS material (`outcome="ok"` or `"failed"`); otherwise it
changes nothing (`outcome="noop"`). `docker kill --signal=HUP <container>` is
therefore safe, and is how to rotate certificates in a container (see [TCP +
mutual TLS](../operations/tcp-mtls.md#cert-rotation-via-sighup)). In the
shipped image weir is PID 1 (exec-form `CMD`, no init), and the kernel
already dropped a default-disposition SIGHUP sent to it. Under
`docker run --init`, `shareProcessNamespace: true` or a shell wrapper weir is
not PID 1, and in 4.0.x and earlier a SIGHUP to a daemon without a TLS
listener killed it without sealing or draining.

### One daemon per WAB volume

A WAB volume must be used by exactly one running daemon. The daemon enforces
this: before it reads the WAB, it takes an exclusive `flock` on
`<wab_dir>/.weir.lock` (in the image, `/var/lib/weir/wab/.weir.lock`, on the
`wab_data` volume). A second container that mounts the same volume on the
same host refuses to start, before recovery, with exit status 1:

```text
wab_dir /var/lib/weir/wab is locked by another weir-server (pid 1 on host
<container hostname>, started …, version …, socket /run/weir/weir.sock). Two
daemons on one wab_dir corrupt each other's segments and can lose acked
records. Stop the other instance or point this one at a different wab_dir.
```

`host` is the holder's `gethostname(2)`: the container hostname under Docker
and the pod name in Kubernetes. Because the holder is PID 1 in its own
namespace, the hostname is what identifies it.

What this means for orchestration:

- **`docker compose up --scale weir=2`** with the shipped compose file:
  the fixed host-port mapping `127.0.0.1:9185:9185` already makes Docker
  refuse the second replica before weir runs. Remove the mapping and the
  second replica starts, refuses on the lock, and `restart: unless-stopped`
  restarts it in a loop. That is safe but noisy. Scale weir by giving each
  instance its own WAB volume.
- **Kubernetes.** Give each replica its own WAB volume (a StatefulSet with
  a volume claim template). Where two pods mount one WAB volume on the same
  node (a `hostPath`, or a `ReadWriteOnce` volume, which allows several pods
  on one node), the lock behaves as above: one pod holds it and the others
  refuse and crash-loop. A `RollingUpdate` that starts the new pod on the same
  node while the old one is still draining makes the new pod refuse with
  "locked by" until the old process exits. The old process holds the lock
  until it exits, which can take up to `shutdown_timeout_secs` after SIGTERM;
  `strategy: type: Recreate` avoids the overlap. A `ReadWriteMany` volume
  shared across nodes is normally a network filesystem, where the lock is not
  a supported guard (next item).
- **Network volumes** (NFS, EFS and other shared filesystems) are not
  supported for the WAB. The lock may be granted there with weaker semantics,
  and it is not a guard across hosts. See the [threat
  model](threat-model.md#residual-windows).
- **Never delete `.weir.lock`** on a live volume. If it is deleted, a second
  instance can start in the moment before the running daemon notices (about
  1 s). If the running daemon then finds another instance holding the lock,
  it Nacks every new push and exits with status **3**. Under
  `restart: unless-stopped` it then restarts and refuses with "locked by"
  until the other instance stops.
- **A shared socket volume** (`/run/weir`) with a different WAB volume is
  caught as well. Before recovery, the second daemon probes the socket path
  and refuses with `socket_path '/run/weir/weir.sock' is in use by a running
  process`. A stale socket left by a killed container is still removed on
  the next start.

### Admin socket

In the image the admin socket lives at `/var/lib/weir/wab.admin.sock`, the
path derived from `wab_dir = /var/lib/weir/wab`. That is inside the image's
`/var/lib/weir` (chowned to uid 10001, not a `VOLUME`), not in the
`/run/weir` volume that producer containers mount. A sidecar running as uid
10001 so that `peer_uid_check` admits it to the producer socket therefore has
no admin access: it cannot see the admin socket, and the daemon's peer check
would only matter if it could.

The shipped image contains only `weir-server`, not `weir-ctl`. In an image
that adds `weir-ctl`, `docker exec <container> weir-ctl status` reaches the
admin socket, since the exec runs as the image's user (uid 10001) in the
daemon's filesystem. Without it, the read-only `/status` document on the
metrics port is the way in from outside the container.

With a read-only root filesystem (`docker run --read-only`, or Kubernetes
`readOnlyRootFilesystem: true`), `/var/lib/weir` is not writable, so the
bind fails, the daemon logs `admin socket unavailable` at ERROR, raises the
`admin_unavailable` condition and carries on without it. To keep the admin
socket, set `WEIR_ADMIN_SOCKET_PATH` to a file in a private writable mount
(a `tmpfs` or an `emptyDir` that is not mounted into producer containers),
whose directory is writable only by uid 10001.

### Healthcheck

`HEALTHCHECK` opens a TCP connection to the metrics port (9185) using
bash's `/dev/tcp` virtual file — no extra packages installed, no curl
or wget needed. The check succeeds iff:

1. The daemon process is alive.
2. The metrics HTTP server has bound the port.

It does not exercise the wire protocol (Unix socket) end-to-end. For a
deeper liveness check, an operator can override `HEALTHCHECK CMD` at
runtime with one that connects to the Unix socket and sends a
`HealthCheck` frame.

The defaults (10s interval, 3s timeout, 5s start period, 3 retries) are
suited to development. Orchestrators (Kubernetes, ECS) usually replace
the Dockerfile `HEALTHCHECK` with their own probes; the in-image version
is a fallback for plain `docker run`.

## What the image does NOT do

Operator hardening that **must be applied externally**:

- **No `seccomp` / AppArmor / SELinux profile baked in.** Apply at the
  container runtime: `docker run --security-opt seccomp=…
  --security-opt apparmor=…`. A minimum seccomp profile for weir would
  allow read/write/socket/bind/connect/accept/fsync/openat/unlinkat/fstatat,
  `flock` and the basic process control set; everything else (especially
  `ptrace`, `mount`, `setuid`, `clone` with namespace flags) can be denied.
  `flock` is load-bearing: if a profile denies it with `EPERM`, the
  `wab_dir` lock fails and the daemon refuses to start (`cannot lock
  wab_dir …`), whatever `wab_dir_lock` is set to. `connect` is needed by the
  sinks and by the startup liveness probe of `socket_path`.
- **No capability dropping.** Apply at runtime: `docker run --cap-drop=ALL`.
  Weir needs no capabilities; it does not bind privileged ports (9185 is
  unprivileged), does not need raw sockets, does not need `CAP_CHOWN` or
  `CAP_DAC_OVERRIDE` (its files are created by its own uid).
- **No filesystem read-only constraints.** Apply at runtime:
  `docker run --read-only --tmpfs /tmp` with volume mounts for the WAB
  and socket directories. Weir does not write outside `wab_dir`,
  `dead_letter_dir`, and the socket parent. Note on testing the
  restart-recovery guarantee: un-drained records survive a restart on a
  persistent WAB volume, but that is only **observable** when a segment is
  genuinely undrained (sink down / stranded). With the `noop` sink or a healthy
  sink, records drain at/just-before graceful shutdown — the drain writes a
  `.confirmed` tombstone and deletes the sealed segment — so a naive "restart and
  list the volume" test sees only `.confirmed` markers, not replayable data. That
  is correct behaviour (delivered, then removed), not data loss. To observe
  recovery, strand a segment first (e.g. point at a down sink).
- **No PID namespace constraints, no user namespace remap, no network
  namespace by default.** These are orchestrator-level settings. By
  default — no `tcp_bind` configured — the daemon listens on a Unix
  socket only, and in most such deployments the network namespace should
  be `none` (with bind-mounts providing the socket path to peer
  containers) or restricted to localhost. The image is built with
  `--features tls`, so it can also serve the TCP+mTLS listener if an
  operator sets `tcp_bind` and the TLS cert paths; in that case the
  container needs the TCP port published (e.g. `-p 7100:7100`) and a
  network namespace that allows the intended inbound access — see
  [TCP + mutual TLS](../operations/tcp-mtls.md).
- **No image-level scanning in CI.** Recommended: add a Trivy or Grype
  scan step to the `docker.yml` workflow to catch CVEs in the base image
  on every rebuild.
- **No file-based secret indirection (`*_FILE`).** Weir's sink credentials
  — the bearer token (`WEIR_SINK_BEARER_TOKEN`, read from env at startup in
  `crates/weir-server/src/main.rs`) and the sink URL, which may carry a
  password (`WEIR_SINK_URL`) — are **env-only**. There is currently no
  `WEIR_SINK_BEARER_TOKEN_FILE` (or equivalent) that would read the secret
  from a mounted file; see the gap note below. Operators who want to keep
  secrets off the process environment cannot do so with weir today.

## A note on secret handling and env inspectability

Weir does the env-side hygiene well, but does not — and cannot — hide the
environment itself. Be honest about what is and isn't protected.

**What weir protects.** The bearer token and sink URL are passed via env
(`WEIR_SINK_BEARER_TOKEN`, `WEIR_SINK_URL`), and weir keeps them out of the
two places it controls:

- **Out of logs.** The token is logged only as a presence boolean
  (`bearer = true|false`), never its value; its `Debug` impl renders
  `<redacted>` (`crates/weir-server/src/sink/http.rs`). A sink URL's password
  is run through `redact_url_password` before it reaches a log line, and the
  `Config: Debug` impl wraps the URL in a redacting newtype.
- **Out of argv.** The bearer token is env-only by design — it is never
  sourced from a `--sink-*` flag or the TOML config file (so it does not
  appear in the process command line or on disk). This is documented in
  `weir-server --help` (see `crates/weir-server/src/config/cli.rs`).

**What weir does NOT protect — the environment is inspectable.** Keeping a
secret in an env var does *not* hide it. The value of `WEIR_SINK_BEARER_TOKEN`
(and the credentials in `WEIR_SINK_URL`) is visible to anyone who can read the
container's environment, e.g.:

- `docker inspect <container>` → `.Config.Env`,
- `/proc/<PID>/environ` (any process with access to that PID's namespace),
- `kubectl describe pod` / the pod spec, for env wired from a manifest.

Do not assume "it's in an env var, not a flag" means the secret is concealed.
It is concealed from *weir's logs and argv*, not from the host or orchestrator.

**Mitigations available today.**

- Source the env var from a **Kubernetes Secret** (`valueFrom.secretKeyRef`)
  rather than a literal in the Deployment. A Secret is encrypted at rest in
  etcd (when encryption-at-rest is configured) and is RBAC-gated, so the raw
  value is not sitting in plaintext in the manifest/`describe` output —
  though it is still materialised into the container's environment at runtime
  and so remains visible via `/proc/<PID>/environ` and `docker inspect` from
  inside the node.
- Restrict who can run `docker inspect`, read `/proc`, and `kubectl
  describe`/`get pod -o yaml` / exec into pods (node access + RBAC).
- Rotate the token; treat node-level access as token-disclosing.

**Possible future improvement (not implemented).** A `*_FILE` indirection —
e.g. a `WEIR_SINK_BEARER_TOKEN_FILE` that reads the token from a mounted file
(a Kubernetes Secret mounted as a volume, or a tmpfs file) — would let a
security-conscious shop keep the secret out of the process environment
entirely. **Weir does not have this today** (no `*_FILE` variant exists in
`crates/weir-server/src/config/env.rs`); it is flagged here as a candidate
enhancement, not a current feature.

## Recommended `docker run` invocation

For a single-host development setup approximating production hardening:

```bash
docker run \
    --read-only \
    --tmpfs /tmp:rw,noexec,nosuid,size=64m \
    --cap-drop=ALL \
    --security-opt no-new-privileges:true \
    --pids-limit 256 \
    --memory=512m \
    --cpus=2 \
    -v weir-wab:/var/lib/weir/wab \
    -v weir-run:/run/weir \
    -v $(pwd)/weir.toml:/etc/weir/weir.toml:ro \
    --tmpfs /run/weir-admin:rw,noexec,nosuid,size=1m,uid=10001,gid=10001,mode=0700 \
    -e WEIR_ADMIN_SOCKET_PATH=/run/weir-admin/wab.admin.sock \
    -p 127.0.0.1:9185:9185 \
    --name weir \
    weir:latest
```

The `127.0.0.1:9185` binding matches the recommendation in
[`threat-model.md`](threat-model.md): the metrics port is not part of
the trust boundary and should not be exposed publicly. `--read-only`
makes `/var/lib/weir` unwritable, so the private `/run/weir-admin` tmpfs
holds the admin socket instead (see [Admin socket](#admin-socket)); it is
not the `/run/weir` volume, and no producer container mounts it.

## Updating this document

When changing the Dockerfile, update this document if you change:

- A base image (re-evaluate the digest-pin recommendation).
- The daemon user uid/gid (operators may have configured volume
  ownership to match — and producer sidecars set `runAsUser` to it; see
  the peer-uid section).
- The peer-uid check default or the `WEIR_PEER_UID_CHECK` var name (the
  producer-sidecar uid-alignment guidance depends on both).
- How sink secrets are sourced — if a `*_FILE` (mounted-secret) indirection
  is ever added, update the secret-handling section, which currently
  documents env as the only path.
- The HEALTHCHECK (orchestrators may rely on the default behaviour).
- The WAB or socket volume layout, or the compose `restart`/port mapping
  (the one-daemon-per-volume section describes how a second replica fails),
  or anything that moves the admin socket onto a volume producers mount
  (the admin-socket section relies on `/var/lib/weir` being private).
- Anything in the `What the image does NOT do` section (operators
  reading this document trust that the image leaves those concerns to
  them).
