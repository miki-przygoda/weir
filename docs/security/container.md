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
configurable signal) still get the clean shutdown path. Weir's SIGTERM
handler unwinds the pipeline in dependency order: socket close → workers
drain → drain task finishes → WAB flush. See
`crates/weir-server/src/main.rs` and the recent deadlock fix in PR #10.

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
  allow read/write/socket/bind/accept/fsync/openat/unlinkat/fstatat and
  the basic process control set; everything else (especially `ptrace`,
  `mount`, `setuid`, `clone` with namespace flags) can be denied.
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
  namespace by default.** These are orchestrator-level settings. The
  daemon listens on a Unix socket only; in most deployments the network
  namespace should be `none` (with bind-mounts providing the socket
  path to peer containers) or restricted to localhost.
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
    -p 127.0.0.1:9185:9185 \
    --name weir \
    weir:latest
```

The `127.0.0.1:9185` binding matches the recommendation in
[`threat-model.md`](threat-model.md): the metrics port is not part of
the trust boundary and should not be exposed publicly.

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
- Anything in the `What the image does NOT do` section (operators
  reading this document trust that the image leaves those concerns to
  them).
