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
  `dead_letter_dir`, and the socket parent.
- **No PID namespace constraints, no user namespace remap, no network
  namespace by default.** These are orchestrator-level settings. The
  daemon listens on a Unix socket only; in most deployments the network
  namespace should be `none` (with bind-mounts providing the socket
  path to peer containers) or restricted to localhost.
- **No image-level scanning in CI.** Recommended: add a Trivy or Grype
  scan step to the `docker.yml` workflow to catch CVEs in the base image
  on every rebuild.

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
  ownership to match).
- The HEALTHCHECK (orchestrators may rely on the default behaviour).
- Anything in the `What the image does NOT do` section (operators
  reading this document trust that the image leaves those concerns to
  them).
