# Production deployments should pin base images by digest (e.g. via Renovate
# or Dependabot) for supply-chain reproducibility. The version-pinned tags
# below are a starting point; see docs/security/container.md for rationale.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from source.
# Copy manifests first so this layer is only invalidated when deps change.
COPY Cargo.toml Cargo.lock ./
COPY crates/weir-core/Cargo.toml     crates/weir-core/Cargo.toml
COPY crates/weir-wab/Cargo.toml      crates/weir-wab/Cargo.toml
COPY crates/weir-status/Cargo.toml   crates/weir-status/Cargo.toml
COPY crates/weir-server/Cargo.toml   crates/weir-server/Cargo.toml
COPY crates/weir-client/Cargo.toml   crates/weir-client/Cargo.toml
# weir-sink-sdk is a real dependency of weir-server (the Sink trait contract);
# weir-sink-s3, weir-ctl, weir-testkit and the `weir-rs` facade are workspace
# members that don't ship in the image (the S3 sink is behind the non-default
# `s3-sink` feature, which this image does not enable) but whose manifests Cargo
# must read to resolve the workspace. All member Cargo.tomls must be present
# here (+ a stub source below) or `cargo build` fails resolving the workspace --
# adding a workspace member without touching this list breaks the image build,
# which is exactly what happened when weir-sink-s3 landed.
COPY crates/weir-sink-sdk/Cargo.toml crates/weir-sink-sdk/Cargo.toml
COPY crates/weir-sink-s3/Cargo.toml  crates/weir-sink-s3/Cargo.toml
COPY crates/weir-ctl/Cargo.toml      crates/weir-ctl/Cargo.toml
COPY crates/weir-testkit/Cargo.toml  crates/weir-testkit/Cargo.toml
COPY crates/weir-rs/Cargo.toml       crates/weir-rs/Cargo.toml
COPY crates/weir-attest/Cargo.toml   crates/weir-attest/Cargo.toml

# Stub sources so Cargo can resolve the workspace and compile dependencies.
RUN mkdir -p \
        crates/weir-core/src \
        crates/weir-wab/src \
        crates/weir-status/src \
        crates/weir-server/src \
        crates/weir-client/src \
        crates/weir-sink-sdk/src \
        crates/weir-sink-s3/src \
        crates/weir-ctl/src \
        crates/weir-testkit/src \
        crates/weir-rs/src \
        crates/weir-attest/src && \
    echo 'fn main(){}' > crates/weir-server/src/main.rs && \
    echo 'fn main(){}' > crates/weir-ctl/src/main.rs && \
    touch \
        crates/weir-core/src/lib.rs \
        crates/weir-wab/src/lib.rs \
        crates/weir-status/src/lib.rs \
        crates/weir-client/src/lib.rs \
        crates/weir-sink-sdk/src/lib.rs \
        crates/weir-sink-s3/src/lib.rs \
        crates/weir-testkit/src/lib.rs \
        crates/weir-rs/src/lib.rs \
        crates/weir-attest/src/lib.rs

# --locked enforces that Cargo.lock is used verbatim; cargo refuses to update
# it during the build. Without --locked, a transient lockfile-resolution issue
# could silently pull a newer transitive dep than the lockfile specifies, which
# is a real supply-chain concern for a security-sensitive daemon.
# `--features tls` is what gives the image a TCP+mTLS ingest listener. Without
# it the daemon is Unix-socket-only, which is useless in a container: the socket
# lives inside the container's filesystem and nothing outside can reach it.
# Every image before 2.0.3 shipped that way.
RUN cargo build --release --locked -p weir-server --features tls && \
    rm -rf crates/*/src

# Now copy real sources and rebuild only weir-server (deps are cached above).
COPY crates/ crates/

# Docker BuildKit sets COPY'd file timestamps to the Unix epoch (time=0),
# identical to the stub files created by `touch` above. Cargo's fingerprinter
# compares mtimes, so both weir-core and weir-server appear unchanged and the
# empty stub artifact would be reused. Touching all workspace .rs files sets
# their mtime to "now" (> 0), signalling a change and forcing a full recompile
# of all workspace crates while keeping the cached external-dep artifacts.
RUN find crates -name "*.rs" -exec touch {} + && \
    cargo build --release --locked -p weir-server --features tls

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Pinned UID/GID for the daemon user. Pinning matters for bind-mounted
# volumes: if the host's /var/lib/weir is owned by uid 10001, the container
# must run as the same uid to read/write it. Without a pin the uid could
# drift between rebuilds and break existing volume mounts.
RUN groupadd --system --gid 10001 weir && \
    useradd  --system --uid 10001 --gid 10001 \
             --no-create-home --shell /usr/sbin/nologin weir

COPY --from=builder /build/target/release/weir-server /usr/local/bin/weir-server

# Default directories. Override with volume mounts in production.
# /run/weir gets 0o700 so the bind-time TOCTOU race window (documented in
# docs/security/socket-bind.md) is closed by directory permissions.
RUN mkdir -p /var/lib/weir/wab /run/weir /etc/weir && \
    chown -R weir:weir /var/lib/weir /run/weir /etc/weir && \
    chmod 0700 /run/weir /var/lib/weir/wab

# Prometheus metrics endpoint.
EXPOSE 9185

# WAB data and socket live on volumes so they survive container restarts.
# Note: /run/weir is mounted as a volume so the socket path is accessible
# to sidecars or host bind-mounts; in a single-container deployment, a
# tmpfs would be cleaner. The bind hardening (bind_hardened) handles any
# stale socket from a previous run on the same volume; a socket another
# daemon is still listening on is never removed (the start refuses instead).
# One WAB volume = one running daemon: the daemon flocks
# /var/lib/weir/wab/.weir.lock before reading the WAB, so a second container
# on the same volume refuses to start ("locked by"). Never delete that file.
VOLUME ["/var/lib/weir/wab", "/run/weir"]

# Weir shuts down on SIGTERM, within a bound: listeners close at once, an idle
# connection is closed at once and a connection in the middle of a frame
# finishes it and gets its ack (for at most shutdown_timeout_secs), active
# segments are sealed, and the drain gets drain_shutdown_budget_secs to deliver
# before the rest is left sealed for replay. Give it at least
#   shutdown_timeout_secs + drain_shutdown_budget_secs + 5
# seconds (45 s at the defaults): `docker stop -t 45`, compose
# `stop_grace_period: 45s`, Kubernetes `terminationGracePeriodSeconds: 45`.
# (The formula must stay on one line: a docs_drift test searches for it.)
# SIGHUP never stops it: `docker kill --signal=HUP` reloads TLS material when
# tcp_bind is set and is a counted no-op otherwise.
STOPSIGNAL SIGTERM

# LIVENESS: GET /livez over bash's /dev/tcp (the image has no curl or wget).
# /livez answers 200 whenever the runtime serves HTTP: from a few milliseconds
# after start, through WAB recovery, and while shutting down. It fails only on
# the checks selected in `livez_checks` (default none), each a state a restart
# fixes. It never fails on sink health, a paused, retrying or blocked drain, or
# a long recovery, so Docker never restarts weir for a slow sink. Readiness
# ("can it take ingest now?") is GET /readyz; probe it from outside
# (deploy/systemd/weir-readiness.sh, or a Kubernetes readinessProbe). The port
# is the default metrics_port: change 9185 here if you change it.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/bin/bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $l == *' 200 '* ]]"]

USER weir

# Config file is expected at /etc/weir/weir.toml (bind-mount or secret).
# Override CMD or pass --wab-dir / --socket-path flags to skip the file.
CMD ["weir-server", "--config", "/etc/weir/weir.toml"]
