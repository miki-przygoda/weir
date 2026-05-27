# Production deployments should pin base images by digest (e.g. via Renovate
# or Dependabot) for supply-chain reproducibility. The version-pinned tags
# below are a starting point; see docs/security/container.md for rationale.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from source.
# Copy manifests first so this layer is only invalidated when deps change.
COPY Cargo.toml Cargo.lock ./
COPY crates/weir-core/Cargo.toml    crates/weir-core/Cargo.toml
COPY crates/weir-server/Cargo.toml  crates/weir-server/Cargo.toml
COPY crates/weir-client/Cargo.toml  crates/weir-client/Cargo.toml

# Stub sources so Cargo can resolve the workspace and compile dependencies.
RUN mkdir -p \
        crates/weir-core/src \
        crates/weir-server/src \
        crates/weir-client/src && \
    echo 'fn main(){}' > crates/weir-server/src/main.rs && \
    touch \
        crates/weir-core/src/lib.rs \
        crates/weir-client/src/lib.rs

# --locked enforces that Cargo.lock is used verbatim; cargo refuses to update
# it during the build. Without --locked, a transient lockfile-resolution issue
# could silently pull a newer transitive dep than the lockfile specifies, which
# is a real supply-chain concern for a security-sensitive daemon.
RUN cargo build --release --locked -p weir-server && \
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
    cargo build --release --locked -p weir-server

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
# stale socket from a previous run on the same volume.
VOLUME ["/var/lib/weir/wab", "/run/weir"]

# Weir's signal handlers expect SIGTERM for graceful shutdown. Make this
# explicit so orchestrators that override Docker's default still get the
# clean shutdown path. SIGTERM unwinds the pipeline in dependency order
# and waits for in-flight connections to drain (shutdown_timeout_secs).
STOPSIGNAL SIGTERM

# Lightweight liveness check using bash's /dev/tcp magic (no extra packages
# needed). Opens a TCP connection to the metrics port; succeeds iff the
# daemon is alive and the metrics server has bound. Tunable via
# --health-* flags at run time.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/bin/bash", "-c", "exec 3</dev/tcp/127.0.0.1/9185 || exit 1"]

USER weir

# Config file is expected at /etc/weir/weir.toml (bind-mount or secret).
# Override CMD or pass --wab-dir / --socket-path flags to skip the file.
CMD ["weir-server", "--config", "/etc/weir/weir.toml"]
