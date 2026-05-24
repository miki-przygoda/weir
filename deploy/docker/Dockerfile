# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from source.
# Copy manifests first so this layer is only invalidated when deps change.
COPY Cargo.toml Cargo.lock ./
COPY crates/weir-core/Cargo.toml    crates/weir-core/Cargo.toml
COPY crates/weir-server/Cargo.toml  crates/weir-server/Cargo.toml
COPY crates/weir-client/Cargo.toml  crates/weir-client/Cargo.toml
COPY crates/weir-bench/Cargo.toml   crates/weir-bench/Cargo.toml

# Stub sources so Cargo can resolve the workspace and compile dependencies.
RUN mkdir -p \
        crates/weir-core/src \
        crates/weir-server/src \
        crates/weir-client/src \
        crates/weir-bench/src && \
    echo 'fn main(){}' > crates/weir-server/src/main.rs && \
    touch \
        crates/weir-core/src/lib.rs \
        crates/weir-client/src/lib.rs \
        crates/weir-bench/src/main.rs

RUN cargo build --release -p weir-server && \
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
    cargo build --release -p weir-server

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Non-root user for the daemon process.
RUN useradd --system --no-create-home --shell /usr/sbin/nologin weir

COPY --from=builder /build/target/release/weir-server /usr/local/bin/weir-server

# Default directories. Override with volume mounts in production.
RUN mkdir -p /var/lib/weir/wab /run/weir /etc/weir && \
    chown -R weir:weir /var/lib/weir /run/weir /etc/weir

# Prometheus metrics endpoint.
EXPOSE 9185

# WAB data and socket live on volumes so they survive container restarts.
VOLUME ["/var/lib/weir/wab", "/run/weir"]

USER weir

# Config file is expected at /etc/weir/weir.toml (bind-mount or secret).
# Override CMD or pass --wab-dir / --socket-path flags to skip the file.
CMD ["weir-server", "--config", "/etc/weir/weir.toml"]
