# Installation

> **TL;DR** — Today, build from source (`cargo build --release -p weir-server`)
> or use the container image (`docker compose -f deploy/docker-compose.yml up`).
> `cargo install weir-server` and pre-built release binaries are planned for
> v1.0; not yet published.

Three supported install paths are covered below. All install paths
produce the same `weir-server` daemon; the choice is purely about how
you want to build, deploy, and update it.

## From source (Cargo)

### Prerequisites

- **Rust 1.85+** (edition 2024). `rustup default stable` is enough.
- A Unix host (Linux or macOS). weir-server does not build on Windows
  due to Unix-only socket APIs; `weir-core` and `weir-client` do.
- ~500 MB free disk (build artifacts).

### Build

```bash
git clone https://github.com/miki-przygoda/weir.git
cd weir
cargo build --release -p weir-server
```

The binary lands at `./target/release/weir-server`. It's a single
statically-linked-ish executable (links libc and libdl; everything else
is in the binary). Strip with `strip target/release/weir-server` if
you care about size (~15 MB → ~8 MB).

### Install to PATH

```bash
sudo install -m 0755 target/release/weir-server /usr/local/bin/
```

Or symlink for development:

```bash
sudo ln -s "$(pwd)/target/release/weir-server" /usr/local/bin/weir-server
```

### Verify

```bash
weir-server --help    # not yet implemented; will be added in v1.0
weir-server --wab-dir /tmp/wab-test --socket-path /tmp/weir-test.sock
# Ctrl-C to stop.
```

See [quickstart.md](quickstart.md) for the full first-run walkthrough.

### Updating

```bash
cd /path/to/weir
git pull
cargo build --release -p weir-server
sudo install -m 0755 target/release/weir-server /usr/local/bin/
```

Restart the daemon. Updates are not hot — the daemon does not support
SIGHUP-driven reload. Plan for a graceful restart window (default 30
s, see `shutdown_timeout_secs`).

### Building other crates

```bash
cargo build --release -p weir-client    # client library (lib)
cargo build --release                   # all three crates
```

To exercise the daemon end-to-end without writing producer code, build and
run the `push_simple` example:

```bash
cargo run --release -p weir-client --example push_simple -- \
    --socket /run/weir/weir.sock --count 5
```

`weir-core` (wire protocol types) is built transitively.

## Container

The repository ships a multi-stage Dockerfile under
`deploy/docker/Dockerfile`. The image is hardened for production use
(non-root user, pinned UID 10001, STOPSIGNAL, HEALTHCHECK, supply-chain
considerations). See [container hardening](../security/container.md)
for the security review.

### Build the image

```bash
docker build -f deploy/docker/Dockerfile -t weir:latest .
```

The build uses BuildKit's cache mounts where available. First build
takes ~3–5 minutes; subsequent builds with unchanged dependencies are
~30 seconds.

### Run with Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up
```

This mounts `./data/wab` for the WAB and exposes the metrics port on
`127.0.0.1:9185`. Configuration goes in
`deploy/docker/weir.toml.example` — copy to your own path and
bind-mount.

### Run with bare `docker run`

For a production-leaning invocation:

```bash
docker run \
    --read-only \
    --tmpfs /tmp:rw,noexec,nosuid,size=64m \
    --cap-drop=ALL \
    --security-opt no-new-privileges:true \
    --pids-limit 256 \
    --memory 512m \
    --cpus 2 \
    -v weir-wab:/var/lib/weir/wab \
    -v weir-run:/run/weir \
    -v $(pwd)/weir.toml:/etc/weir/weir.toml:ro \
    -p 127.0.0.1:9185:9185 \
    --name weir \
    weir:latest
```

See [container hardening](../security/container.md) for the rationale
behind each flag.

### Image variants

Only one image variant is currently built. Future plans:

- A `distroless` variant on `gcr.io/distroless/cc-debian12` for the
  smallest possible attack surface.
- An `alpine` variant for size-constrained deployments (~20 MB
  total).

Neither is published yet.

## `cargo install` *(not yet — planned for v1.0)*

Once weir is published to crates.io, you'll be able to:

```bash
cargo install weir-server
```

This will fetch the latest published version, build it with the host's
Rust toolchain, and install to `~/.cargo/bin/weir-server`.

Until then, install from source as above. The pre-v1.0 crate is not
on crates.io because the wire protocol may still receive breaking
changes, and `cargo install` users have no way to see the version
caveat before installing.

## Pre-built release binaries *(not yet — planned for v1.0)*

A planned `release.yml` GitHub Actions workflow will build and attach
binaries for `x86_64-linux`, `aarch64-linux`, `x86_64-macos`,
`aarch64-macos`, and `x86_64-windows` (for `weir-core` only) on every
version tag. Not yet implemented.

Until then, use the from-source or container paths.

## Verifying the install

After installing via any method:

```bash
weir-server --wab-dir /tmp/wab --socket-path /tmp/weir.sock &
DAEMON_PID=$!

# Wait a moment for startup.
sleep 1

# Metrics endpoint should respond.
curl -fsS http://localhost:9185/metrics | grep -E "^weir_records_accepted" | head -1

# Clean up.
kill -TERM $DAEMON_PID
wait $DAEMON_PID
```

Expected output:

```
weir_records_accepted_total{tier="sync"} 0
```

Zero acceptances is correct — no producer has connected yet.

## Uninstalling

### From source / cargo install

```bash
sudo rm /usr/local/bin/weir-server
# Or, if you used cargo install:
cargo uninstall weir-server
```

### Container

```bash
docker compose -f deploy/docker-compose.yml down
docker rmi weir:latest
```

### Data

The WAB directory and dead-letter directory are **not** removed by
any of the above. If you want a full wipe:

```bash
sudo rm -rf /var/lib/weir       # production default
rm -rf /tmp/weir-quickstart     # if you followed quickstart.md
```

## Next steps

- [Quickstart](quickstart.md) — first run + push a record.
- [Configuration reference](../operations/configuration.md) — every option.
- [Container hardening](../security/container.md) — production
  deployment recommendations.
