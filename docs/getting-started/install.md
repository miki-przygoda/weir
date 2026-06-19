# Installation

> **TL;DR** — build from source (`cargo build --release -p weir-server`),
> grab a pre-built binary from the
> [GitHub Releases](https://github.com/miki-przygoda/weir/releases), or use the
> container image (`docker compose -f deploy/docker/docker-compose.yml up`).
> `cargo install weir-server` lands with the 1.0 crates.io release (see below).

Several install paths are covered below — from source, container image,
`cargo install`, or a pre-built release binary. They all produce the same
`weir-server` daemon; the choice is purely about how you want to build,
deploy, and update it. (The systemd section below is for *running* an
already-installed binary as a bare-metal service, not a separate install
method.)

## From source (Cargo)

### Prerequisites

- **Rust 1.88+** (edition 2024) — the declared MSRV (`rust-version` in
  `Cargo.toml`, enforced in CI). `rustup default stable` is enough.
- A Unix host (Linux or macOS) to **run** the daemon. `weir-server`
  *builds* on Windows (CI and the release workflow produce a
  `weir-server.exe`), but it is a non-functional stub there — there is no
  Unix-socket listener, so the daemon never serves. `weir-core` is
  genuinely cross-platform; `weir-client` compiles everywhere but its
  client type (`WeirClient`) is Unix-only, so Windows has no usable client.
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
you care about size (~9.5 MB → ~8.3 MB).

#### Minimal build (smaller dep tree)

The default build enables the `http-sink`, `mysql-sink`, and `postgres-sink`
features, which pull in the full MySQL and PostgreSQL client stacks (and several
duplicate-version transitive crates — `cargo tree -d` shows them, largely pinned
by `mysql_async` / `postgres-protocol`). If you only need one sink, build with
just that feature for a much smaller dependency tree and faster compile:

```bash
# HTTP sink only (no SQL client stacks):
cargo build --release -p weir-server --no-default-features --features http-sink
# noop only (soak-testing, no sinks compiled in):
cargo build --release -p weir-server --no-default-features
```

Available sink features: `http-sink`, `mysql-sink`, `postgres-sink`,
`clickhouse-sink` (the `noop` sink is always compiled in). Trimming the *default*
feature set is a breaking change deferred to a future major version, so the full
set stays the default for now — use `--no-default-features` to opt out today.

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
weir-server --help    # prints the full flag/option reference
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

Restart the daemon to pick up a new binary or any config change. The only
hot-reload is `SIGHUP`, which reloads **TLS cert/key/CA only** (and only on
builds with the `tls` feature) — see `docs/operations/configuration.md`. Every
other setting requires a restart, so plan for a graceful restart window (default
30 s, see `shutdown_timeout_secs`).

### Building other crates

```bash
cargo build --release -p weir-client    # client library (lib)
cargo build --release                   # the whole workspace
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

(This is a manual build tagged `weir:latest`. Note that `docker compose`
builds and tags its own image `weir-server:local` — a different tag — so
the two don't collide and `docker rmi weir:latest` won't remove the
compose image.)

The build uses BuildKit's cache mounts where available. First build
takes ~3–5 minutes; subsequent builds with unchanged dependencies are
~30 seconds.

### Run with Docker Compose

```bash
docker compose -f deploy/docker/docker-compose.yml up
```

This stores the WAB in a named Docker volume (`wab_data`, mounted at
`/var/lib/weir/wab`) and exposes the metrics port on `127.0.0.1:9185`.
Configuration goes in
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

## systemd (bare-metal / VM)

For a non-container host, a ready-to-adapt hardened unit ships in
[`deploy/systemd/`](https://github.com/miki-przygoda/weir/tree/main/deploy/systemd):

- `weir.service` — graceful-shutdown wiring (`TimeoutStopSec` aligned to the
  daemon's `shutdown_timeout_secs`), `RuntimeDirectory`/`StateDirectory` for the
  socket and WAB dirs, an `ExecStartPre` that creates the `wab/` subdir (the
  daemon refuses to start if `wab_dir` is missing), resource limits, and the
  sandbox the [threat model](../security/threat-model.md) recommends.
- `weir.toml` — companion config (no secrets) referenced via `--config`.
- `weir.env.example` — `EnvironmentFile` pattern for keeping the sink URL /
  bearer token off-disk and out of `argv` (set it mode `0600`, never commit real
  secrets).
- `weir-readiness.sh` — a liveness+readiness probe over `weir-ctl health` plus a
  `/metrics` scrape.

See `deploy/systemd/README.md` for install, enable, and shutdown-tuning steps.

## `cargo install`

> **Status:** the crates are not on crates.io yet — publishing is part of
> the 1.0 release rollout. Until then, use the from-source path above. Once
> published, this will work:

```bash
cargo install weir-server
```

It will fetch the latest published version, build it with the host's Rust
toolchain, and install to `~/.cargo/bin/weir-server`. The wire protocol and
public API are frozen at 1.0 under Semantic Versioning.

## Pre-built release binaries

The `release.yml` GitHub Actions workflow builds and attaches `weir-server`
binaries for `x86_64-linux`, `aarch64-linux`, `x86_64-macos`, `aarch64-macos`,
and `x86_64-windows` to each version tag's
[GitHub Release](https://github.com/miki-przygoda/weir/releases). The
`x86_64-windows` artifact is a `weir-server.exe`, but it is a non-functional
stub: there is no Unix-socket listener on Windows, so the daemon does not
serve — run the daemon only on Linux or macOS. Download the archive for your
platform, or use the from-source or container paths above.

## Verifying the install

After installing via any method:

```bash
# The daemon does not create its directories (Postgres model) — make them first.
mkdir -p /tmp/weir-verify/wab /tmp/weir-verify/run && chmod 0700 /tmp/weir-verify/run
weir-server --wab-dir /tmp/weir-verify/wab --socket-path /tmp/weir-verify/run/weir.sock &
DAEMON_PID=$!

# Wait a moment for startup.
sleep 1

# Metrics endpoint should respond. weir_drain_state is pre-initialised on
# startup, so it has data on the very first scrape (no producer needed).
curl -fsS http://localhost:9185/metrics | grep '^weir_drain_state'

# Clean up.
kill -TERM $DAEMON_PID
wait $DAEMON_PID
rm -rf /tmp/weir-verify
```

Expected output (exactly one state is `1.0`; gauge values render as floats and
the lines may appear in any order):

```
weir_drain_state{state="draining"} 1.0
weir_drain_state{state="retrying_transient"} 0.0
weir_drain_state{state="blocked_dead_letter_full"} 0.0
```

That confirms the daemon started and the metrics endpoint is live. (Counter
families like `weir_records_accepted_total` only emit a data line once a record
of that durability tier has been pushed, so they won't appear yet — that's
expected.)

## Uninstalling

### From source / cargo install

```bash
sudo rm /usr/local/bin/weir-server
# Or, if you used cargo install:
cargo uninstall weir-server
```

### Container

```bash
docker compose -f deploy/docker/docker-compose.yml down
# Compose tags its image weir-server:local; remove that one:
docker rmi weir-server:local
# (If you ran the manual `docker build -t weir:latest .` above, also: docker rmi weir:latest)
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
