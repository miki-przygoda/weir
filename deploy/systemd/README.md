# systemd / bare-metal deployment

A hardened systemd unit and companion config for running `weir-server` directly
on a host (no container). For the containerised path, see
[`deploy/docker/`](../docker/).

| File | Installs to | Purpose |
|------|-------------|---------|
| `weir.service` | `/etc/systemd/system/weir.service` | Hardened systemd unit (sandboxing, graceful shutdown, journald logging). |
| `weir.toml` | `/etc/weir/weir.toml` | Daemon config (no secrets). Every key falls back to the documented default. |
| `weir.env.example` | `/etc/weir/weir.env` | Secrets template (sink URL / bearer token). **Copy, fill in, lock down.** |
| `weir-readiness.sh` | anywhere (e.g. `/usr/local/bin/`) | Optional liveness + readiness probe. |

## Install

```bash
# 1. Binary + a dedicated, unprivileged system user.
sudo useradd --system --no-create-home --shell /usr/sbin/nologin weir
sudo install -m0755 weir-server /usr/local/bin/weir-server

# 2. Config dir + files. The TOML and env file are 0640 root:weir: readable by
#    the daemon, not world-readable.
sudo install -d -m0755 /etc/weir
sudo install -m0640 -o root -g weir weir.toml        /etc/weir/weir.toml
sudo install -m0640 -o root -g weir weir.env.example /etc/weir/weir.env
sudo $EDITOR /etc/weir/weir.env   # set WEIR_SINK_URL / WEIR_SINK_BEARER_TOKEN

# 3. The unit.
sudo install -m0644 weir.service /etc/systemd/system/weir.service
sudo systemctl daemon-reload
sudo systemctl enable --now weir
```

You do **not** need to create `/run/weir` or `/var/lib/weir` by hand:

- `RuntimeDirectory=weir` creates `/run/weir` (mode `0700`, owned by `weir`) on
  every start — its `0700` mode is part of weir's socket-bind security model.
- `StateDirectory=weir` creates `/var/lib/weir` (mode `0700`, owned by `weir`).
- `ExecStartPre=` creates the `wab/` subdir. The daemon **refuses to start** if
  `wab_dir` does not already exist (it canonicalizes the path at startup and
  errors with `cannot canonicalize '/var/lib/weir/wab': No such file or
  directory`) — weir follows the Postgres model and never creates its own data
  directory, so the unit pre-creates it.

## Operate

```bash
sudo systemctl status weir          # unit state + recent log lines
journalctl -u weir -f               # follow logs (tagged SyslogIdentifier=weir)
journalctl -u weir -t weir          # filter by the syslog tag
sudo systemctl reload-or-restart weir   # restart (config is read once at startup)
sudo systemctl stop weir            # graceful SIGTERM shutdown (see below)
```

Config is read **once at startup** — there is no hot reload (SIGHUP reloads TLS
material only). Editing `weir.toml` or `weir.env` requires a restart.

## Graceful shutdown: `TimeoutStopSec` vs `shutdown_timeout_secs`

On `systemctl stop` (or a host shutdown), systemd sends `SIGTERM`. weir traps it,
seals the open WAB segment, drains in-flight connections, then exits. Two
timeouts bound that:

- **`shutdown_timeout_secs`** (in `weir.toml`, default **30**) — how long *weir*
  waits for in-flight connections to finish before forcibly closing them.
- **`TimeoutStopSec`** (in `weir.service`, **35**) — how long *systemd* waits
  after `SIGTERM` before escalating to `SIGKILL`.

Keep `TimeoutStopSec` a few seconds **above** `shutdown_timeout_secs` so the
daemon completes its own drain before systemd kills it. If you raise
`shutdown_timeout_secs` (e.g. for long-running batched producers), raise
`TimeoutStopSec` to match (`shutdown_timeout_secs + ~5s`).

## Secrets

Credentials never go in `weir.toml` (it has no on-disk redaction):

- `WEIR_SINK_BEARER_TOKEN` is **env-only** — never read from TOML or a flag.
- A credential-bearing `WEIR_SINK_URL` belongs in env so the password stays off
  disk and out of the process argv (`ps aux`).

Both live in `/etc/weir/weir.env`, sourced via `EnvironmentFile=`. `WEIR_*` env
vars override the TOML. Keep the file `0640 root:weir` and never commit a
filled-in copy. See `weir.env.example`.

## Readiness probe (optional)

`weir-readiness.sh` combines a liveness check (`weir-ctl health` over the socket
+ a `/metrics` scrape) with a readiness check (sink not `down`, drain not blocked
on a full dead-letter dir, zero fsync failures / flusher panics, and a loud WARN
if the sink is `noop`). Exit codes: `0` ready, `1` degraded, `2` dead.

A `/metrics` endpoint that answers `200` but exposes no `weir_*` metrics (a
misrouted `--addr` pointing at some other live service) is treated as `2` dead
("wrong target?"), not as a degraded `1` — so a misconfigured probe surfaces
loudly instead of looking like a transient not-ready.

```bash
sudo install -m0755 weir-readiness.sh /usr/local/bin/weir-readiness.sh
weir-readiness.sh --socket /run/weir/weir.sock --addr 127.0.0.1:9185
```

It needs `bash`, `curl`, and `awk` (standard on a bare-metal host; not present in
the container image — inside Docker, use the Dockerfile's bash `/dev/tcp` probe
instead). Wire it into a monitoring cron, or a `systemd` timer, or a k8s
`readinessProbe` exec. To scrape `/metrics` remotely set `metrics_bind =
"0.0.0.0"` in `weir.toml` **and firewall the port** — `/metrics` is
unauthenticated.
