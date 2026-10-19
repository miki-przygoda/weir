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
sudo systemctl kill --signal=SIGHUP weir  # reload TLS material; a counted no-op otherwise
sudo systemctl stop weir            # graceful SIGTERM shutdown (see below)
```

Config is read **once at startup** — there is no hot reload (SIGHUP reloads TLS
material only). Editing `weir.toml` or `weir.env` requires a restart.

## SIGHUP and why there is no `ExecReload=`

SIGHUP never stops the daemon:

- **During startup** it is ignored only until the runtime starts. `main` sets
  SIGHUP to `SIG_IGN` before it reads the config; the runtime, and with it the
  SIGHUP handler, starts right after the `wab_dir` lock and preflight, before
  WAB recovery. A HUP before that point is dropped; from then on, recovery
  included, it is served.
- **Once the runtime is up** each SIGHUP is a reload request, counted in
  `weir_config_reloads_total{trigger="sighup",outcome=…}`:
  - **without TLS** (no `tcp_bind`): nothing is reloadable. The daemon logs
    `SIGHUP received: nothing reloadable in this build/config (config is read
    once at startup); nothing changed` and counts `outcome="noop"`;
  - **with TLS** (`tcp_bind` set in a `tls` build): it re-reads the cert, key
    and CA and counts `outcome="ok"` or `"failed"`, in this counter and in
    `weir_tls_config_reloads_total`. A failed reload keeps the previous certs.

In 4.0.x and earlier, a daemon without a TLS listener kept the default SIGHUP
disposition, so `kill -HUP` (or a log-rotation script that signals every
process) killed it without sealing or draining.

The unit still has **no `ExecReload=`**, on purpose. `systemctl
reload-or-restart weir` falls back to a restart only while the unit has no
reload command. With an `ExecReload=` that sent SIGHUP, the same command would
become a reload that changes nothing but TLS material, while the operator
believes the edited `weir.toml` took effect. Restart for config changes; send
SIGHUP explicitly (`systemctl kill --signal=SIGHUP weir`) to rotate
certificates.

## One daemon per `wab_dir`: exit codes and restarts

Before it touches the WAB, the daemon takes an exclusive `flock` on
`<wab_dir>/.weir.lock` (here `/var/lib/weir/wab/.weir.lock`). It then runs
preflight: the socket-path liveness probe, a test-bind of the metrics port and,
with `tcp_bind`, a TLS load and a test-bind of the TCP address. Every refusal
happens before recovery.

| Exit status | Meaning | What `Restart=on-failure` does |
|---|---|---|
| `1` | Refused to start. Examples: `wab_dir … is locked by another weir-server (pid … on host …, …)`, `socket_path '/run/weir/weir.sock' is in use by a running process`, the metrics port is taken, invalid TLS material, a filesystem that cannot lock. | Restarts every `RestartSec=5`. Each attempt refuses again, before recovery, until the conflict is gone. That is safe: a lock or preflight refusal reads no WAB and delivers nothing. |
| `3` | WAB ownership lost: the once-a-second lock check found **another** weir-server holding this `wab_dir`'s lock (typically because `.weir.lock` was deleted and a second instance started). The daemon Nacks every new push and exits at once without sealing. | Restarts, then refuses with "locked by" (exit 1) every 5 s while the other instance holds the lock. |

To find the other holder, read the "locked by" line, or `sudo cat
/var/lib/weir/wab/.weir.lock` (a `key=value` holder record, informational
only). Stop that instance, or point one of the two at a different `wab_dir`.
**Never delete `.weir.lock`** to "clear" the error: the lock is the guard, and
deleting it under a running daemon opens the window the exit-3 path exists
for. A lock held by a crashed process does not need clearing, because the kernel
releases it when that process exits.

`systemctl restart weir` is unaffected. systemd waits for the old process to
exit before it starts the new one, so the new process finds the lock free and
the old socket removed (or stale, after a crash).

The lock needs the `flock` syscall. The unit's `SystemCallFilter=@system-service`
with `SystemCallErrorNumber=EPERM` would turn a filtered `flock` into `EPERM`,
which the daemon treats as a refusal (`cannot lock wab_dir …`). To check what
your systemd version's set contains:

```bash
systemd-analyze syscall-filter @system-service | grep -w flock
```

CI runs the same check against the runner's systemd (the `systemd
@system-service still allows flock` step in `.github/workflows/ci.yml`). That
guards the systemd the runner ships, not every host's, so the check above is
still worth running on an unusual distribution. If you tighten the filter, keep
`flock` in it.

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

## TCP + mutual TLS on this deployment path

The unit's `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6` already allows
network sockets, so the TCP listener works under systemd — but it is **off unless
configured**. To turn it on you need a `tls`-feature build plus `tcp_bind`,
`tls_cert_path`, `tls_key_path` and `tls_client_ca_path`; client certificates are
verified against that CA and there is no anonymous TCP path.

Two operational notes specific to bare metal: **firewall the port you publish**
(there is no container network namespace to hide behind here), and **SIGHUP
reloads TLS material only** — see [SIGHUP and why there is no
`ExecReload=`](#sighup-and-why-there-is-no-execreload) — so a certificate
rotation (`systemctl kill --signal=SIGHUP weir`) does not need a restart while a
config change still does.

Full setup, including generating a test CA: [`docs/operations/tcp-mtls.md`](../../docs/operations/tcp-mtls.md).

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
