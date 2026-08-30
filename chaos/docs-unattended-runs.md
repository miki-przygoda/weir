# Running a soak unattended

Written after monitoring of a 24-hour Durable soak went blind four hours in
(2026-08-30). The run itself was never at risk — it survived intact — but a
failure would have sat unnoticed for sixteen hours.

## Reach beast by SSH key, never Tailscale SSH

Tailscale SSH's `"action": "check"` ACL mints a **fresh browser-auth URL per
connection** and enforces a `checkPeriod` (12 h by default). When that lapses
mid-run, every `ssh` prints

```
# Tailscale SSH requires an additional check.
# To authenticate, visit: https://login.tailscale.com/a/...
```

and then times out. **A browser-auth gate and unattended automation are
fundamentally incompatible**, and raising `checkPeriod` only moves the
collision further out.

Do not confuse this with **node key expiry**, which is the thing the admin
console's "Disable key expiry" toggle controls. They present completely
differently:

| | node key expired | Tailscale SSH check lapsed |
|---|---|---|
| `ping` to the tailnet IP | **fails** | **succeeds** |
| device in `tailscale status` | offline / expired | online |
| `ssh` | no route | prints an auth URL, times out |

Disabling key expiry does nothing for the second case.

The fix is a plain SSH key, which has no session and cannot expire, and works
whether Tailscale SSH is enabled, disabled, or reconfigured underneath you:

```sh
ssh-copy-id -i ~/.ssh/id_ed25519.pub <host>
```

`~/.ssh/config` on the controlling machine:

```
Host beast
    HostName 100.117.23.71
    User miki_przygoda
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
    ServerAliveInterval 30
    ServerAliveCountMax 6
    ConnectTimeout 20
    TCPKeepAlive yes
```

## Keep the box awake for the length of the run

`sleep.target` is **not** masked on beast, so suspend is possible. It has not
bitten (uptime routinely exceeds a day) but nothing prevents it. Rather than
disabling suspend permanently on a desktop, hold an inhibitor for exactly as
long as the run:

```sh
tmux new-session -d -s soak \
  "systemd-inhibit --what=sleep:idle --why='weir chaos soak' \
     sudo -A python3 orchestrator/run.py schedules/<sched>.toml 2>&1 | tee /tmp/soak.log; \
   echo EXIT=\$? | tee -a /tmp/soak.log"
```

## Always `tee` to a file, and always run under tmux

`tmux capture-pane` only returns the **visible** pane, so a long run's early
episodes scroll out of reach. If you forget, `tmux pipe-pane -o -t <session>
"cat >> /tmp/x.log"` starts capturing from that moment on.

tmux is what makes the run survive SSH loss — which it did here, for sixteen
hours of blind monitoring.

## Build the monitor so silence cannot look like health

The first monitor for this run polled over SSH and emitted a heartbeat with
every field empty when SSH failed. That reads as "checked, fine" but means
"could not check at all" — the same vacuous-truth failure the harness's own
negative control exists to prevent, one layer up.

A monitor must distinguish, and say which:

1. **host unreachable** — `ping` fails; the run may be dead
2. **transport failed** — host pings, SSH does not; the run is almost certainly
   fine and observation is blind
3. **the run itself** — completed, died, or produced a non-PASS episode

Only emit a heartbeat when data was actually retrieved, and report a state
change once rather than on every tick.
