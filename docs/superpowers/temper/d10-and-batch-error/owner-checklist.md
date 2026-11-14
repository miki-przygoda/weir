# d10-and-batch-error: owner evidence checklist

These are the checks only the owner can run. The Docker image could not be
built in the build box (`docker build` fails at `apt-get update` behind the
proxy), and the systemd unit needs root on a host with systemd. No agent ran
any of them. The commands are in [`owner-steps.sh`](owner-steps.sh), next to
this file; `bash owner-steps.sh help` lists them. Each step prints PASS/FAIL
lines and saves them, with its logs, under `~/weir-evidence/d10-and-batch-error/`
(the systemd steps save them under `<bundle>/evidence/` on the disposable host).

Test the sha the head is about to push: the tip of `epic/d10-and-batch-error`
in beast's clone (`/srv/bulk/Projects/weir/src`). `setup` makes a detached
worktree at `~/work/weir/d10-and-batch-error/owner-evidence`, so the epic
branch stays checked out in no worktree. The script never pushes, merges or
deletes anything.

Not on this list:

- The full gate. The head runs it and reports it in the PR.
- Flow 1 (an explicit missing `--config` on the bare binary) and flow 4
  (`push_batch` of 2049 records). Agents ran both on the debug binary, and the
  acceptance tests cover them in the gate.
- The GitHub CI run of `docker.yml`, which runs CONFIG-STARTUP-05 and -08 too.
  Step 2 is the same run on beast, before the PR is opened.

## 1. Setup and image build (beast, your user, ~15-20 min cold)

- [ ] Run `bash owner-steps.sh setup`. It prints the sha under test and saves it
  to `sha.txt`.
- [ ] Run `bash owner-steps.sh docker-build`. It builds `weir-server:local`
  (also tagged `weir-server:d10-<sha12>`) from `deploy/docker/Dockerfile`, and
  replaces any `weir-server:local` you already have. It records the image id
  and the image's CMD.
- [ ] Check that `image-cmd.txt` reads `["weir-server"]`, with no `--config`
  (CONFIG-STARTUP-09 checks the file; this checks the built image).

## 2. Docker acceptance tests (beast, ~3-5 min)

These prove CONFIG-STARTUP-05 (a file bind-mounted at `/etc/weir/weir.toml` is
read with no `--config`) and CONFIG-STARTUP-08 (`docker run weir-server:local`
with only `WEIR_*` env starts, logs the INFO `config source` line with
`loaded=false`, and logs the NOOP SINK WARN). Both have only run in the build
box as skips.

- [ ] Run `bash owner-steps.sh docker-tests`. It refuses to run unless
  `weir-server:local` is the image step 1 built. It runs only these two tests,
  with `WEIR_ACCEPTANCE_DOCKER=1`.
- [ ] Expect PASS on all three checks: `rc=0`, `test result: ok. 2 passed`, no
  `skipping:` line. A `0 passed` or a skip line means the tests did not run.

## 3. The container flows by hand (beast, ~2 min)

This is the same ground as step 2, with every log kept for the PR.

- [ ] Run `bash owner-steps.sh docker-manual`. It runs three containers, each
  named `weir-d10-*-<pid>` and removed (with its volumes) at the end:
  1. **Env-only** (spec flow 2): the container keeps running, and its log has
     these lines:
     - the INFO `config source` line, naming `/etc/weir/weir.toml` with
       `loaded=false`;
     - `shards=2`, from `WEIR_SHARD_COUNT`;
     - the NOOP SINK WARN.

     The log has neither the D10 error nor "will become a startup error".
  2. **`weir-server --config /etc/weir/missing.toml`** in the image (D10):
     - it exits 1 with `config file /etc/weir/missing.toml (passed with
       --config) does not exist`;
     - it logs no `config source` line;
     - it leaves the WAB volume empty and no socket in `/run/weir`.
  3. **Shipped `weir.toml` bind-mounted at `/etc/weir/weir.toml`**:
     - the container keeps running;
     - its log has `config source` with `loaded=true` and origin `default
       path`, and `shards=3`, from the file;
     - its log has no NOOP SINK "no config file" WARN.
- [ ] Expect every line in the `.results` file to be PASS.

## 4. The systemd unit on a disposable host (root, ~5 min)

This proves spec flow 3 / CONFIG-STARTUP-10 with the real `systemctl`. The
shipped `weir.service`, unmodified, runs `--config /etc/weir/weir.toml`. On a
host without that file the unit must fail, with the D10 error in the journal.
Once the file is installed, the same unit must come up. The gate's test only
runs the unit's `ExecStart` line as a user.

Use a throwaway VM or system container with systemd as PID 1, running Debian
12+ or Ubuntu 24.04+. The binary comes from the image, which is built on
bookworm (glibc 2.36). The step creates the `weir` user, so don't run it on
beast's host or on any machine you keep. It refuses to start if
`/etc/systemd/system/weir.service`, `/etc/weir`, `/usr/local/bin/weir-server`,
`/var/lib/weir` or `/run/weir` exists, or if systemd already knows a `weir`
unit.

- [ ] On beast, run `bash owner-steps.sh systemd-bundle`. It copies the image's
  `weir-server`, the shipped `weir.service` and `weir.toml`, this script and a
  `SHA256SUMS` into `~/weir-evidence/d10-and-batch-error/systemd-bundle/`.
- [ ] Copy that dir to the disposable host. With Incus, for example:
  `incus launch images:debian/12 weir-d10` and then
  `incus file push -r ~/weir-evidence/d10-and-batch-error/systemd-bundle weir-d10/root/`.
- [ ] On the host, as root, run `cd /root/systemd-bundle && bash owner-steps.sh systemd-check`.
  It checks the bundle's checksums first.
  - **A, no `/etc/weir/weir.toml`:**
    - the journal has `config file /etc/weir/weir.toml (passed with --config)
      does not exist`;
    - `ExecMainStatus=1` and `Result=exit-code`;
    - there is no `config source` line;
    - `/var/lib/weir/wab` is empty: only `ExecStartPre` made it, with no lock
      and no shards;
    - there is no socket.

    `Restart=on-failure` retries every 5 s, so the step stops the unit before B.
  - **B, the shipped `weir.toml` installed 0640 root:weir:**
    - the unit is active;
    - the `config source` line has `loaded=true` and origin `--config`;
    - there is no D10 error, and `/run/weir/weir.sock` is bound;
    - it stops cleanly (`Result=success`).
- [ ] Expect every line in the `.results` file to be PASS.
- [ ] Clean up: delete the host (`incus delete -f weir-d10`), or run
  `bash owner-steps.sh systemd-cleanup` as root there. That removes only what
  `systemd-check` installed, as listed in `/var/tmp/weir-d10-evidence.manifest`.

## What to paste back

One block per step, for the PR's "How it was tested":

- the sha from `sha.txt`;
- step 1: the image id and `image-cmd.txt`;
- steps 2, 3 and 4: the full `.results` file;
- step 4: also the D10 line from `systemd-check-*-missing.journal`, and the
  `systemctl show` block from `systemd-check-*-missing.log`.

A FAIL in any step blocks the PR. Send its log to the head, rather than
rerunning the step until it passes.
