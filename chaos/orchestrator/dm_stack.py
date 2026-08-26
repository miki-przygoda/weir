"""Loopback + ext4 storage stack for the chaos harness.

Phase 1 built the plumbing only: a sparse backing file, a loop device, an
ext4 filesystem, and a mount. Phase 2 inserts a device-mapper target between
the loop device and the filesystem:

    losetup -> dmsetup create -> mkfs.ext4 -> mount

`dm_target` selects what that target is:
  None     -> Phase 1, unchanged. mkfs and mount target the loop device
              directly. This is the default, so every existing schedule
              behaves exactly as before.
  "flakey" -> dm-flakey, the real fault. mkfs and mount target the mapper
              device. Created DISENGAGED — mkfs and the steady-state
              workload must run against an honest disk — and
              `engage_fault`/`disengage_fault` flip it per episode.
  "linear" -> a pass-through stand-in for machines without dm-flakey (the
              Pi does not ship the module and has no DNS to install it).
              Builds the identical stack and exercises the identical
              create/read-back/remove/suspend-reload-resume paths, but
              injects nothing. `engage_fault` on it always raises: engaging
              a pass-through is meaningless.

Linux and root only.
"""
import os
import subprocess
import sys
import time

import dm_flakey

# ext4 needs a few MiB of metadata before it will even mkfs.
MIN_SIZE_MB = 16

#: The dm targets this phase knows how to build.
_DM_TARGETS = (None, "flakey", "linear")


def _run(cmd, check=True):
    """Runs a command, returning CompletedProcess.

    On failure with `check=True` this raises with stderr INCLUDED in the
    message. `subprocess.run(check=True)` raises `CalledProcessError`, whose
    `__str__` omits captured stderr entirely — so a real `mkfs.ext4` refusal
    would reach the orchestrator as an undiagnosable "returned non-zero exit
    status 1" and the operator would have no idea why the run died.
    """
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed (exit {proc.returncode}): {' '.join(cmd)}\n"
            f"stderr: {proc.stderr.strip()}"
        )
    return proc


def _linear_table(device, sectors):
    """One dm-linear table line: a straight 1:1 pass-through.

    Unlike flakey, linear takes no optional feature arguments, so there is no
    "kernel substitutes an erroring default" failure mode to guard against —
    read-back only needs to confirm the target TYPE that actually came up.
    """
    return f"0 {sectors} linear {device} 0"


def _dm_remove(name, timeout_s=10):
    """Removes a mapping and WAITS for it to really be gone.

    `dmsetup remove` is not synchronous: it returns before udev releases the
    node. The next `create` then fails EBUSY and — this is the dangerous part —
    the PREVIOUS table stays installed, so the following episode measures the
    old mapping and reports a confident result about a device it never created.
    Found the hard way: a portable rewrite of the control script ran fast enough
    to lose this race, which the original's per-command sudo round-trip had hidden.
    """
    _run(["dmsetup", "remove", name], check=False)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _run(["dmsetup", "info", name], check=False).returncode != 0:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"dm mapping {name!r} still present {timeout_s}s after remove. "
        f"Refusing to continue: the next create would fail EBUSY and leave the "
        f"stale table installed, so the next episode would measure the wrong device."
    )


class StorageStack:
    """A loopback-backed ext4 filesystem the WAB can live on.

    Args:
        backing_file: path to the sparse image file (on the HOST fs).
        size_mb: image size. Small on purpose — a small volume is what makes
            real ENOSPC reachable in Phase 3.
        mount_point: where to mount it. Must already exist.
        dm_target: None (default), "flakey", or "linear". See the module
            docstring. Defaults to None so every existing Phase 1 schedule
            behaves exactly as before.
    """

    def __init__(self, backing_file, size_mb, mount_point, dm_target=None):
        if size_mb < MIN_SIZE_MB:
            raise ValueError(
                f"size_mb={size_mb} is below the {MIN_SIZE_MB} MiB ext4 floor"
            )
        if dm_target not in _DM_TARGETS:
            raise ValueError(
                f"dm_target must be one of {_DM_TARGETS!r}, got {dm_target!r}"
            )
        self.backing_file = backing_file
        self.size_mb = size_mb
        self.mount_point = mount_point
        self.dm_target = dm_target
        self.loop_device = None
        self.dm_name = None
        self._sectors = None

    @property
    def fault_device(self):
        """The path mkfs and mount target.

        `/dev/mapper/<dm_name>` once a dm layer has been built — flakey and
        linear both stack the same way. The loop device directly when no
        layer exists, which is what keeps dm_target=None's plumbing
        byte-identical to Phase 1.
        """
        if self.dm_name:
            return f"/dev/mapper/{self.dm_name}"
        return self.loop_device

    def setup(self):
        """Creates the image, attaches a loop device, builds the dm layer (if
        any), mkfs, and mounts.

        mkfs and mount always target `fault_device`: the loop device itself
        when dm_target is None, or the mapper device once a dm layer has been
        inserted between the loop device and the filesystem.
        """
        with open(self.backing_file, "wb") as f:
            f.truncate(self.size_mb * 1024 * 1024)

        out = _run(["losetup", "--find", "--show", self.backing_file]).stdout
        self.loop_device = out.strip()

        if self.dm_target is not None:
            # Sector count of the loop device — the dm table must map the
            # whole device or the filesystem sees a truncated disk.
            self._sectors = int(
                _run(["blockdev", "--getsz", self.loop_device]).stdout.strip()
            )
            self.dm_name = f"weir-chaos-{self.dm_target}-{os.getpid()}"
            if self.dm_target == "flakey":
                # Created DISENGAGED. mkfs and the steady-state workload must
                # run against an honest disk; the fault is engaged
                # per-episode by engage_fault().
                table = dm_flakey.flakey_table(
                    self.loop_device, self._sectors, engaged=False
                )
            else:  # "linear"
                table = _linear_table(self.loop_device, self._sectors)
            self._dm_create(self.dm_name, table)

        # -F: the device is fresh; don't prompt. -q: quiet.
        _run(["mkfs.ext4", "-F", "-q", self.fault_device])
        # -t ext4 explicitly, rather than relying on blkid auto-probe. Being
        # explicit makes a mis-stacked device fail fast here instead of
        # mounting something unexpected.
        _run(["mount", "-t", "ext4", self.fault_device, self.mount_point])
        # weir requires 0700 on its WAB dir, and create_dir_private's mode only
        # applies to directories it actually creates — not a pre-existing mount
        # point. Set it here.
        os.chmod(self.mount_point, 0o700)

    def teardown(self):
        """Unmounts, tears down the dm layer (if any), detaches the loop
        device, and removes the image.

        Every step tolerates already-undone state so teardown is idempotent
        and safe to call from a finally block after a partial setup.
        """
        _run(["umount", self.mount_point], check=False)
        if self.dm_name:
            # _dm_remove is Task 4's polling guard: `dmsetup remove` is
            # asynchronous, and creating too soon after fails EBUSY, leaving
            # the previous table installed.
            _dm_remove(self.dm_name)
            self.dm_name = None
        if self.loop_device:
            detach = _run(["losetup", "--detach", self.loop_device], check=False)
            if detach.returncode != 0:
                # Do NOT clear `loop_device`, and do NOT remove the image.
                # Unlinking it would not free the device — the loop driver's
                # open fd keeps the inode alive — and it would make the orphan
                # UN-FINDABLE, because `losetup -j <path>` matches by path.
                # Leaving both in place keeps the leak visible to an operator.
                print(
                    f"WARNING: could not detach {self.loop_device}: "
                    f"{detach.stderr.strip()}. Leaving it and {self.backing_file} "
                    f"in place so `losetup -j {self.backing_file}` still finds it.",
                    file=sys.stderr,
                )
                return
            self.loop_device = None
        if os.path.exists(self.backing_file):
            os.remove(self.backing_file)

    def is_mounted(self):
        """True if mount_point is currently a mount point."""
        return os.path.ismount(self.mount_point)

    def engage_fault(self):
        """Engages the fault on a built dm-flakey layer.

        Raises rather than no-oping on any other stack — a "power loss"
        episode that injected nothing would go green while proving nothing.
        """
        self._require_flakey_layer("engage the fault")
        self._reload_flakey(engaged=True)

    def disengage_fault(self):
        """Disengages the fault on a built dm-flakey layer. See
        `engage_fault` for why anything else raises.
        """
        self._require_flakey_layer("disengage the fault")
        self._reload_flakey(engaged=False)

    def _require_flakey_layer(self, action):
        if self.dm_target != "flakey" or not self.dm_name:
            raise RuntimeError(
                f"cannot {action}: this stack has no built dm-flakey layer "
                f"(dm_target={self.dm_target!r}, dm_name={self.dm_name!r}). "
                f"A linear stack validates plumbing only and injects "
                f"nothing; dm_target=None has no dm layer at all."
            )

    def _reload_flakey(self, engaged):
        """Suspends, reloads a new table, and resumes — then reads the table
        back to confirm it actually took.

        Read the table back after every reload, same as after create: a
        reload that silently no-opped would leave the previous state active
        while looking like it succeeded.
        """
        table = dm_flakey.flakey_table(self.loop_device, self._sectors, engaged=engaged)
        _run(["dmsetup", "suspend", self.dm_name])
        try:
            _run(["dmsetup", "reload", self.dm_name, "--table", table])
        finally:
            _run(["dmsetup", "resume", self.dm_name])
        installed = _run(["dmsetup", "table", self.dm_name]).stdout.strip()
        self._verify_flakey_state(installed, expect_engaged=engaged, context="reload")

    def _dm_create(self, name, table):
        """Creates a mapping and reads the installed table back to verify it.

        The kernel silently substitutes erroring defaults for a flakey table
        submitted without explicit feature args, and a create that fails
        (e.g. EBUSY) leaves the PREVIOUS table installed rather than raising
        cleanly — read-back is the only way to know what actually got
        installed.
        """
        _run(["dmsetup", "create", name, "--table", table])
        installed = _run(["dmsetup", "table", name]).stdout.strip()
        if self.dm_target == "flakey":
            # Always created disengaged: mkfs and the steady-state workload
            # must run against an honest disk.
            self._verify_flakey_state(installed, expect_engaged=False, context="create")
        else:  # "linear"
            self._verify_linear_target(installed)

    def _verify_flakey_state(self, installed, expect_engaged, context):
        # dm_flakey.table_is_engaged raises UnexpectedTable if the kernel
        # substituted error_reads/error_writes, or if the table isn't a
        # flakey table at all — both propagate straight out of here.
        actual = dm_flakey.table_is_engaged(installed)
        if actual != expect_engaged:
            raise RuntimeError(
                f"{self.dm_name} came up engaged={actual} after {context}, "
                f"expected engaged={expect_engaged}. A device that came up "
                f"engaged when it shouldn't have would corrupt the "
                f"filesystem or the steady-state workload; a reload that "
                f"silently didn't take would run a 'power loss' episode "
                f"with no power loss in it. Got: {installed!r}"
            )

    def _verify_linear_target(self, installed):
        fields = installed.split()
        target = fields[2] if len(fields) > 2 else None
        if target != "linear":
            raise RuntimeError(
                f"{self.dm_name} did not come up as a linear target — a "
                f"stale table from a failed create may have survived. "
                f"Got: {installed!r}"
            )
