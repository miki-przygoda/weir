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
              `drop_and_remount` is what actually converts an engaged fault
              into filesystem-level loss — a dropped write leaves a
              correct, resident page in the cache until something forces
              writeback and then evicts it; see its docstring and the Phase
              2 spec's "Protocol: kill first, then drop and remount".
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

    `--retry` (what the validated control script used) asks dmsetup itself to
    retry the removal a few times before giving up, rather than firing once and
    leaving all the waiting to the poll loop below. And the removal's own
    stderr is kept: the likeliest real failure is `remove` refusing because the
    preceding `umount` (also `check=False`, in `teardown`) itself failed, and
    without this an operator sees only "still present 10s after remove" instead
    of the kernel's actual reason.
    """
    remove = _run(["dmsetup", "remove", "--retry", name], check=False)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _run(["dmsetup", "info", name], check=False).returncode != 0:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"dm mapping {name!r} still present {timeout_s}s after `dmsetup remove "
        f"--retry`. Refusing to continue: the next create would fail EBUSY and "
        f"leave the stale table installed, so the next episode would measure "
        f"the wrong device. The remove itself exited {remove.returncode}, "
        f"stderr: {remove.stderr.strip()!r} — likeliest cause is a preceding "
        f"`umount` that failed silently (it is also check=False)."
    )


def _device_major_minor(path):
    """`major:minor` for a device node — what `dmsetup table` reports for the
    underlying device, never the path it was given (see dm_flakey.py's module
    docstring). Lets `_verify_linear_target` compare identity, not just shape.
    """
    st = os.stat(path)
    return f"{os.major(st.st_rdev)}:{os.minor(st.st_rdev)}"


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
            try:
                _dm_remove(self.dm_name)
            except RuntimeError as exc:
                # A stuck mapping must not leak the loop device AND the
                # backing image behind it too — that would contradict
                # teardown's documented idempotence, which Phase 1 had. Warn,
                # same as the detach failure below, and keep going: the loop
                # device is still worth trying to detach even with a wedged
                # mapping sitting on top of it, and either way the operator
                # needs both problems reported, not just whichever this
                # function reached first. Do NOT clear dm_name — a mapping
                # that failed to remove must stay visible to a later
                # teardown() call, not be silently forgotten.
                print(
                    f"WARNING: could not remove dm mapping {self.dm_name!r}: "
                    f"{exc}. Leaving it in place; `dmsetup remove --retry "
                    f"{self.dm_name}` by hand before this seed can run again.",
                    file=sys.stderr,
                )
            else:
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

    def drop_and_remount(self, wab_dir=None):
        """Converts "the disk lied about a write" into "the filesystem
        actually lost it" — see the Phase 2 spec's "Protocol: kill first,
        then drop and remount" section for the full argument this rests on.

        Must be called with the fault ALREADY ENGAGED. `drop_writes` sits
        BELOW the page cache: a dropped write still leaves a correct,
        resident page behind, and nothing (not even `kill -9`) evicts it on
        its own. So nothing is actually at risk until:

        1. `umount` — while still engaged. The writeback this forces is what
           the lying disk discards. This is the step that puts anything at
           risk at all.
        2. Disengage — the daemon this episode is about to restart, and the
           next episode's steady-state load, must run against an honest disk.
        3. `mount` again — ext4's journal replay reconstructs the filesystem
           from whatever the disk actually still has, which is the fault made
           real and observable.

        A failed remount is reported LOUDLY — raised, not printed as a
        warning and limped past — because the daemon this episode is about to
        restart would otherwise come up against a missing or unmountable WAB
        directory, and the resulting failure would read as a weir defect
        rather than the injector damage it actually is.

        `wab_dir`, if given, is recreated with the `0o700` mode weir requires
        if it did not survive the cycle intact — but ONLY when it is actually
        inside this stack's mount point; a caller-supplied path outside it is
        none of this method's business. The ext4 journal replay SHOULD carry
        it through unchanged (it existed on the honest disk before the fault
        engaged), but this is a fault injector — verify, don't trust.
        """
        self._require_flakey_layer("drop and remount")
        _run(["umount", self.mount_point])
        self.disengage_fault()
        result = _run(
            ["mount", "-t", "ext4", self.fault_device, self.mount_point],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"remount of {self.fault_device} onto {self.mount_point} "
                f"FAILED (exit {result.returncode}) after drop_and_remount's "
                f"umount/disengage: {result.stderr.strip()}. This is a "
                f"genuine harness/injector failure — the daemon is about to "
                f"restart with nowhere to put its WAB — and must not be "
                f"limped past as a warning."
            )
        if wab_dir is not None and self._is_inside_mount(wab_dir):
            os.makedirs(wab_dir, exist_ok=True)
            os.chmod(wab_dir, 0o700)

    def _is_inside_mount(self, path):
        """True if `path` lives under this stack's mount point — the guard
        that keeps `drop_and_remount`'s wab_dir re-creation from touching a
        caller-supplied path that has nothing to do with this mount.
        """
        mount = os.path.abspath(self.mount_point)
        target = os.path.abspath(path)
        return target == mount or target.startswith(mount + os.sep)

    #: Canary file name at the mount root (a sibling of `wab/`, never inside
    #: it — it has nothing to do with weir and must not be mistaken for WAB
    #: content by a post-mortem scan).
    CANARY_NAME = ".chaos-canary"

    def write_canary(self, content):
        """Writes `content` to a known file at the mount root and fsyncs it.

        I6: called once before the fault engages (establishing ground truth)
        and again while it is engaged, still mounted (the overwrite the
        injector is actually being tested against — under the same
        conditions weir's own WAB writes are under). `read_canary` after the
        remount turns "did the injector bite" from an inference into a
        measurement.
        """
        path = os.path.join(self.mount_point, self.CANARY_NAME)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, content)
            os.fsync(fd)
        finally:
            os.close(fd)

    def read_canary(self):
        """Reads the canary file back, or `None` if it does not exist (never
        written, or the fault destroyed it outright)."""
        path = os.path.join(self.mount_point, self.CANARY_NAME)
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

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
        # --nolockfs IS THE POINT, not an optimisation. Without it, suspend
        # freezes and flushes every dirty page to the still-honest disk
        # before the new table takes effect — so nothing is at risk when the
        # fault goes live — and weir's blocked writes then unblock AFTER the
        # thaw, against a lying disk, manufacturing exactly the false acks
        # this whole protocol exists to prevent. Every dm-flakey harness
        # (xfstests' common/dmflakey) passes it for the same reason.
        _run(["dmsetup", "suspend", "--nolockfs", self.dm_name])
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
        """Confirms the installed table is really OUR linear pass-through,
        not just some linear target that happened to survive a failed
        create — target type alone can't distinguish those.

        Sector count is checked whenever `_sectors` is already known (the
        real call path from `_dm_create` always has it by then). The backing
        device is checked too, but only best-effort: `dmsetup table` reports
        it as `major:minor`, never the path this stack tracks, so it must be
        resolved via `_device_major_minor` — which needs the device node to
        actually exist. If it doesn't (or `loop_device` isn't set), the
        device check is skipped rather than raising an unrelated OSError out
        of a routine whose whole job is raising the RIGHT error.
        """
        fields = installed.split()
        target = fields[2] if len(fields) > 2 else None
        if target != "linear":
            raise RuntimeError(
                f"{self.dm_name} did not come up as a linear target — a "
                f"stale table from a failed create may have survived. "
                f"Got: {installed!r}"
            )
        sectors = fields[1] if len(fields) > 1 else None
        if self._sectors is not None and sectors != str(self._sectors):
            raise RuntimeError(
                f"{self.dm_name} came up linear over {sectors} sectors, "
                f"expected {self._sectors} — a stale table covering a "
                f"DIFFERENT device may have survived a failed create. "
                f"Got: {installed!r}"
            )
        expected_device = None
        if self.loop_device:
            try:
                expected_device = _device_major_minor(self.loop_device)
            except OSError:
                expected_device = None
        device = fields[3] if len(fields) > 3 else None
        if expected_device is not None and device != expected_device:
            raise RuntimeError(
                f"{self.dm_name} came up linear over {device!r}, expected "
                f"{expected_device!r} ({self.loop_device}) — a stale table "
                f"pointing at a DIFFERENT backing device may have survived a "
                f"failed create. Got: {installed!r}"
            )
