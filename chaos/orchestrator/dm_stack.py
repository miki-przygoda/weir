"""Loopback + ext4 storage stack for the chaos harness.

Phase 1 builds the plumbing only: a sparse backing file, a loop device, an
ext4 filesystem, and a mount. The dm-delay and dm-flakey layers that inject
faults land in Phases 2-3; this class owns the lifecycle they will slot into.

Linux and root only.
"""
import os
import subprocess

# ext4 needs a few MiB of metadata before it will even mkfs.
MIN_SIZE_MB = 16


def _run(cmd, check=True):
    """Runs a command, returning CompletedProcess. Raises on failure if check."""
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


class StorageStack:
    """A loopback-backed ext4 filesystem the WAB can live on.

    Args:
        backing_file: path to the sparse image file (on the HOST fs).
        size_mb: image size. Small on purpose — a small volume is what makes
            real ENOSPC reachable in Phase 3.
        mount_point: where to mount it. Must already exist.
    """

    def __init__(self, backing_file, size_mb, mount_point):
        if size_mb < MIN_SIZE_MB:
            raise ValueError(
                f"size_mb={size_mb} is below the {MIN_SIZE_MB} MiB ext4 floor"
            )
        self.backing_file = backing_file
        self.size_mb = size_mb
        self.mount_point = mount_point
        self.name = os.path.basename(mount_point.rstrip("/"))
        self.loop_device = None

    def setup(self):
        """Creates the image, attaches a loop device, mkfs, and mounts."""
        with open(self.backing_file, "wb") as f:
            f.truncate(self.size_mb * 1024 * 1024)

        out = _run(["losetup", "--find", "--show", self.backing_file]).stdout
        self.loop_device = out.strip()

        # -F: the device is fresh; don't prompt. -q: quiet.
        _run(["mkfs.ext4", "-F", "-q", self.loop_device])
        _run(["mount", self.loop_device, self.mount_point])
        # weir requires 0700 on its WAB dir, and create_dir_private's mode only
        # applies to directories it actually creates — not a pre-existing mount
        # point. Set it here.
        os.chmod(self.mount_point, 0o700)

    def teardown(self):
        """Unmounts, detaches the loop device, and removes the image.

        Every step tolerates already-undone state so teardown is idempotent and
        safe to call from a finally block after a partial setup.
        """
        _run(["umount", self.mount_point], check=False)
        if self.loop_device:
            _run(["losetup", "--detach", self.loop_device], check=False)
            self.loop_device = None
        if os.path.exists(self.backing_file):
            os.remove(self.backing_file)

    def is_mounted(self):
        """True if mount_point is currently a mount point."""
        return os.path.ismount(self.mount_point)
