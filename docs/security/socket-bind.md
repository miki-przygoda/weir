# Socket bind hardening

This document describes a TOCTOU (time-of-check to time-of-use) vulnerability
that existed in the original Unix socket bind sequence, the attack surface it
exposed, the fix applied in `crates/weir-server/src/socket/mod.rs::bind_hardened`,
and the residual race window that the deployment is expected to close via
directory permissions.

## The original sequence

```text
1. lstat(socket_path)              // check file type (no symlink follow)
2. if exists and is_socket:
     remove_file(socket_path)      // unlink the stale socket
3. UnixListener::bind(socket_path) // create the new socket file
4. set_permissions(socket_path, 0o600)  // chmod via path
```

Steps 2, 3, and 4 each touch the socket path as a separate syscall. Each
gap between syscalls is a window in which an attacker with write access to
the parent directory can interleave.

## Attack surface

Three concrete scenarios, with their actual impact:

### A — replace with a non-socket between lstat and remove_file

```text
daemon: lstat(P)                   → ok, S_IFSOCK
attacker:                            unlink(P) ; create P as regular file
daemon: remove_file(P)             → removes attacker's regular file
daemon: bind(P)                    → ok
```

Outcome: harmless. The daemon's intent (a socket at P) is achieved.

### B — symlink-out between lstat and remove_file

```text
daemon: lstat(P)                   → ok, S_IFSOCK (the real socket)
attacker:                            unlink(P) ; ln -s /important/file P
daemon: remove_file(P)             → unlink(2) on a symlink only removes
                                     the symlink, not the target
```

Outcome: harmless on Linux — `unlink(2)` does not follow symlinks at the
final component. Documented for completeness; non-Linux Unices have
historically had quirks here, hence the defensive coding.

### C — symlink-in between bind and chmod (the actual exploit)

```text
daemon: bind(P)                    → creates inode X at P, mode 0o755
                                     (assuming umask 022)
attacker:                            unlink(P) ; ln -s /attacker/file P
daemon: set_permissions(P, 0o600)  → path-based chmod follows symlink,
                                     chmod /attacker/file to 0o600
                                     (which attacker can do anyway)
```

Outcome: **the daemon's socket inode X ends up with mode 0o755 (world-
readable, world-writable for connect), not 0o600**. Any user on the host
can connect to the daemon and push records. The chmod that was supposed
to protect the socket silently operates on an attacker-controlled path.

The window in Scenario C is the gap between `bind(2)` and `chmod(2)` —
typically a few microseconds. On a parent directory that any non-daemon
user can write to, a tight `inotify`+`rename` loop can hit it
deterministically.

## The fix

`bind_hardened` replaces the path-based sequence with a directory-fd-based
one and eliminates the post-bind chmod entirely by tightening the umask
before bind:

```text
1. validate_socket_path(P)         // unchanged: absolute, no '..', no nulls
2. open(parent, O_PATH | O_DIRECTORY | O_NOFOLLOW)
                                   // dirfd pins the parent inode; O_NOFOLLOW
                                   // refuses a symlinked parent
3. fstatat(dirfd, basename, AT_SYMLINK_NOFOLLOW)
4. if exists and is_socket:
     unlinkat(dirfd, basename, 0)  // remove relative to dirfd, not by path
   else if exists:
     return error                  // refuse to remove anything that isn't a socket
5. umask(0o177)                    // tighten so bind creates inode at 0o600
6. UnixListener::bind(P)           // bind(2) cannot be redirected at the
                                   // final component for AF_UNIX
7. umask(saved)                    // restore
8. fstatat(dirfd, basename, AT_SYMLINK_NOFOLLOW) → our_inode
   if mode_type != S_IFSOCK:  bail
   if mode & 0o777 != 0o600:  bail (umask tightening failed)
9. fstatat(dirfd, basename, AT_SYMLINK_NOFOLLOW) → final_inode
   if (our_inode.dev, our_inode.ino) != (final_inode.dev, final_inode.ino):
     bail (rename swap detected)
```

Why each change matters:

- **dirfd pin (step 2)**: every subsequent inspection uses `*at()` syscalls
  relative to the fd. An attacker who replaces the parent directory entry
  with a symlink does not redirect us; the dirfd refers to the inode we
  opened originally.
- **AT_SYMLINK_NOFOLLOW everywhere (steps 3, 8, 9)**: catches symlinks at
  the final component, so a `ln -s` swap fails the `S_IFSOCK` check rather
  than silently following.
- **unlinkat instead of remove_file (step 4)**: `unlink(2)` and `unlinkat(2)`
  both operate on the directory entry itself, not the target — but
  `unlinkat(dirfd, name, 0)` makes the directory context explicit and
  cannot be redirected by a parent-dir symlink swap because the dirfd has
  already pinned the parent.
- **umask tightening (steps 5-7)**: this is the critical fix for
  Scenario C. By making `bind(2)` itself create the socket inode at mode
  0o600, we eliminate the post-bind chmod entirely. There is no chmod call
  to redirect. The umask is process-global, but every other file-creation
  path in weir specifies mode bits explicitly (WAB segments use
  `OpenOptions::mode(0o600)`, directories use `DirBuilder::mode(0o700)`)
  and is therefore unaffected by the temporary tightening. A tighter umask
  is also a safer default for any code that doesn't.
- **fchmod was considered and rejected**: on Linux, `fchmod` on a
  `UnixListener` file descriptor operates on the in-kernel sockfs object,
  not on the bound filesystem inode. The mode change does not propagate
  to the bind path's mode bits. (Empirically: after `fchmod(fd, 0o600)`,
  `stat(path)` still shows the pre-chmod mode.) umask is the only
  reliable mechanism we found for getting 0o600 onto the bound inode.
- **Inode equality check (step 9)**: catches a `rename(2)` swap between
  step 8 and step 9. The window is two adjacent syscalls — sub-microsecond
  on commodity Linux. Cannot be reduced to zero without a `bind_at(dirfd,
  basename)` syscall, which Linux does not provide.

## Residual race window

The window between `bind(2)` (step 6) and the first `fstatat` (step 8) is
two adjacent syscalls and cannot be closed in software. An attacker who can
*write to the parent directory* and *win this race* could in principle:

1. Wait for the daemon's `bind(2)` to land.
2. Atomically `rename(attacker_socket, daemon_path)`, replacing the just-
   bound inode with an attacker-controlled socket of mode 0o600.
3. The daemon's `fstatat` at step 8 would see a socket with mode 0o600 and
   not detect the swap (the bind didn't snapshot the inode it created).

The defense for this is **operational, not in-process**:

- The parent directory of the socket path MUST be writable only by the
  daemon's user. With a parent at mode 0o700 owned by the daemon user,
  no other user can rename anything in. The race window becomes
  exploitable only by a process already running as the daemon user,
  which has equivalent access by definition.
- The default deployment layout (`/run/weir/weir.sock` with `/run/weir/`
  owned by the daemon user at 0o700) closes this window.
- A non-default deployment that places the socket under a world-writable
  directory (e.g. `/tmp/weir.sock`) re-opens the window. Don't do that.

At startup `bind_hardened` `fstat`s the pinned parent dirfd and emits a
`WARN` (`warn_if_parent_world_writable`) if the parent is group- or
world-writable — `/tmp` (0o1777) trips it. The warning notes the sticky bit
explicitly, because the sticky bit only restricts *deletion*, not *creation*,
so it does not close the rename race. The daemon does **not** refuse to start:
operators legitimately place sockets under `/tmp` during development, and a
hard refusal would break them. A future change could add an opt-in
`require_private_parent` config flag that promotes this warning to a
start-time refusal for hardened deployments.

## What is tested

The unit tests in `crates/weir-server/src/socket/mod.rs::tests` cover:

- `bind_hardened_refuses_regular_file_at_socket_path` — Scenario A guard:
  refuses to remove a non-socket file.
- `bind_hardened_refuses_symlink_at_socket_path` — Scenario B guard:
  refuses to remove a symlink (even one pointing to a real socket).
- `bind_hardened_replaces_stale_socket` — happy path: stale socket at
  the path is cleanly replaced.
- `bind_hardened_succeeds_when_no_file_exists` — happy path: clean parent.
- `bind_hardened_sets_mode_0600_even_with_loose_umask` — Scenario C
  defense: with `umask(0o000)` set before the call, the socket still ends
  up at mode 0o600.
- `bind_hardened_restores_umask_after_bind` — the umask tightening is
  scoped: the process umask is the same after the call as before.
- `bind_hardened_fails_when_parent_directory_missing` — clean error when
  parent does not exist.
- `bind_hardened_fails_when_parent_is_symlink` — `O_NOFOLLOW` on the
  parent open refuses a symlinked last-component parent.
- `is_group_or_other_writable_classifies_modes` — the world-writable
  predicate: private modes (0o700/0o600) are clean, group/world-writable
  and `/tmp`-style 0o1777 are flagged, bare sticky/setuid bits are not.
- `bind_hardened_warns_but_succeeds_on_world_writable_parent` — a 0o1777
  parent triggers the warning but the bind still succeeds (warn, don't refuse).
- `stat_at_dir_observes_inode_swap` — building-block test that confirms
  the snapshot mechanism actually detects an inode change at a directory-
  relative name.
- `bind_hardened_never_silently_succeeds_under_swap_pressure` — adversarial
  stress: an attacker thread races to rename a decoy socket over the
  target while `bind_hardened` runs 200 times. Asserts the contract:
  every `Ok` return is accompanied by a socket of mode 0o600 at the path.
  Errors are allowed; silent corruption is not.
