# Socket bind hardening

This document describes a TOCTOU (time-of-check to time-of-use) vulnerability
that existed in the original Unix socket bind sequence, the attack surface it
exposed, the fix applied in `crates/weir-server/src/socket/mod.rs::bind_hardened`,
and the residual race window that the deployment is expected to close via
directory permissions. It also covers what the daemon does about a socket
that is already at the path: the liveness probe that tells a stale socket
from a live one, and the identity check that limits shutdown to removing
only the socket this daemon bound.

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
4. if exists and !is_socket:
     return error                  // refuse to remove anything that isn't a socket
   if exists and is_socket:
     probe_socket(P)               // connect-only liveness probe (see below)
       Live / Unknown → return error, leave the file
       Absent         → nothing to remove
       Stale          → re-fstatat by dirfd and lstat by path: both must
                        still name the inode from step 3, else error
                        unlinkat(dirfd, basename, 0)  // relative to dirfd
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
10. Linux only: openat(dirfd, basename, O_PATH | O_NOFOLLOW | O_CLOEXEC) → pin
    if fstat(pin) is not our_inode: bail (swap while pinning)
    return BoundSocket { listener, dev, ino, pin }
```

On targets other than Linux, step 2 opens the parent `O_RDONLY | O_DIRECTORY
| O_NOFOLLOW | O_CLOEXEC`, because there is no `O_PATH`. Step 10 has no
equivalent there; see [Own-inode-only removal at
shutdown](#own-inode-only-removal-at-shutdown).

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
- **Probe before unlink (step 4)**: in 4.0.x and earlier, step 4 unlinked
  *any* socket at the path, so a second instance pointed at a
  running daemon's `socket_path` silently took the path over. It now unlinks
  only a socket the probe calls stale. The probe resolves the path by name
  while the unlink goes through the dirfd, so both are re-checked against
  the inode first seen. A socket swapped in while the probe ran is refused
  (`the socket at '…' was replaced while probing whether it is in use;
  refusing to remove it`) rather than unlinked unseen.
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

## Stale socket removal and the liveness probe

A socket file at `socket_path` is either **stale** (a SIGKILLed or crashed
predecessor left it; nobody is listening) or **live** (another process,
usually another weir-server, is serving it). Only a stale one may be removed.
`probe_socket` (`crates/weir-server/src/socket/mod.rs`) tells them apart with
a real `connect(2)`. It opens `socket(AF_UNIX, SOCK_STREAM)`, non-blocking and
close-on-exec, connects to the path, and closes the socket. It never unlinks.
The socket must be non-blocking because, on Linux, a blocking `AF_UNIX`
connect to a listener with a full backlog hangs.

| `connect` result | Verdict | Startup action |
|---|---|---|
| `0`, `EAGAIN`, `EINPROGRESS` | `Live` (`EAGAIN` means a full backlog, so something is listening, on Linux) | Refuse: `socket_path '…' is in use by a running process; refusing to remove it. Stop the other weir-server or choose another socket_path.` |
| `ECONNREFUSED` on Linux | `Stale` | Unlink, INFO `removed stale socket` |
| `ECONNREFUSED` elsewhere | Stale *or* live, because BSD and macOS also refuse a stream connect when a live listener's backlog is full or it is bound but not listening | Settled by the attachment probe below; the stream verdict alone is never acted on |
| `ENOENT`, `ENOTDIR` | `Absent` | Nothing to remove; the bind reports any real problem |
| `EACCES`, `EPERM`, anything else | `Unknown(errno)` | Refuse (fail closed): `cannot tell whether socket_path '…' is in use: the liveness probe's connect failed with … (errno …); refusing to remove it. If no weir-server is using it, remove the file manually and start again.` |

A path too long for `sockaddr_un` is `Absent` if nothing is there and
`Unknown(ENAMETOOLONG)` otherwise. Preflight also refuses such a path before
the probe runs.

**The attachment probe (macOS and the BSDs).** On these kernels a refused
stream connect is ambiguous. The listener's accept backlog may be full: the
daemon is stuck in its `EMFILE`/`ENFILE` accept back-off, it is stopped, or it
is paused in a debugger. It may also be bound but not yet listening. A re-probe
window can only narrow that, because the backlog stays full for as long as the
listener cannot accept. So `probe_socket` settles the refusal with a second,
backlog-independent probe. It connects a non-blocking `SOCK_DGRAM` socket to
the same path.

The BSD-derived `unp_connect` (XNU, FreeBSD, OpenBSD, NetBSD) first looks up the
socket bound to the file's vnode and refuses with `ECONNREFUSED` only if there
is none. When a socket is bound, the type mismatch fails with `EPROTOTYPE`
*before* the stream-only backlog and `listen` checks are reached.

| datagram `connect` result | Verdict |
|---|---|
| `ECONNREFUSED` | `Stale`: no socket is attached to the file, so its owner has closed it or died |
| `EPROTOTYPE`, `0` | `Live`: a stream socket (or a datagram one) is bound there |
| `ENOENT`, `ENOTDIR` | `Absent`: the file went away between the two probes |
| anything else | `Unknown(errno)`: refuse (fail closed), as above |

A socket file is therefore removed on these targets only when *both* a stream
and a datagram connect are refused. The datagram probe never creates a
connection on the other side, so it adds no connection metrics and no
`peer_uid_check` log.

This was observed on macOS (XNU 12377, Darwin 25.6). A listener with a full
backlog, and a socket that is bound but not listening, both answer the stream
connect with `ECONNREFUSED` and the datagram connect with `EPROTOTYPE`. A
dropped listener's file, and a SIGKILLed process's file, answer `ECONNREFUSED`
to both. The unit tests `full_backlog_listener_is_live_not_stale` and
`bound_but_not_listening_socket_is_live_not_stale` pin this.

For FreeBSD, OpenBSD and NetBSD the same ordering is taken from their
`unp_connect` source; it is untested here. Those targets refuse every
connection anyway ([platform support](../platform-support.md)). On Linux the
stream verdict is unambiguous (a full backlog is `EAGAIN`, so `Live`), and the
attachment probe does not run there.

An earlier version re-probed a refused socket three times at 50 ms intervals
and called it stale if every probe was refused. A live daemon whose backlog
stayed full for 150 ms lost its socket to a second instance that had the same
`socket_path` but a different `wab_dir`. The `wab_dir` lock cannot catch that
case (conf-security-7).

The probe runs twice on a normal start:

1. **Preflight**, before the WAB is read. It refuses a non-socket entry, then
   a `Live` or `Unknown` verdict. It does not unlink a stale socket; that is
   left to the bind. This is what makes a duplicate instance refuse *before*
   its recovery and drain start (see the [threat
   model](threat-model.md#single-ownership-of-wab_dir-and-socket_path)).
2. **`bind_hardened` step 4**, after recovery. This is the authoritative
   check. It unlinks a stale socket through the dirfd, after re-checking that
   the path still names the inode it probed.

**Side effects.** Against a live daemon, the probe is a real connection.
That daemon's connection metrics count it. If the prober runs as a different
uid, the live daemon's `peer_uid_check` refuses it and logs `refusing
connection: peer uid does not match daemon uid` (counted in
`weir_connection_rejected_peer_uid_total`). A daemon that is shutting down
keeps its listener open while in-flight connections drain (up to
`shutdown_timeout_secs`), so a probe in that window sees it as live and the
new instance refuses. Under systemd the old process has exited before the
new one starts.

## Own-inode-only removal at shutdown

At shutdown the daemon's own listener is still open, so the probe would call
its own socket live. Shutdown therefore uses identity, not liveness.
`bind_hardened` returns a `BoundSocket` carrying the `(dev, ino)` of the
inode it bound and verified. `remove_own_socket` opens the parent dirfd and
`fstatat`s the path (no symlink follow). It `unlinkat`s the path only if the
entry is still a socket with that identity. A missing file is fine. On a
mismatch, such as a successor that unlinked this daemon's socket and bound
its own, it logs WARN `socket path now names a different socket (another
instance replaced it); leaving it` and removes nothing. Any failure is
logged, never fatal, because the daemon is exiting.

**Linux: the `O_PATH` pin.** `(dev, ino)` alone is not an identity if the
inode number can be reused, and ext4 and xfs reuse freed inode numbers
promptly. Immediately after the late-swap check (step 9), `bind_hardened`
opens an `O_PATH | O_NOFOLLOW | O_CLOEXEC` fd on the bound socket inode,
checks it is the verified inode, and holds it for the listener's life. While
the fd is open the inode cannot be freed, so a successor's bind cannot
receive the same number, and `(dev, ino)` is sound. If the `O_PATH` open
fails outright, startup continues without the pin and logs WARN `could not pin
the bound socket inode with O_PATH; shutdown will identify the socket by
(dev, ino) alone`.

**Other unix targets: a best-effort identity.** macOS and the BSDs have no
`O_PATH`, so nothing pins the inode. The comparison adds `st_ctime`, and on
macOS also `st_birthtime`, as tie-breakers. That makes a false match unlikely
but not impossible. A successor whose socket received the freed inode number
*and* matching timestamps would be unlinked at this daemon's shutdown. This
is a limitation, not a supported guarantee. On these targets, do not unlink
and re-create a running daemon's socket path.

The residual `fstatat` → `unlinkat` window at shutdown is the same class as
the `bind(2)` → `fstatat` window above, and the same private-parent-directory
assumption closes it.

## What is tested

The unit tests in `crates/weir-server/src/socket/mod.rs::tests` cover:

- `bind_hardened_refuses_regular_file_at_socket_path` — Scenario A guard:
  refuses to remove a non-socket file.
- `bind_hardened_refuses_symlink_at_socket_path` — Scenario B guard:
  refuses to remove a symlink (even one pointing to a real socket).
- `bind_hardened_replaces_stale_socket` — happy path: stale socket at
  the path is cleanly replaced.
- `connect_errno_classification_matches_the_probe_table`,
  `probe_classifies_live_stale_and_absent`,
  `probe_reports_absent_when_parent_missing` — the probe table above, and
  that the probe never unlinks.
- `startup_cleanup_unlinks_a_dropped_listener_socket` /
  `startup_cleanup_refuses_a_live_listener` — a stale socket is removed, and
  a live one is refused and still accepts afterwards.
- `remove_own_socket_unlinks_our_socket`,
  `remove_own_socket_keeps_a_replaced_socket`,
  `remove_own_socket_refuses_non_socket`, `remove_own_socket_missing_is_noop`
  — shutdown removes only the inode it bound.
- `full_run_start_then_shutdown_removes_its_own_socket` — the regression
  for the self-probe trap: a clean `socket::run` start and shutdown leaves no
  socket behind.
- `o_path_pin_opens_on_socket_inode` (Linux) — the `O_PATH` pin opens on a
  socket inode.
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

The system tests in `crates/weir-server/tests/system.rs` cover the same
behaviour end to end:

- `second_daemon_refuses_a_live_socket` — a second daemon with its own
  `wab_dir` and metrics port but the first one's socket exits non-zero with
  `in use by a running process`, and the first daemon still accepts a
  Durable push.
- `refused_duplicate_never_delivers_shared_socket` — that refusal comes from
  preflight: a pre-seeded backlog in the refused daemon's `wab_dir` delivers
  nothing to its sink.
- `shutdown_leaves_a_successor_socket_alone` — a socket bound at the path
  after the daemon's own was unlinked survives the daemon's SIGTERM.
- `starts_cleanly_over_a_stale_socket_left_by_sigkill` — the stale-unlink
  path after a real crash.

## Known limitation — the umask window is process-global

The bind sequence tightens the process umask to `0o177` around `bind(2)` so the
socket inode is created at mode `0o600` without a post-bind `chmod`. The
tightening is RAII-scoped to that single syscall, but umask is a **process**
attribute, not a thread attribute, so it applies to every thread for the
duration.

Specifying mode bits explicitly does not escape it — `mkdir(2)` and `open(2)`
both mask the requested mode. A directory created by another thread inside the
window lands at `0o700 & !0o177 = 0o600`, losing its execute bit; a 0o600 file
(a WAB segment, a `.confirmed` sidecar) is unaffected (`0o600 & !0o177 = 0o600`);
WAB directories go through `wab::create_dir_private`, which chmods after mkdir.

**In the daemon this is not exploitable and not reachable.** The bind happens
before any producer can connect, so no records exist and no segment is being
created. The WAB flushers and workers are running by then, but idle. (The
earlier justification — that startup is single-threaded — was wrong: `main.rs`
calls `wab::spawn` and then `worker::spawn_workers`, both well before
`socket::run` binds. The correct reason is that the process is *idle*, not that
it is single-threaded. Symbol names rather than line numbers, deliberately: the
numbers this paragraph originally carried had drifted by ~250 lines before anyone
noticed, which made a true claim look false.)

**Under `cargo test` it is reachable and constant**, because ~300 unrelated
tests create directories concurrently with ~50 tests that call `bind_hardened`.
A parallel run of the `weir-server` bin target produces ~66 spurious
`PermissionDenied` failures and leaves `crates/weir-server/proptest-regressions/`
unwritable, so proptest cannot record failing seeds. That is why the documented
gate runs this target with `--test-threads=1`.

Removing the umask dependency is a security redesign, not a cleanup: `fchmod`
operates on the sockfs object rather than the bound inode on Linux, and a
path-based `chmod` reintroduces the symlink-swap window analysed above. The
alternative worth evaluating if this is ever revisited is binding to a private
name inside the already-`0o700` parent directory and publishing it with
`renameat`, which would need its own analysis against the attack surface
described in this document.
