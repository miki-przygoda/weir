# weir Safety Floor: wab_dir ownership, SIGHUP, socket ownership, and drain resend loops (SP-A + SP-B)

Status: design, revision 2, ready for owner review. Base: main @ 18c931a (4.0.1). It ships as its own small release alongside Foundations (SP-D/E/F). Every line reference was re-read at this commit. Revision 2 applies two reviews (invariant/concurrency red team; code fit and feasibility). Rejected review items are listed separately with reasons. Where a research claim did not hold, section 4.10 says so.


> **Owner review status (2026-10-05).** The owner approved this spec in advance ("I approve the spec") and went AFK, granting full autonomy to plan and implement it. Every open owner decision below was therefore taken **autonomously, using the recommended option**, and is flagged for the owner to confirm or overturn. Nothing here was pushed or released.
>
> | # | Decision | Taken (recommended) |
> |---|---|---|
> | D1 | Release vehicle | Ship ahead of Foundations as its own minor release (tentatively 4.1.0; Foundations then 4.2.0). The version number is chosen at release time; changes go under `[Unreleased]`. |
> | D2 | Filesystems that cannot flock | Fail closed by default; `wab_dir_lock = "best-effort"` opt-out; contention is never overridable |
> | D3 | Lock contention | Fail fast; a bounded wait is revisited with the k8s manifests |
> | D4 | Lock file deleted/replaced while running | Split by evidence: re-lock if nobody else holds it, fail closed (exit 3) on proven loss |
> | D5 | Persistent non-corruption read errors | By class: `Process` never escalates, `Access` parks, `Media` quarantines, once the error has lasted **both** 3 strand cycles **and** `LOCAL_READ_ESCALATION_MIN_AGE` (10 min) since the segment's first local read failure in the run. **The 10-minute floor was added after review (conf-regressions-8) as a further autonomous decision; owner to confirm.** See §4.1. |
> | D6 | Plain-mode duplicate key | `id_column` set: set-based lookup, dups count as committed; no `id_column`: bisection, dup rows dead-lettered; config-load WARN for plain mode without `id_column` |
> | D7 | Orphan sealed segments with a valid `.confirmed` | Delete them (lock held only) |
> | D8 | Ordering wording | Per-connection order is WAB/ingest order; delivery may reorder |
> | D9 | `ExecReload=` in weir.service | Not now; with SP-L |
> | D10 | Explicit missing `--config` | WARN now, error in a later minor alongside a Dockerfile CMD change |
> | D11 | Local-I/O strands and `weir_drain_segments_stranded_total` | Counted separately in `weir_drain_local_io_strands_total` |

---

## 1. Context

Foundations makes weir observable and controllable. The safety floor removes the failure modes that no amount of observability excuses.

- **A live crown-invariant breach (SP-A), reproduced by the owner.** Daemon B was started on the same `wab_dir` as a running daemon A, with its own socket and metrics port.
  1. B ran startup recovery on A's ACTIVE `seg_00000001.wab`.
  2. B sealed it, and B's drain delivered, confirmed and deleted it.
  3. A then acked 50 more Durable records into the unlinked inode. None were delivered and nothing remained on disk.
  4. A's shutdown seal logged `No such file or directory`.

  Code path:
  - Startup is `Config::load` (main.rs:452), then `wab::spawn` (main.rs:559).
  - `wab::spawn` runs `recover_open_segments` (wab/mod.rs:276) before any bind. Recovery truncates, adds a footer to and renames every `*.wab` (recovery.rs:103-120, 640-656).
  - A's segment fd has no `O_APPEND` (segment.rs:64-69), so its next writes overwrite the footer region, while fdatasync still succeeds and acks.
  - The replay (main.rs:885) and the metrics bind (main.rs:899) both come after the damage.
  - The code's own safety argument assumes a single writer: "Single-writer-per-shard" (segment.rs:284). Nothing in `crates/*/src` calls flock.
- **SIGHUP kills a daemon that is not running TLS (exit status 129), and systemd does not restart it.**
  - The only `hangup()` registration is `spawn_tls_reload_task` (main.rs:357-394). It is called only inside the `Some(bind_addr)` arm of the `all(unix, feature="tls")` block (main.rs:1143).
  - The default features have no tls, and `tcp_bind` defaults to None.
  - `Restart=on-failure` (weir.service:71) treats death by SIGHUP as a clean exit.
  - The TLS design spec promised "SIGHUP is ignored with a debug log" when `tcp_bind` is unset (2026-06-10-tcp-tls-listener-design.md:143-144). That promise was never implemented.
- **A live socket can be stolen.**
  - `cleanup_existing_socket` (socket/mod.rs:555-601) unlinks any `S_IFSOCK` entry without checking whether anyone is listening.
  - Shutdown removes the socket by name. `remove_socket_hardened` (socket/mod.rs:298-312) is a thin wrapper over the same `cleanup_existing_socket`, so a daemon that exits later can delete its successor's socket.
  - `socket_takeover_does_not_corrupt_wab_data` (tests/system.rs:2170-2256) codifies the takeover.
- **Drain resend loops cause unbounded duplicate delivery and false corruption alarms (SP-B).** There are seven verified defects in drain/mod.rs, confirmed.rs and the SQL sinks. Review found three more of the same class: a torn sidecar, a dead-letter write failure after commit, and a deterministic drain panic. There is also one liveness-signal defect, which is a hard prerequisite for Foundations' liveness: the drain heartbeat is not bumped inside `process_segment`, and nothing tells a slow sink or a slow disk apart from a wedge.

## 2. Goals and non-goals

**Goals**

- **G1.** At most one daemon touches a `wab_dir` at a time.
  - Ownership is established before any byte of the WAB is read, and held until the process is gone.
  - A second instance fails fast and names the holder.
  - A daemon that *proves* it has lost ownership stops acking and exits.
- **G2.** SIGHUP never terminates weir.
  - It is ignored from the first instruction of `main`.
  - Once the runtime exists it is handled: TLS reload when configured, otherwise a logged and counted no-op.
  - There is one reload code path, which the Foundations admin `reload` verb reuses.
- **G3.** weir never unlinks a socket someone is listening on. At shutdown it never unlinks a socket it did not bind.
- **G4.** Every drain loop that re-delivers already-delivered records on a cadence becomes bounded. Within one process lifetime, no committed record is re-committed because of a strand, a sidecar failure, a quarantine-move failure, a dead-letter write failure or a dead-letter block. Local I/O blips never become "corruption" quarantines. Format corruption (`InvalidData`/`UnexpectedEof`) is quarantined on the first attempt; persistent media errors (EIO class) are quarantined after a bounded number of cycles.
- **G5.** The drain publishes a truthful liveness signal: heartbeat progress inside a segment, plus "blocking operation of kind K in flight since T". A slow sink or a slow disk never reads as `Wedged`.
- **G6.** Every refuse-to-start condition this spec introduces is evaluated before recovery and before the drain thread starts (preflight). No new refusal can fire after delivery has begun.
- **G7.** Constraints that hold throughout:
  - The ack path is untouched apart from one wait-free `AtomicBool` load (3.1, ownership lost).
  - No wire changes.
  - Metrics and config changes are additive only.
  - No new crate dependencies.
  - MSRV 1.88. `std::fs::File::try_lock` is stable only from 1.89, so the lock uses `libc::flock`; `libc` is already a dependency (Cargo.toml:79).

**Non-goals**

- General config reload (SP-L).
- Lock wait or handoff for zero-downtime (later, D3).
- Deferring SIGTERM during startup, lame-duck and bounded shutdown (SP-F).
- Moving the replay into the drain (SP-F).
- The `DrainControl` command channel itself (SP-F). This spec only states what it must reach (section 5).
- Cross-strand cool-down and breaker (SP-H).
- Sink reclassification (SP-C).
- Worker or flusher supervision budgets (SP-G).
- Persisting the drain cursor to disk. Explicitly never in this release; the 4.0 cursor is in memory only.
- NFS support beyond "not a supported guard".

---

## 3. SP-A: ownership, preflight and signals

### 3.0 Preflight (new, G6)

**Current behaviour.** The later refusals happen after recovery, the drain start and the replay:
- Metrics port in use: main.rs:899-912.
- TLS load failure: main.rs:~1112.
- TCP bind failure: main.rs:~1118.
- `socket::run` → `validate_socket_path` / `bind_hardened`: main.rs:1215.

When one of them fires, `main` returns through `?` while the drain is mid-commit and the flushers are detached (main.rs:1236). Under `Restart=on-failure` (weir.service:71-72, RestartSec=5) or compose `restart: unless-stopped`, each restart re-commits the head of B's oldest unconfirmed segment and is killed before it confirms. The result is unbounded duplicate delivery at the restart cadence. The metrics-port case already has this hole today. The live-socket refusal in 3.3 would add a second, more likely trigger, because "a different `wab_dir` with the same `socket_path`" is the documented mistake.

**Change.** Add `fn preflight(config: &Config) -> io::Result<Preflight>` in a new `crates/weir-server/src/preflight.rs`. It is called right after `acquire_wab_dir_lock` and before `Metrics::new` and `wab::spawn`. Steps, in order:

1. `socket::validate_socket_path` (pure, socket/mod.rs:349-373).
2. A **read-only liveness probe** of `socket_path`: `probe_socket(path) -> SocketProbe { Absent, Stale, Live, Unknown(errno) }`, using the 3.3 table, connect only and **never unlink**. `Live` or `Unknown` refuses with the 3.3 message.
3. A test-bind of the metrics listener (`std::net::TcpListener::bind((metrics_bind, metrics_port))`), dropped immediately. On failure it refuses with today's message (main.rs:902-910).
4. Under `all(unix, feature="tls")` with `tcp_bind` set: validate the TLS material with a throwaway `ReloadableServerConfig::load(cert, key, ca)`, then test-bind `tcp_bind` and drop it.

The authoritative operations stay where they are: the real metrics and TCP binds, the real TLS load, and `bind_hardened` with probe-and-unlink at main.rs:1215. The producer socket must still bind only after recovery has sealed open segments (segment-counter safety, wab/mod.rs:671-702). Preflight narrows the late-failure window to a race (another process binding in the meantime), which is acceptable.

**Why test-bind and release instead of holding the listener.** Holding a bound but unserved metrics listener through a long replay makes scrapes hang until timeout instead of being refused. SP-F replaces the test-bind with a real early bind that is also served.

**Pre-existing late failures this spec does not close** (listed in Unresolved):
- `wab::replay_unconfirmed(..)?` (main.rs:885) on a scan error.
- `bind_hardened` errors other than a live socket.

Both are closed by SP-F, because replay moves into the drain and lame-duck changes the bind. **Contract for SP-F:** no refuse-to-start check may run after the drain has begun delivering.

**Test (system).** `refused_duplicate_never_delivers`:
1. Start A.
2. Start B with A's `socket_path`, its own `wab_dir` pre-seeded with a 3-segment sealed backlog, and an HTTP recording sink.
3. Assert that B exits non-zero within 5 s with `in use by a running process`, and that the recording sink received **0** requests.

Add the same test with a shared metrics port.

### 3.1 The wab_dir lock

**Current behaviour.** There is no lock. Recovery runs inside `wab::spawn` (main.rs:559, wab/mod.rs:262-276) before any bind, and a second instance seals the live daemon's active segments (section 1).

**Change.** A new module `crates/weir-server/src/wab/lock.rs` exposes:

```rust
pub(crate) fn acquire_wab_dir_lock(wab_dir: &Path, policy: LockPolicy, holder: &HolderInfo)
    -> Result<LockOutcome, LockError>;          // LockOutcome { Held, Unsupported } (best-effort only)
pub(crate) fn check_wab_dir_lock(wab_dir: &Path) -> LockIntegrity; // Ok | Relocked | LostToOther{holder} | Error(errno)
```

- **Path.** `<wab_dir>/.weir.lock`. `wab_dir` is already canonicalised and must exist (config/mod.rs:345, 632-633, 1189-1211). A dotfile is chosen on purpose: `rm -rf wab/*` does not match it under default shell globbing.
- **Open.** `OpenOptions::new().read(true).write(true).create(true).mode(0o600).custom_flags(O_NOFOLLOW)`. std sets `O_CLOEXEC`, and weir spawns no children. Then `fstat`, and refuse if the result is not `S_IFREG`. A mode other than 0600 gets a WARN but does not refuse, mirroring `audit_segment_modes` (recovery.rs:60-101).
- **Lock.** `libc::flock(fd, LOCK_EX | LOCK_NB)`:
  - `0`: the lock is held. Write the holder record with `ftruncate(0)` and then `pwrite` at offset 0, **only after** flock succeeds, so a refused contender never truncates the holder's record. The record is informational, so there is no fsync.
  - `EWOULDBLOCK` (contention): read the holder record (best effort) and return `LockError::Held`. **Contention is never overridable.**
  - `ENOLCK`, `EOPNOTSUPP`, `ENOTSUP` (distinct from EOPNOTSUPP on macOS, a shipped target), `ENOSYS` or `EINVAL` (the filesystem cannot lock): with `wab_dir_lock = "required"` (the default), refuse to start. With `"best-effort"`, WARN on every start, set the gauge to 0, and continue (D2).
  - Anything else refuses. Examples: EACCES on an existing root-owned lock, EROFS, ELOOP from a symlinked lock path, and EPERM from a seccomp filter.
- **Vetting the opened file (revised in review: conf-security-5, cont-security-2).** Before the flock, acquisition refuses (exit 1, `LockError::Unsafe`, message starting `cannot lock wab_dir`) a file with more than one hard link (the record write would truncate every other name for it) and a file whose owner is not the daemon's euid (that user could loosen its mode and hold the lock while the daemon is down). An open that fails with EACCES/EPERM on a file another uid owns gets the same owner refusal, which names the uid and the `chown` that fixes it (the `sudo weir-server` leftover). Nothing is written to a refused file. Once the flock succeeds, a file of ours whose mode is not 0600 is `fchmod`ed to 0600 with a WARN. The record read back is untrusted: at most 4 KiB is parsed, control and bidi formatting characters are stripped, and a value over 256 bytes is dropped.
- **Holder record.** Plain `key=value` lines, no serde, parsed best effort:
  ```
  pid=…
  host=…          # libc::gethostname; equals the pod name in k8s
  started_unix=…
  boot_id=…       # Linux only, /proc/sys/kernel/random/boot_id
  version=4.1.0
  socket_path=…
  metrics=…:…
  ```
  The pid alone means nothing across pid namespaces; host plus start time plus boot_id identify the holder. The daemon caches its own record in memory and **never re-opens `.weir.lock` in-process** (see NFS below). This is enforced by a code comment on the module and by a unit test that greps `lock.rs` for any second `open` of the path.
- **Refusal message.** It is logged at ERROR and returned from `main` (stderr, exit 1):
  > `wab_dir /var/lib/weir/wab is locked by another weir-server (pid 1234 on host weir-0, started 2026-10-04T10:00:00Z, version 4.1.0, socket /run/weir/weir.sock). Two daemons on one wab_dir corrupt each other's segments and can lose acked records. Stop the other instance or point this one at a different wab_dir.`
- **Placement.** Immediately after the tracing init and the `weir starting` log line (main.rs ~452-485), and before preflight (3.0), `Metrics::new` (main.rs:~506), the drain channel (main.rs:518) and `wab::spawn` (main.rs:559). The lock comes before every other preflight step on purpose, so a duplicate on the same `wab_dir` reports "locked by" rather than "metrics port in use" or "socket in use".
- **Storage and lifetime.**
  - `static WAB_DIR_LOCK: std::sync::Mutex<Option<File>>` (const-constructible). It is touched only at acquisition and by the integrity thread, never by the ack path or the drain. (As built, the slot is a `HeldLock`: the held `File` plus any alias fds proven to name it, below.)
  - It is never released explicitly. The kernel releases it at process exit, which is strictly after the flusher joins and the drain join (main.rs:1252-1258), and it also covers early `?` returns from `main`. A stack guard would drop the lock on those returns while detached flusher and drain threads were still running.
  - A successful re-lock (below) replaces the `File`. The old fd refers to an unlinked inode nobody else can reach, so dropping it is harmless.
  - The lock file is never unlinked: unlink-on-exit opens the classic two-inode race.
- **Integrity check** (it guards against the lock being defeated by deleting or replacing the file).
  - It runs on **its own** std thread, `weir-lock-integrity`, every 1 s. It is not in the 5 s WAB poll, because that task `continue`s past everything when the scan returns `None` (main.rs:990-994), and 4.9 makes `None` more likely. (Revised in review, conf-durability-3: the first design was a tokio `interval` task spawned in `block_on`, which started only after recovery and the startup replay. During a long replay neither the socket nor the metrics port is bound, so a duplicate with an identical config passes preflight and the ~1 s window became unbounded. The thread is spawned in `main` right after `acquire_wab_dir_lock` when the lock is `Held`, checks once at once, and a panicking check is caught and retried.)
  - Each tick does three things:
    1. `lstat(path)`, comparing (dev, ino) with `fstat(held fd)` and with any alias id already proven to name the held file.
    2. Re-issue `flock(held_fd, LOCK_EX|LOCK_NB)`. On a local filesystem this is a no-op conversion that returns 0. Where flock is emulated with POSIX locks (NFS), it returns EWOULDBLOCK after a lease loss once another client holds the lock.
    3. Classify the result:
       - Match and 0 → `Ok`.
       - Held-fd flock `EWOULDBLOCK` → `LostToOther`.
       - Mismatch or ENOENT → the re-create, which as built (revised in review) is:
         - **ENOENT**: create with `O_CREAT|O_EXCL`, so a file planted after the `lstat` (a hard link included) is never adopted; EEXIST is an error, and the next tick inspects that file as a replacement.
         - **Mismatch**: open without creating. If its `fstat` id equals the held fd's, the path is the held file and `lstat` and `fstat` merely disagree (overlayfs before Linux 4.19; conf-regressions-12): `Ok`, and the proving fd is kept as an alias (closing it could drop a POSIX-emulated lock) so later ticks open nothing.
         - Then, on a regular file: `flock`; `EWOULDBLOCK` → `LostToOther`. Then read the record **before** writing anything: a record naming another daemon (pid, host, start time or boot id differ) is proof one held the directory and may have recovered our live segments before exiting → `LostToOther`, record left in place as evidence (conf-durability-2). Only then the hard-link and owner vet, as the gate on writing our record: a failure is an error, and the file is untouched. The proof comes first because a second daemon running as another uid owns its file, and a hard link to its file has two names; vetting first turned that proof into an error retried for ever while acks continued.
         - Otherwise (an empty or unparseable file, or a copy of our own record) the record is written → `Relocked`. ERROR log, `weir_wab_dir_lock_integrity_failures_total` +1, the gauge stays 1. No fail-stop: the file was deleted by hand and nobody else holds it (D4a).
       - `LostToOther` is **proof that a second writer owns the directory.** See fail-closed below (D4b).
       - Any other errno → ERROR log and counter. Retry on the next tick.
- **Fail-closed on proven loss (D4b).** On `LostToOther`:
  1. Store `true` into `wab_ownership_lost: Arc<AtomicBool>`. It is created next to `wab_cap_rejecting` (main.rs:952) and checked in `socket/connection.rs` immediately before both `over_wab_cap` call sites (connection.rs:356, 404) and in the TCP handler. New pushes are NACKed with the existing `WireNack::InternalError` (no wire change) and counted under the existing `records_nack`. This is one `Relaxed` load per push; it takes no lock and cannot delay acks.
  2. Set `weir_wab_dir_lock_held` to 0 and log CRITICAL with B's holder record.
  3. `std::process::exit(EXIT_WAB_OWNERSHIP_LOST)` (= 3) **immediately**, without sealing: sealing would rename files B now owns. There is no grace period, because a grace only lengthens the window of knowing false acks.

  Under systemd the restart is then refused with "locked by" every RestartSec (5 s), which is safe and loud. Durable records A acked before the exit are fsynced into files that B now recovers and delivers; records acked into an inode B already deleted are the breach.
- **Residual window (stated in threat-model.md).** From the moment `.weir.lock` is deleted until A's next tick (at most about 1 s), a B that starts in that window acquires a fresh lock and runs recovery on A's active segments. Durable records A acks between B's recovery and A's exit can be false acks. Detection cannot close this window. Only "never delete `.weir.lock`" can, and the docs say so.
- **Who must not lock.**
  - weir-ctl never calls flock, not even `LOCK_SH|LOCK_NB` probes. A momentary probe can make a starting daemon fail with "locked by", and the offline tools are designed to run beside a live daemon.
  - Foundations' `weir-ctl status` may read `.weir.lock` as the *recorded* owner and must label it as such. Liveness comes from the socket, admin or metrics probes.
  - An in-daemon `/status` handler must use the cached record and never open the file.
- **Walkers are already immune** to a regular file in the `wab_dir` root:
  - `recover_open_segments` (recovery.rs:40-42 `!is_dir`)
  - `scan_unconfirmed_sealed` (wab/mod.rs:390)
  - `compute_wab_bytes_on_disk` (main.rs:90-92)
  - weir-ctl `scan_segments` (main.rs:491-493) and `shard_dirs` (attest.rs:84)

  Add a unit test per walker so a future walker cannot regress this.

**Invariant analysis.**

- The lock is protective except for the new fail-stop, which converts a proven false-ack state into an outage. That is the intended trade.
- The new availability failure modes are:
  - refusing to start on a filesystem without locks (D2);
  - a stale root-owned lock file (clear message);
  - flock blocked by seccomp (EPERM; gated in section 7).
- flock locks the inode on the local kernel, so bind mounts, overlayfs, symlinked paths and containers on one host all conflict correctly.
- **Network filesystems.** NFS with `nolock` gives local-only locks *silently*. With lockd or NFSv4, Linux emulates flock using whole-file POSIX locks:
  - closing *any* fd to the file anywhere in the process drops the lock, which is why the file is never re-opened in-daemon;
  - a lease expiry can drop the lock unseen, which is partially covered by the held-fd re-flock.

  platform-support.md:133-135 already lists network filesystems as unsupported. It now says explicitly that the lock may be *granted* there with weaker semantics and is not a supported guard.

**Tests**

- Unit (lock.rs):
  - A second `acquire` in the same process (a separate open file description) gets `Held` with the holder parsed.
  - A symlinked `.weir.lock` is refused (ELOOP).
  - A directory at `.weir.lock` is refused.
  - The holder record round-trips.
  - A refused contender does not modify the holder record (byte compare).
  - Injected `ENOLCK` and `ENOTSUP`, through a `cfg(test)` flock hook, refuse under `required` and pass with a WARN under `best-effort`.
  - `check_wab_dir_lock`: unlink and recreate gives `Relocked`; unlink plus a second process-local OFD taking the new file gives `LostToOther`. As built, also: a replacement nobody holds carrying another daemon's record gives `LostToOther`; one held by, or left by, a daemon of another uid, or hard-linked, gives `LostToOther`; a foreign-owned or hard-linked replacement with no such evidence is an error and untouched; an `lstat`/`fstat` disagreement on our own file is `Ok`.
  - No in-daemon re-open of the lock path.
- Unit (connection.rs): with `wab_ownership_lost=true`, a Durable push is NACKed `InternalError` before reaching the queue.
- System (tests/system.rs): `second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking`, the owner's reproduction as a regression.
  1. Start A with a **recording HTTP sink**. The harness default is the noop sink (config/mod.rs:1344), and graceful shutdown deletes every delivered segment, so asserting on WAB bytes after SIGTERM cannot work.
  2. Push 20 Durable records.
  3. Snapshot A's tree, including names, sizes and a content hash.
  4. Spawn B with the same `wab_dir`, its own socket and `free_port()` metrics, through a **new testkit helper** `WeirServerBuilder::spawn_expecting_exit(timeout) -> (ExitStatus, String /*log*/)`. `start()` cannot be used: `wait_ready` panics on early exit (harness.rs:255-272), and reusing the tag shares A's tmp dir (harness.rs:442-444). The testkit is `publish = false`, so the helper costs nothing.
  5. Assert that B exits non-zero within 5 s, that its log contains `locked by` and A's pid, and that A's tree is byte-identical.
  6. Push 50 more Durable records to A, then SIGTERM A.
  7. Assert that the recording sink holds all 70 payloads, and that A's log has no `No such file or directory` on seal.
- System: `lock_file_stolen_by_second_daemon_fails_closed`.
  1. Start A.
  2. Unlink `.weir.lock`.
  3. Start B, which must acquire the lock.
  4. Within 2 s, A exits with status 3 and logs CRITICAL. A push to A during that window gets a NACK, never an ack.

### 3.2 SIGHUP

**Current behaviour.**
- No disposition is set before the runtime is created at main.rs:890, which happens after recovery and the blocking replay.
- After the runtime, a handler exists only with TLS plus `tcp_bind` (main.rs:1143).
- SIGTERM and Ctrl-C are installed at main.rs:1172-1197.
- In Docker, weir is PID 1 (Dockerfile:134, exec-form CMD with no init), so the kernel drops a SIG_DFL SIGHUP. The kill is real on systemd hosts, under `shareProcessNamespace`, under an init or shell wrapper, and on bare runs.

**Change.**

1. **Ignore SIGHUP first.** At the top of `main`, in the same `#[cfg(unix)] unsafe` block as `umask(0o077)` (main.rs:447-450), add `libc::signal(libc::SIGHUP, libc::SIG_IGN)`. It precedes `Config::load`. A HUP during recovery or replay is dropped. That is safe because the real TLS load still happens later and reads the current files; the preflight load (3.0) is a throwaway validation.
2. **One reload path.** A new `crates/weir-server/src/reload.rs`:

   ```rust
   pub(crate) enum ReloadTrigger { Sighup, Admin }          // Admin used by Foundations
   pub(crate) enum ReloadOutcome { Ok, Noop, Failed }
   pub(crate) struct ReloadReport { trigger: ReloadTrigger, outcome: ReloadOutcome,
                                    changed: Vec<&'static str>, rejected: Vec<(&'static str, String)>,
                                    note: Option<&'static str> }
   pub(crate) struct Reloader { tls: OnceLock<ReloadableServerConfig> /* cfg(tls) */, metrics: Arc<Metrics> }
   impl Reloader { fn register_tls(&self, t: ReloadableServerConfig); fn reload(&self, t: ReloadTrigger) -> ReloadReport }
   pub(crate) struct ReloadRequest { trigger: ReloadTrigger, reply: Option<oneshot::Sender<ReloadReport>> }
   ```

   - The `Reloader` is shared as `Arc<Reloader>`. `reload_loop` holds one clone and drains a `tokio::sync::mpsc::channel::<ReloadRequest>(8)`, so reloads are serialized. The TLS branch holds another clone for `register_tls`.
   - The SIGHUP task (`signal(SignalKind::hangup())`) is spawned **first thing inside `rt.block_on`**, before the metrics bind. It only enqueues `ReloadRequest{Sighup, None}`.
   - The TLS branch calls `reloader.register_tls(tls.clone())` (a `OnceLock::set`) at the point where it calls `spawn_tls_reload_task` today (main.rs:1143). `spawn_tls_reload_task` is deleted.
   - Outcomes:
     - **TLS registered:** `tls.reload()`, with the unchanged fail-safe semantics. It bumps the existing `weir_tls_config_reloads_total{outcome}` (contract unchanged) and the new counter.
     - **TLS configured but not yet registered** (a HUP in the milliseconds between the task spawn and main.rs:1143): `outcome=noop`, `note="tls_not_loaded_yet"`. This is accurate because the pending load reads the current files.
     - **Nothing reloadable:** `outcome=noop`, logged at INFO: `SIGHUP received: nothing reloadable in this build/config (config is read once at startup); nothing changed`.
   - **Foundations seam.** The admin `reload` verb sends `ReloadRequest{Admin, Some(reply)}` and serializes the `ReloadReport` as its JSON answer, so SIGHUP and admin share one code path by construction. SP-L later extends `Reloader::reload` with the allowlisted keys. No lock is shared with the ack path; `OnceLock::get` is wait-free.
3. **New metric** `weir_config_reloads_total{trigger="sighup",outcome="ok|failed|noop"}`. The `admin` label value is reserved.
4. **Do NOT add `ExecReload=` to weir.service yet (D9).** deploy/systemd/README.md:51 documents `systemctl reload-or-restart weir` as a restart ("config is read once at startup"). Adding ExecReload now would silently turn that into a no-op reload while operators believe the config changed.

**Invariant analysis.** None on acks. The change removes an abrupt-exit path that skipped seal and drain. Durable records were fsynced anyway; Buffered records survived in the page cache.

**Tests (system).** No SIGHUP system test exists today (grep finds none in tests/).

- `sighup_is_a_counted_noop_on_a_default_daemon`:
  1. Send `kill -HUP` and wait 300 ms.
  2. Assert the process is alive, a Durable push succeeds, and `weir_config_reloads_total{trigger="sighup",outcome="noop"} 1`.
- `sighup_reloads_tls_when_configured`:
  1. Under the tls feature, use the harness `.tls()` fixture.
  2. Rotate the cert files and send HUP.
  3. Expect `outcome="ok"` on both counters, and a new handshake sees the new cert.
- `sighup_during_startup_replay_is_ignored`. The premise of revision 1 was wrong: a connection-refused sink fails fast, and the RetryingTransient wait pulls the channel into `pending` (drain/mod.rs:782-805), so the replay never blocks. Revised version:
  1. Use a **black-hole HTTP listener** that accepts and never responds, with a large `sink_timeout_secs`, so `commit` waits.
  2. Pre-seed more than 257 sealed segments, so `replay_unconfirmed` blocks on send.
  3. Wait for the log line `queuing segment for drain replay` (this asserts the phase), then send HUP and assert the pid is alive.
  4. Close the black hole, then assert the socket binds and the process stays up.
- A second variant sends HUP during `recover_open_segments` (many large `.wab` files pre-seeded) before the runtime exists.

### 3.3 Socket ownership

**Current behaviour.**
- `bind_hardened` (socket/mod.rs:400) calls `cleanup_existing_socket` (555-601), which unlinks any `S_IFSOCK` entry.
- After the bind it already snapshots `our_inode` (socket/mod.rs:453) and checks for a late swap, but it then discards the identity.
- Shutdown's `remove_socket_hardened` (298-312) delegates to the same `cleanup_existing_socket`. The listener bound at socket/mod.rs:116 is still open at the unlink (socket/mod.rs:286). So a probe placed naively inside the shared function would see the daemon's *own* listener as live, and every clean shutdown would leave its socket behind.
- In practice, the takeover happens only when B has a different metrics port; with a shared port, B dies at main.rs:899 first.

**Change.** Split the shared function:

- `remove_stale_socket_at_startup(dirfd, basename, path)`: fstatat, `S_IFSOCK` check, liveness probe, then `unlinkat`. It is used only by `bind_hardened`.
- `remove_own_socket(path, &BoundSocket)`: fstatat, identity comparison, then `unlinkat`. There is **no probe**. It is used only at shutdown.
- **Probe** (shared by preflight and `remove_stale_socket_at_startup`): `probe_socket(path)` opens a non-blocking `socket(AF_UNIX, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC)` and calls `connect(path)`. It must be non-blocking, because a blocking AF_UNIX connect to a full backlog hangs on Linux.

  | connect result | meaning | action |
  |---|---|---|
  | `ECONNREFUSED` (Linux) | stale | `unlinkat` (startup path only), INFO `removed stale socket` |
  | `ECONNREFUSED` (non-Linux) | stale *or* backlog full (BSD/macOS report a full backlog as ECONNREFUSED) | settled by a `SOCK_DGRAM` connect: `ECONNREFUSED` = stale, `EPROTOTYPE`/`0` = live, other = unknown (revised by review finding conf-security-7; the original 3× 50 ms re-probe only narrowed the race) |
  | `ENOENT` | already gone | proceed |
  | `0`, `EAGAIN`, `EINPROGRESS` | live (`EAGAIN` = backlog full = listening, on Linux) | refuse: `socket_path … is in use by a running process; refusing to remove it. Stop the other weir-server or choose another socket_path.` |
  | `EACCES`, `EPERM`, other | cannot tell | refuse (fail closed), naming the errno and telling the operator to remove the file manually if it is stale |
- **Bound identity.** `bind_hardened` returns `BoundSocket { listener, dev, ino, pin }` built from the existing `our_inode` snapshot. On Linux, `pin` is an `O_PATH|O_NOFOLLOW|O_CLOEXEC` fd opened at the bound path immediately after the late-swap check, and held for the life of the listener. While it is held, the socket inode cannot be freed, so its number cannot be reused by a successor's bind on ext4/xfs. On other unix targets, `pin` is `None` and the comparison adds `st_ctime` (and `st_birthtime` on macOS) as a best-effort tie-breaker. The limitation is documented in socket-bind.md.
- **Unlink only our inode.** `remove_own_socket` runs `fstatat`. On a mismatch it logs WARN `socket path now names a different socket (another instance replaced it); leaving it` and does not unlink. The residual `fstatat`-to-`unlinkat` window is the same class as the documented bind-to-`fstatat` window (docs/security/socket-bind.md) and is closed by the same private-parent-directory assumption.
- **Reuse.** Foundations' admin Unix socket must use the same `bind_hardened` / `BoundSocket` / `remove_own_socket`.
- **Side effects to document.**
  - The probe is a real connection to the live daemon: it bumps that daemon's connection metrics, and under `peer_uid_check` a foreign uid is rejected and logged there.
  - A daemon that is mid-shutdown still has its listener open until socket/mod.rs:286, so a probe in that window sees it as live and the new instance refuses. Foundations' lame-duck closes this window. Under systemd the old process has exited before the new one starts.

**Tests**

- Unit:
  - A bound-then-dropped listener leaves the file (ECONNREFUSED): startup cleanup unlinks it.
  - A held listener makes startup cleanup refuse.
  - `remove_own_socket` with a mismatched (dev, ino) keeps the file.
  - **A full `socket::run` start-then-shutdown removes its own socket file** (the regression for the self-probe trap).
  - On Linux, the `O_PATH` pin opens successfully on a socket inode.
  - Extend the existing tests at socket/mod.rs:943-971.
- System:
  - **Replace** `socket_takeover_does_not_corrupt_wab_data` (system.rs:2170-2256) with `second_daemon_refuses_a_live_socket`. B has its own `wab_dir` and metrics port but A's socket, and B is spawned with `spawn_expecting_exit`. B exits non-zero with `in use by a running process` **from preflight** (its log contains no `recovery` line), A still accepts a Durable push afterwards, and A's WAB is unchanged. Rewrite the doc comment, which claims removal "even if another process is listening".
  - `shutdown_leaves_a_successor_socket_alone`:
    1. With A running, the test unlinks A's socket path and binds its own `UnixListener` there.
    2. SIGTERM A.
    3. Assert the test's socket file still exists.
  - The existing crash tests that use `restart_in_place` (SIGKILL, stale socket, ECONNREFUSED; harness.rs:199-222) are the regression check for the stale-unlink path and must stay green.

### 3.4 Rider: a missing config file is silent

- `file::read` returns an empty layer on NotFound (config/file.rs:194-199).
- `--config` defaults to `/etc/weir/weir.toml` whether or not it was passed (cli.rs:128-131), so explicit and implicit cannot be told apart.
- **Correction to the research.** Making an explicit-but-missing `--config` a hard error would break every env-only Docker user, because the image CMD *always* passes `--config /etc/weir/weir.toml` (Dockerfile:134).
- In this release:
  - Track `config_path_explicit`.
  - Always WARN when the file is missing, naming the path. Escalate the wording when the resolved `sink_type` is `noop`.
  - Log the config source at startup.
- The hard error is deferred to a minor release that also changes the Dockerfile CMD (D10).

---

## 4. SP-B: drain resend loops and misclassification

### 4.0 Shared mechanism: SegmentLedger

**Ledger.** A new `drain/ledger.rs` holds drain-owned, in-memory state keyed by segment path:

```rust
struct SegmentLedger {
    resume_from:        HashMap<PathBuf, ResumeCursor>,   // { processed: u64, dev: u64, ino: u64 } — F05 across strands
    dl_carry:           HashMap<PathBuf, DeadLetterCarry>, // { through: u64, payloads: Vec<Payload>, bytes: u64 } (4.5, 4.11)
    confirm_pending:    HashMap<PathBuf, ConfirmPending>,  // delivered; sidecar or delete outstanding (4.3)
    quarantine_pending: HashMap<PathBuf, &'static str>,    // quarantine move outstanding (4.4)
    parked:             HashMap<PathBuf, ParkReason>,      // not re-queued this run
    local_read_failures:HashMap<PathBuf, u32>,             // segment-class local read failures (4.1/4.2)
    not_before:         HashMap<PathBuf, Instant>,         // per-segment cool-down (4.6)
    panic_count:        HashMap<PathBuf, u32>,             // drain panics while this segment was in flight (4.12)
    in_flight:          Option<PathBuf>,
    counts:             Arc<LedgerCounts>,                 // AtomicUsize mirrors, see Seams
}
enum ParkReason { LocalReadFailed, UnsupportedVersion, DrainPanic }
```

**Rules.**

- **Ownership.** `run_drain_supervised` (drain/mod.rs:235) owns the ledger and passes `&mut` through the existing `catch_unwind(AssertUnwindSafe)` (drain/mod.rs:254). It is mutated only on the drain thread. It holds paths, counters and at most a bounded set of payloads (`dl_carry`, capped below), so it is panic-safe. It is lost on restart, which falls back to today's replay-from-record-0 behaviour.
- **One admission function, consulted at every entry point.**

  ```rust
  fn admit(&mut self, path: &Path, now: Instant) -> Admit   // Admit::{ Process { skip: u64 }, Drop(&'static str) }
  ```

  It is called in the Draining arm on **both** sources, `pending.pop_front()` (drain/mod.rs:679) and `drain_rx.recv_timeout` (drain/mod.rs:690), before `process_segment`.
  - A path in `confirm_pending`, `quarantine_pending` or `parked`, or still inside `not_before`, is dropped with a debug log. The rescan is the re-entry point once it becomes admissible.
  - An admissible path returns `skip = resume_from[path].processed` when the cursor's (dev, ino) still matches `stat(path)`. Otherwise `skip = 0`, and a stale cursor is discarded.

  This closes the double-queue hole. Under sustained load the rescan runs with `pending` empty (drain/mod.rs:417) while up to 256 copies of the same paths still sit in the crossbeam channel, which is not enumerable (metrics/mod.rs:333-338 already documents this over-count). Today the second copy is a harmless NotFound no-op (drain/mod.rs:1118-1123). Under this design it would otherwise re-deliver a `confirm_pending` segment or bypass a `not_before`. Paths pushed into `pending` from the RetryingTransient and Blocked waits (drain/mod.rs:789, 907) are filtered when they are popped.
- **The rescan excludes ledger-owned paths before any side effect.** `scan_unconfirmed_sealed` gains an `exclude: &dyn Fn(&Path) -> bool` parameter (the startup replay passes `|_| false`). `probe_and_resume_stranded` passes `ledger.excludes` (confirm_pending ∪ quarantine_pending ∪ parked ∪ not-yet-due `not_before`). Excluded paths are skipped **before** `check_confirmed` runs (wab/mod.rs:427), so `check_confirmed`'s quarantine side effects (recovery.rs:859-876) never touch a ledger-owned segment.
- **Order inside every health poll** (Draining arm, drain/mod.rs:654-676):
  1. `ledger.retry_dispositions()`: sidecar, delete and quarantine-move retries. **It never delivers.**
  2. `probe_and_resume_stranded`.
  3. The dead-letter and quarantine gauge refresh.
- On `'outer` exit (drain/mod.rs:996) there is one best-effort `retry_dispositions()`.
- **Cursor lifecycle.**
  - `resume_from[path]` is written whenever `process_segment` returns `Transient` or `BlockedDeadLetter` with `processed > 0`, and it **survives the strand**. This revises the H2 comment (drain/mod.rs:826-842): F05 now holds across strands within a run. Persistence across restarts is still deliberately not done.
  - It is removed on `Confirmed`, on `Quarantined` and on park.
  - Safety: sealed segments are immutable, names are unique within a run (the writer's in-memory counter only grows), and the (dev, ino) check rejects a cursor for a file that was replaced.
- **Cause classification.** `ProcessResult::Transient` gains `cause: TransientCause { Sink, LocalRead { class: ErrClass, errno: Option<i32> }, LocalWrite }`.

  | `ErrClass` | errno (`raw_os_error`) | counts toward escalation | escalation after K cycles and the 10-minute floor (§4.1) |
  |---|---|---|---|
  | `Process` | EMFILE, ENFILE, ENOMEM, ENOBUFS, EAGAIN, EINTR | **never** (daemon-wide, not a property of the segment) | none: strands and resumes as today, with the cursor kept |
  | `Access` | EACCES, EPERM, EISDIR, ENOTDIR, ELOOP | yes | **park** (`local_read_failed`) |
  | `Media` | EIO, ENXIO, ESTALE, EBADMSG and any other kind | yes | **quarantine** (today's mid-segment path) |
- **Strand bookkeeping by cause.**
  - `Sink` strands keep today's behaviour: `weir_drain_segments_stranded_total` +1 and `prev_health_ok = false` (drain/mod.rs:822-826).
  - `LocalRead` and `LocalWrite` strands **do not** flip `prev_health_ok` (they do not fake a sink-down edge) and count in the new `weir_drain_local_io_strands_total{cause="read|write"}` instead of the sink-oriented `stranded` counter, whose help text says "transient sink failures … investigate the sink" (metrics/mod.rs:628-634). See D11.
  - Local strands are resumed by the idle rescan, which runs whenever `pending` is empty.

**Shared new metrics**

- `weir_drain_confirm_failures_total{stage="sidecar|delete"}` (counter)
- `weir_drain_quarantine_failures_total` (counter)
- `weir_drain_segments_parked{reason="local_read_failed|unsupported_version|drain_panic"}` (gauge)
- `weir_drain_segments_disposition_pending{kind="confirm|quarantine"}` (gauge)
- `weir_drain_local_io_strands_total{cause="read|write"}` (counter)
- `weir_drain_dead_letter_write_failures_total` (counter)

### 4.1 A sealed segment with a corrupt header is retried forever

**Current behaviour.**
- `process_segment` maps every non-NotFound `SegmentReader::open` error to `Transient` (drain/mod.rs:1116-1136).
- `SegmentReader::open` reports bad magic, version 0 or above `FORMAT_VERSION_MAX_SUPPORTED`, and a short header (its `UnexpectedEof` is remapped) all as `InvalidData` (weir-wab lib.rs:112-156). Other I/O errors pass through.
- The result: `max_retries+1` attempts, then a strand. The strand sets `prev_health_ok=false` (drain/mod.rs:822-826), and the next non-Down probe re-queues the segment, every poll.
- Across restarts it replays again, because recovery handles only `.wab` (recovery.rs:112-119) and `scan_unconfirmed_sealed` checks only the sidecar (wab/mod.rs:427-446).
- The comment at drain/mod.rs:1127-1130 ("quarantined by crash recovery on the next restart") is false.

**Change.**

- On `InvalidData` from open, read header byte 4 directly. It is already in the error path; read 5 bytes with `File::open` plus `read_exact`.
  - Version byte `> weir_wab::format::FORMAT_VERSION_MAX_SUPPORTED` (pub, format.rs:81): **park** with `reason=unsupported_version`, plus an ERROR naming the version. This segment was written by a newer weir and is not corrupt. Quarantining it would break rollback, because a downgraded daemon would move every newer-format segment into manual recovery. Parked segments replay on the next start, and a newer binary delivers them.
  - Bad magic, version 0, or a short header: `quarantine_segment` now. Nothing was delivered, so this behaves like a corrupt-header recovery quarantine.
- Any other kind becomes `Transient{processed: skip, cause: LocalRead{class}}` and follows the 4.0 table.
- **Escalation.** When a segment's `local_read_failures` reaches `(max_retries+1) × LOCAL_READ_STRAND_LIMIT` (const, 3), that is three strand cycles. `Access` errors park, `Media` errors quarantine, and both log an ERROR naming the path and errno. `Process` errors and `Sink` causes never count.
  - **Revised after review (conf-regressions-8): escalation needs both conditions.** The count alone escalated about a minute in at the defaults, so a device blip (a remount, a path flap, a `chown -R` or SELinux relabel during maintenance) quarantined a good segment into manual recovery or parked it until a restart, where 4.0.1 kept retrying and healed by itself. Escalation now also requires `LOCAL_READ_ESCALATION_MIN_AGE` (const, 600 s, drain/mod.rs) to have passed since the segment's **first** counted local read failure in this run. The ledger records that instant per segment, and the age is measured to the attempt's start, so the pre-attempt `last_local_read_chance` and the post-attempt escalation always agree, and a persistent mid-segment `Media` error still delivers the readable prefix exactly once. Until both hold the segment keeps cycling (a cool-down and one strand per cycle), as a `Process` error does. Tests override the age through an internal `DrainConfig` field (the `max_respawns` precedent), set to zero in the count-based tests.
  - **This is an autonomous decision taken after review, flagged for the owner** alongside D5 in the table at the top. It does not change the class rules, and a parked `local_read_failed` segment is still not re-admitted at runtime.
- Parked segments are retried on restart, and by Foundations' `drain retry-stranded --include-parked` (D5).
- Fix the comment.

**Invariant and duplicates.**
- No false-ack risk, because quarantine and park keep the bytes.
- For format corruption, duplicates drop from unbounded to zero.
- For open-time local errors nothing was delivered.
- For `Process`-class errors the segment keeps cycling exactly as today and self-heals when descriptors or memory free up. Nothing is parked, so there is no restart dependency.

**Tests.**
- A segment with bad magic is quarantined on the first attempt, with `drain_segments_stranded` at 0 and the file in `quarantine/`.
- A segment with version byte `FORMAT_VERSION_MAX_SUPPORTED+1` is parked (`unsupported_version`) and **not** moved.
- `Access` class, which works as root in CI: put a *directory* at the sealed path, so `File::open` succeeds and `read` fails with `EISDIR`. Expect Transient, then strand, then park after 3 cycles, with no re-queue afterwards and `weir_drain_segments_stranded` unchanged.
- `Process` class: a `cfg(test)` open hook returns EMFILE for 30 cycles. No segment is parked, and all segments deliver once the hook clears.
- The 10-minute floor (added after review): `a_local_read_blip_past_three_strand_cycles_but_under_the_min_age_heals` holds EIO and EACCES at open for 5 strand cycles under a 5 s floor; nothing escalates and the segment delivers whole, once. `a_local_read_fault_past_both_thresholds_escalates_by_class` uses a 1.5 s floor: each escalation (EIO at open quarantines, EIO mid-segment delivers the prefix once and then quarantines, EACCES at open parks) comes at or after the floor and after more failures than the count limit, and the segment is never read again. The count-based tests above run with the floor at zero.

### 4.2 Transient I/O mid-segment or at the footer escalates to quarantine

**Current behaviour.**
- Any `Err` from the reader iterator sets `read_failed` (drain/mod.rs:1170-1180). The readable prefix is committed and the segment is quarantined (drain/mod.rs:1255-1258).
- Any footer read error quarantines (drain/mod.rs:1273-1303).
- The reader documents corruption as `InvalidData` or `UnexpectedEof` (weir-wab lib.rs:64-83). `truncate_context` keeps the kind and passes other errors unchanged (lib.rs:227-230). `read_segment_record_count` reports a short file as `UnexpectedEof` (wab/mod.rs:503-511).

**Change.**

- **Mid-segment.**
  - `InvalidData` and `UnexpectedEof` keep today's path: deliver the prefix, then quarantine.
  - Any other kind breaks out **without committing the partial in-memory batch** and returns `Transient{processed: durable_through, cause: LocalRead{class}}`. The 4.0 cursor makes every later attempt, across strands too, resume exactly past the durable prefix.
  - At the 4.1 limit, `Media` takes today's path: commit the readable prefix (nothing beyond the cursor is re-committed), then quarantine. `Access` parks.
- **Footer.**
  - Count mismatch: quarantine, as today.
  - `UnexpectedEof` or `InvalidData`: quarantine.
  - `NotFound`: treat it like the open-time NotFound (drain/mod.rs:1118-1123), i.e. already consumed.
  - Other kinds: `Transient{processed: durable_through /* = all records */, cause: LocalRead}`. Every retry, across strands, resumes with `skip = record count`. It re-reads the segment (the G13 cost, drain/mod.rs:1192-1198) and re-checks the footer, and **re-commits nothing**. At the limit, `Media` quarantines and `Access` parks.

**Invariant and duplicates.**
- Real corruption is still quarantined. Persistent media errors are quarantined after a bounded number of cycles, with every readable record delivered **exactly once per run**.
- A blip no longer pushes a good segment into manual `weir-ctl quarantine requeue`, which would re-send the prefix.
- Revision 1 claimed this "removes a duplicate source" without the cursor. That was false: each strand cycle re-delivered the prefix, or the whole segment for footer errors. The cursor is what makes the claim true.

**Tests.**
- The fault reader yields `Err(EIO)` at record N and **stays failing for more than K cycles**. Assert that each record below N reaches the sink exactly once in the run, and that the segment ends in `quarantine/`.
- A footer read fault (EIO) held for 2 cycles and then cleared: the sink count equals the record count exactly, and the segment is confirmed.
- Unchanged: a CRC mismatch is quarantined.

### 4.3 A `.confirmed` write failure re-delivers every poll; a torn sidecar becomes a false corruption alarm

**Current behaviour.**
- `confirm_and_delete` logs and returns, with no metric, when the sidecar write fails (confirmed.rs:36-47). The segment stays sealed and unconfirmed, and the idle rescan re-delivers the **whole segment** every `health_poll_interval` (drain/mod.rs:417).
- `write_confirmed_durably` writes **in place** under the final name: create+truncate, `write_all`, `sync_all`, then a parent fsync (confirmed.rs:86-101). On ENOSPC the create succeeds and `write_all` fails, which leaves a 0-byte or partial `seg_X.wab.confirmed`. The next rescan's `check_confirmed` parses it as `WrongLength` and **quarantines the delivered segment** (recovery.rs:859-876), which bumps the corruption counters. An operator's `quarantine requeue` then re-sends the whole segment.
- When `sync_all` fails after `write_all`, a page-cache sidecar parses as valid although it is not durable.
- A `remove_file` failure after a valid sidecar only WARNs (confirmed.rs:49-51). The orphan is then skipped forever with an INFO line (wab/mod.rs:433) and still counted in `wab_bytes_on_disk` and the cap (main.rs:109-116).
- The module docs claim the orphan "can be cleaned up" (drain/mod.rs:22-27, confirmed.rs:11-15). Nothing does that.

**Change.**

- **Atomic sidecar.** `write_confirmed_durably` writes `seg_X.wab.confirmed.tmp` (0600, create+truncate), `write_all`s it, `sync_all`s it, `rename`s it to `seg_X.wab.confirmed`, then fsyncs the parent directory. The final name therefore exists only for complete, fsynced contents. If the parent fsync fails after the rename, the result is `SidecarFailed`: the segment is not deleted, and the ledger retries.

  The walkers ignore `.tmp`:
  - recovery's `is_segment` (recovery.rs:80-82) and `recover_open_segments` (recovery.rs:117-118) match exact suffixes;
  - `compute_wab_bytes_on_disk` (main.rs:111);
  - weir-ctl (main.rs:530-535).

  `segment_counters` (segment.rs:~459-467) parses the `seg_<digits>` prefix of any name, so a leftover tmp file can only *advance* the counter, never rewind it. `scan_unconfirmed_sealed` deletes stale `*.confirmed.tmp` files it finds; at startup they are by definition from a dead attempt.
- `confirm_and_delete` returns `ConfirmOutcome { Done, SidecarFailed, DeleteFailed }`.
  - `SidecarFailed`: insert `confirm_pending{stage: Sidecar, record_count}`. Each health poll retries the sidecar write and then the delete. **Delivery is never retried.** The path is excluded from the rescan before `check_confirmed` (4.0), so no torn or partial state can be misread.
  - `DeleteFailed`: insert `confirm_pending{stage: Delete}`, retrying only `remove_file`.
  - Both bump `weir_drain_confirm_failures_total{stage}`. **ENOENT on any delete counts as `Done`.** The startup replay scan (main thread, main.rs:885) and a drain rescan can race on the same orphan until SP-F moves replay into the drain.
- **Orphan deletion (D7).** In `scan_unconfirmed_sealed`'s `Ok(true)` arm (wab/mod.rs:432-434):
  - Only when the 3.1 lock is held (`LockOutcome::Held`): open the sidecar, `sync_all` it, fsync the shard directory, then `remove_file` the `.wab.sealed`. ENOENT counts as success. If the sync fails, do not delete.
  - Under `best-effort` without a lock, skip orphan deletion and log once.

  This applies at startup (making the module docs true) and in the rescan. Delivery itself was proven when the sidecar was first written; the fsync-before-delete guarantees that the tombstone is durable before the segment disappears, which keeps counter monotonicity across a power loss.
- If the operator deletes the segment manually, the ledger entry is dropped (the existing `!sealed.exists()` early return, confirmed.rs:29-35).
- Update docs/wab_format.md:196 ("left for the operator").

**Invariant and duplicates.**
- The existing rule holds and is tightened: delete only after a sidecar that is complete, valid **and** fsynced.
- Segment-name monotonicity is unaffected, because the `.confirmed` tombstone stays and the counter scan counts it.
- Duplicates per sidecar failure go from one whole segment per poll, unbounded, to zero for the rest of the run, or one replay if the process restarts first.
- False corruption quarantines from a torn sidecar go to zero for sidecars written by this version. Torn sidecars left by 4.0.x on upgrade are still quarantined once, as today.

**Tests.**
- EISDIR, which works as root: pre-create a **directory** at `seg_X.wab.confirmed.tmp`. Run several polls with `health_poll_interval=50ms`. The sink count for that segment stays at exactly 1, the metric rises, and there is no quarantine. Remove the directory; the next poll writes the sidecar and deletes the segment, and the sink count is still 1.
- **Short write:** a `cfg(test)` hook in `write_confirmed_durably` returns ENOSPC after half the bytes. Poll several times and assert no quarantine, no final-name sidecar, and a sink count of 1.
- `sync_all` failure hook: no rename and no delete.
- An orphan pre-seeded with a sealed file and a valid sidecar is removed at startup and by the rescan. Under `best-effort` without a lock it is kept.
- The delete-failure stage uses a `cfg(test)` fault hook in confirmed.rs, because root bypasses permission tricks.
- Manual (beast): fill a tmpfs `wab_dir`. This needs sudo, so it is not in CI.

### 4.4 A failed quarantine move re-delivers the prefix every poll

**Current behaviour.**
- `quarantine_segment` only logs when the move fails (drain/mod.rs:1345-1348). That happens when `create_dir_private` or `rename` fails (recovery.rs:717-736), for example on ENOSPC, EACCES, or a stray file named `quarantine`.
- The callers return `Quarantined` regardless (drain/mod.rs:1257, 1284, 1302). The segment stays sealed and unconfirmed, and the rescan re-delivers its readable prefix (or the whole segment, after a footer mismatch) every poll.

**Change.**
- `quarantine_segment` returns `bool`. On failure, insert into `quarantine_pending` and bump `weir_drain_quarantine_failures_total`.
- `process_segment` returns a new `ProcessResult::QuarantinePending`, and the cursor is dropped.
- Each health poll retries **only the move**. On success it bumps the existing `recovery_segments_quarantined` and `wab_segments{quarantined}` series (drain/mod.rs:1324-1343). ENOENT counts as done.

**Invariant and duplicates.** Bytes are never dropped. Duplicates go from unbounded to zero for the rest of the run.

**Tests.**
- Create a regular file named `<wab_dir>/quarantine`, then feed a segment with a CRC error. The prefix is delivered once across several polls, and the failure counter is 1.
- Remove the file. The next poll completes the move.

### 4.5 Dead-letter headroom: block and unblock predicates differ

**Current behaviour.**
- The blocked arm unblocks on `total_bytes() < dead_letter_max_bytes` (drain/mod.rs:951).
- `dead_letter_records` blocks on `total + estimate > cap` (dead_letter.rs:75-77, drain/mod.rs:1527-1537).
- The sink commit happens *before* the dead-letter write (drain/mod.rs:1442-1455), and the resume cursor excludes the blocked sub-batch.
- So while total is in `[cap−estimate, cap)`, the accepted records of that sub-batch are re-committed every `dead_letter_check_interval` (30 s).
- Separately, `rescan()` propagates a `read_dir` error (dead_letter.rs:68-72). An operator who `rm -rf`s `dead_letter/` to free headroom leaves `total_bytes` stuck at the at-cap value, so the arm never unblocks (drain/mod.rs:921-923 only WARNs).

**Change.**
- Thread the estimate through: `BatchResult::Blocked{needed}`, then `ProcessResult::BlockedDeadLetter{processed, needed, carry}`, then `DrainState::BlockedDeadLetterFull{…, needed}`.
- Unblock only when `!dead_letter.would_exceed_cap(needed, cap)`. A blocked state always has `needed ≤ cap`, because `dead_letter_records` writes an oversized batch anyway (drain/mod.rs:1528-1534), so the predicate is always satisfiable.
- **Carry the rejected payloads (revision 2; it was deferred in revision 1).** On `Blocked`, `commit_batch` returns the sub-batch's dead-lettered payloads as a `DeadLetterCarry { through: durable_through + n, payloads, bytes }`, stored in `ledger.dl_carry[path]`.
  - When the segment is next processed, `process_segment` first runs `dead_letter_records(carry.payloads)`. On `Ok` it advances to `skip = carry.through` **without re-committing** the accepted records. On `Blocked` or `Transient` the carry is kept.
  - Bound: the sum of `dl_carry` bytes is capped at `DL_CARRY_MAX_BYTES` (const, 64 MiB). A carry that would exceed the cap is not stored, and that segment falls back to today's re-commit-once-per-unblock behaviour, with a WARN.
  - The carry is in memory: a restart re-commits and re-dead-letters that sub-batch once.
- `DeadLetterWriter::rescan`: on `NotFound`, recreate the directory with `create_dir_private`, set `total_bytes = 0`, and keep `next_counter`.

**Duplicates.** The accepted records of a blocked sub-batch are committed **once per run** (they were committed once per 30 s before). A restart costs one extra commit plus one DL write of that sub-batch.

**Tests.**
- Set the DL total to `cap − estimate/2` and the check interval to 50 ms. Over 10 intervals the sink commit count for that sub-batch stays at 1. Free space beyond `needed` and the drain resumes exactly once, writes the carry, and does not re-commit.
- While blocked, delete `dead_letter/`. The drain resumes within one check interval.

### 4.6 The final retry backoff is wasted

**Current behaviour.**
- The `RetryingTransient` arm serves the whole wait (drain/mod.rs:768-806), rescans (811-819), and only then checks `retries_left == 0` (drain/mod.rs:822).
- With `max_retries=3` that means 4 attempts and then a dead final wait of `8×base`, or up to 300 s with Retry-After (`next_retry_delay`, drain/mod.rs:1077-1079), on the single drain thread.

**Change.**
- Move the `retries_left == 0` strand branch to the **top** of the arm, before the wait and the rescan.
- Record `ledger.not_before[segment] = now + next_delay` and `ledger.resume_from[segment] = processed`.
- **Correction to the research ("strand right after the final failure" alone).** Stranding alone would let the Draining-arm poll re-queue the segment immediately (`prev_health_ok=false` plus a non-Down probe), which would violate a server-supplied Retry-After. With `not_before`, the 4.0 admission filter and rescan exclusion skip the segment until then, including channel copies. The wait moves off the thread instead of disappearing.

**Duplicates.** Neutral on attempts per episode. With the cursor, the attempt after a strand resumes past the committed prefix. Other segments make progress during the cool-down.

**Tests.**
- With `base=200ms`, `max_retries=2` and an always-transient sink, the time from the first failure to the `stranded` increment is under 1.0 s (it is 1.4 s today).
- With `Retry-After: 2s`, the segment is not re-processed before 2 s, **even when a copy of its path is also in the channel**.

### 4.7 Plain-mode duplicate-key livelock (MySQL and Postgres)

**Current behaviour.**
- In `insert_mode = "plain"`, `ER_DUP_ENTRY` 1062 (mysql.rs:326) and `23505` (postgres.rs:461) are transient. The multi-row INSERT is a single autocommit statement, all-or-nothing, wrapped in one `tokio::time::timeout(self.config.timeout, …)` (postgres.rs:509, mysql.rs:403). The timeout defaults to 10 s; batch sizes range from 1 to 10,000 (config/mod.rs:859-863).
- **Correction to the research (the verifier's version holds).** A strand is not needed to trigger this. A `commit_timeout` elapse on a commit the server actually applied returns Transient (drain/mod.rs:1392-1408) without advancing `durable_through`, so the next retry re-sends the identical INSERT and hits the UNIQUE key on every attempt and after restarts.
- The module docs' rationale ("in case the duplicate is from a stale concurrent writer") cannot be satisfied by re-sending the same statement.

**Change (sink-local, Plain mode only), revised for D6.** When the multi-row INSERT fails with the dup-key code:

- **With `id_column` set (set-based, a constant number of round trips).** The id is `RecordId::for_record` = SHA-256 over segment name, index and payload (weir-sink-sdk lib.rs:492-501). A row whose id is already present is therefore *the same record, already delivered*.
  1. `SELECT <id_column> FROM <table> WHERE <id_column> = ANY($1)` (Postgres, one array bind) or `… IN (?,…,?)` (MySQL, at most 10,000 placeholders, under the 65,535 limit). Present ids go to `committed`.
  2. A multi-row INSERT of the remainder.
     - Success: done.
     - Dup-key again means a race with a concurrent writer or a *different* UNIQUE constraint. Run the bisection below on the remainder only.

  Progress is monotonic across attempts: rows inserted by an attempt that later timed out are found by step 1 on the next attempt.
- **Without `id_column` (bisection).** Split the failing batch in halves and retry each half. A half that inserts is `committed`. A single row with a dup-key goes to `dead_lettered` with reason `duplicate key (insert_mode=plain, no id_column): <server msg>`. That row is not provably the same record, so it is kept losslessly. A batch without duplicates still costs one statement. The worst case, every row a duplicate, costs 2N−1 statements.
- In both paths:
  - Another permanent error dead-letters that row.
  - Another transient error returns transient for the whole batch.
  - The whole fallback runs inside the existing `self.config.timeout`, with a deadline check before each statement. When the deadline passes it returns transient.
  - The `committed ∪ dead_lettered = batch` partition (checked at drain/mod.rs:1418-1428) holds.
- Add a config-load WARN when plain mode is used without `id_column`, recommending `id_column` or `ignore` / `on_conflict_do_nothing`.

**Duplicates and risk.**
- With `id_column` the livelock ends, and duplicates cost nothing: they are counted as committed, nothing goes to the DL, and nothing goes to `weir-ctl dl requeue`. Requeue re-pushes into *new* segment coordinates (weir-ctl main.rs:173), so it would have inserted real downstream duplicates.
- Without `id_column`, every dup row costs one DL record, and the DL can fill. That is visible and actionable.
- **Residual:** an all-duplicate batch where (2N−1) × RTT exceeds the sink timeout still never completes. It is documented, the WARN covers it, and it is listed in Unresolved.

**Tests.** These run in the `sink-integration` CI job (deploy/run-sink-integration-tests.sh), for both MySQL and Postgres.
- Plain mode with a UNIQUE `id_column`: commit `[a,b]`, then `[a,b,c]`. Expect `committed=[a,b,c]` and `dead_lettered=[]`, with exactly 3 rows in the table.
- Plain mode, no `id_column`, UNIQUE on the payload column: commit `[a,b]`, then `[a,b,c]`. Expect `committed=[c]` and `dead_lettered=[a,b]`.
- `max_batch_size=5000` with `id_column` and a full replay: the attempt completes within the sink timeout.
- First attempt timed out midway (inject latency): the next attempt reports all rows committed, with no DL entries.

### 4.8 Truthful drain liveness

**Current behaviour.**
- `heartbeat.bump()` runs only at the `'outer` head (drain/mod.rs:638) and inside the two waits (drain/mod.rs:782, 900).
- `process_segment` (drain/mod.rs:1108-1310) delivers a whole segment, up to 256 MiB in `max_batch_size` sub-batches, inside one `rt.block_on`.
- The drain thread also does long synchronous local I/O without bumping:
  - sidecar `sync_all` plus the directory fsync (confirmed.rs:99-101);
  - the DL seal and fsync (drain/mod.rs:1527-1555);
  - quarantine renames (drain/mod.rs:1322-1348);
  - the health-poll directory walks (drain/mod.rs:654-676).
- The stall threshold is `health_poll_interval + 15 s`, 45 s at defaults (main.rs:150-152). `commit_timeout` is `max(2×sink_timeout, 60 s)` (main.rs:594).
- So a single legitimate commit, or a slow-disk fsync (the beast research measured multi-minute stalls), trips the `Wedged` WARN (main.rs:1040-1051), which advises a restart. A restart re-delivers from record 0 and fixes neither.

**Change.**

- **Heartbeat.** Bump after every `commit_batch` return, and every `READ_HEARTBEAT_EVERY = 1024` records read inside `process_segment`.
- **Busy signal.** `DrainHeartbeat` gains `busy_since: AtomicU64` (ms since a process-epoch `OnceLock<Instant>`, plus 1, with 0 meaning idle) and `busy_kind: AtomicU8`:

  ```rust
  enum BusyKind { SinkCommit=1, SinkProbe=2, SegmentRead=3, LocalFsync=4, DeadLetterWrite=5, QuarantineMove=6, DirScan=7 }
  struct BusyGuard<'a>(&'a DrainHeartbeat);   // set on new(), clear on Drop — also on panic unwind
  ```

  Guards wrap the following:
  - `tokio::time::timeout(commit_timeout, sink.commit(..))` (drain/mod.rs:1392), placed *outside* the timeout so a non-yielding sink keeps it set;
  - `probe_health` (drain/mod.rs:366);
  - the reader loop in `process_segment`, as `SegmentRead`, switched to `SinkCommit` around each commit;
  - `write_confirmed_durably` and the orphan fsync;
  - `dead_letter.write_records`;
  - `quarantine_segment`;
  - `scan_unconfirmed_sealed`, `dead_letter.rescan` and `quarantine_bytes_on_disk`.

  All of these are wait-free `Relaxed` stores, and no lock is shared with anything.
- **Verdict.** `drain_stall_verdict` (main.rs:186-199) takes `busy: Option<(BusyKind, Duration)>` and `commit_timeout`:

  | condition | verdict | log | /livez (Foundations) |
  |---|---|---|---|
  | `drain_state{stopped}` | `Stopped` (as today) | WARN | per SP-E's policy |
  | heartbeat frozen, busy in `SinkCommit`/`SinkProbe` for ≤ `commit_timeout + 15 s` | `SlowSinkCall` | INFO, no restart advice | pass |
  | heartbeat frozen, busy in a local kind (`SegmentRead`…`DirScan`) for any duration | `SlowLocalIo` | WARN naming the kind and duration | **pass**: a restart does not fix a disk |
  | busy in a sink kind for > `commit_timeout + 15 s` (the backstop could not fire, so the sink is blocking the thread) | `Wedged` | WARN with restart advice | fail |
  | heartbeat frozen, nothing busy | `Wedged` | WARN with restart advice | fail |
- **New gauge** `weir_drain_busy_seconds{kind}`. It is set by the poll task from the atomics. Only the current kind is non-zero.
- **Poll placement.** The stall verdict and the gauge move **before** the WAB-scan `let … else { continue; }` (main.rs:990-994), so a failing scan no longer silences them.
- **Foundations contract.** `StatusSnapshot` and `/livez` read `heartbeat.get()`, `busy_kind` and `busy_since`, and apply the table above. The safety floor guarantees that a progressing or merely slow drain never reads as `Wedged`.

**Tests.**
- A unit mock sink sleeps (yielding) 2 s per commit over a 10-batch segment, with test thresholds scaled down. The heartbeat advances between commits and the verdict is never `Wedged`.
- A sink that blocks the thread past `commit_timeout + grace` gives `Wedged`.
- Table tests extending main.rs:1770-1850, including a row for a 60 s sidecar fsync, which gives `SlowLocalIo`.
- `BusyGuard` clears on a panic unwind.

### 4.9 Small riders (each a few lines, in code already touched)

- `DeadLetterWriter::write_records`: on `ENOENT`, call `create_dir_private` on the directory once and retry. Only `open()` creates it today (dead_letter.rs:50-53, 82-118). The `rescan()` counterpart is in 4.5.
- `scan_unconfirmed_sealed`: a per-shard `read_dir` uses `?` (wab/mod.rs:417-419), so one unreadable shard blocks every resume.
  - For the **rescan caller only** (`shard_count == None`), skip that shard with a WARN and return the partial list plus `incomplete=true`. The caller treats `incomplete` like today's `Err` for edge tracking (it keeps the edge live, drain/mod.rs:455-465).
  - **Startup replay stays fail-fast.** This deliberately keeps the comment at wab/mod.rs:367-369, "a silently-skipped sealed segment is silently-dropped acked data", true where no later rescan is guaranteed.
- `compute_wab_bytes_on_disk`: replace the two `.flatten()` calls (main.rs:88, 109) with explicit matching that returns `None` on a dirent error, as its own doc (main.rs:78-82) requires. Its effect on the poll task is neutralised by the 4.8 placement change and the separate 3.1 integrity thread.
- **Stale comments and docs:**
  - drain/mod.rs:22-27 (orphan cleanup; now true)
  - drain/mod.rs:78 ("commit_timeout (default 30 s)"; it is ≥60 s)
  - drain/mod.rs:826-842 (H2: the cursor now survives strands within a run)
  - drain/mod.rs:1127-1130
  - drain/mod.rs:221-223 ("not re-delivered this run"; a respawn rescans, 248-255)
  - confirmed.rs:11-15
  - metrics/server.rs:41-43 ("unrecoverable accept error" exit)
  - metrics/mod.rs:333-338: the resumed over-count remains, but double-processing is now filtered
  - docs/monitoring.md:203-213: the probe *does* run in `RetryingTransient` after each backoff (drain/mod.rs:811-819) and on every Blocked wake (929-946), so lag is about one retry backoff, not a poll interval.
- **Ordering docs (D8).** State that per-connection order is WAB/ingest order. Strands, cool-downs, parking, requeue and HTTP `.buffered(8)` (sink/http.rs:497-506, config/mod.rs:873) reorder delivery (README.md:190-192, weir-client lib.rs:39-40).

### 4.10 Research corrections, consolidated

- The lock must outlive the drain join at **main.rs:1258** (not 1263). The design holds it for the process lifetime instead, which also covers early returns.
- SIGHUP: `systemctl reload` sends nothing, because there is no `ExecReload=`. In Docker (PID 1) a SIG_DFL HUP is dropped, not fatal. The kill is real on systemd hosts, with wrappers, and in shared pid namespaces.
- Socket takeover requires B to have a distinct metrics port; otherwise B dies at main.rs:899 first. Both refusals now happen in preflight (3.0).
- "Strand immediately" without `not_before` would violate Retry-After (4.6).
- An explicit missing `--config` cannot become an error in this release without breaking env-only Docker users (3.4).
- WAB cap "unknown until first scan": the first interval tick is immediate, so the window is milliseconds. Only the `.flatten()` undercount is fixed here. The "cap unknown" state belongs in Foundations' readiness (not cap-rejecting).
- Worker panic supervision (from `misc-give-up-paths`) is out of scope (SP-G).
- Revision 1's claim that parking ends every resend loop through "the rescan as the single choke point" was wrong. Channel copies and in-memory `pending` bypassed it. The 4.0 admission function is the choke point.

### 4.11 Dead-letter write failure after a successful commit (new in revision 2)

**Current behaviour.** `commit_batch` commits to the sink first and writes the rejected payloads afterwards (drain/mod.rs:1442-1455). A DL write error (for example ENOSPC on the WAB device, as in the 2026-09-27 chaos run's full 2 GiB device) returns `BatchResult::Transient{retry_after: None}` (drain/mod.rs:1545-1555), and the cursor does not advance. Every retry and every strand cycle re-commits the accepted records of that sub-batch, with no bound.

**Change.**
- The DL write failure returns `Transient{cause: LocalWrite}` with the same `DeadLetterCarry` as 4.5. The retry first redoes only the DL write, then advances to `carry.through`, so the accepted records are not re-committed. The same 64 MiB carry bound and fallback apply.
- Bump `weir_drain_dead_letter_write_failures_total`.
- `LocalWrite` strands count in `weir_drain_local_io_strands_total{cause="write"}` and do not flip `prev_health_ok`.
- They are **not** parked. A full or unwritable DL device is daemon-wide, so they keep cycling, paced by `not_before`, and self-heal when space frees up.

**Test.** A `cfg(test)` hook makes `write_records` return ENOSPC for 10 polls, then clears. The sink commit count for that sub-batch is 1, and the DL holds the rejected records exactly once.

### 4.12 Deterministic drain panic on one segment (new in revision 2)

**Current behaviour.**
1. A sink or a record that panics deterministically is caught by `catch_unwind`, and the supervisor respawns the drain with `initial_health_ok=false` (drain/mod.rs:235-283).
2. The first healthy probe rescans and re-queues the same segment, re-delivering its prefix.
3. It panics again.
4. After `MAX_DRAIN_RESPAWNS` (10) the drain is `stopped`, and delivery ends for every segment until restart.

**Change.**
- Set `ledger.in_flight = Some(path)` before `process_segment` and clear it after.
- In the supervisor's `Err` arm, increment `panic_count[in_flight]`. At 2, park the segment with `reason=drain_panic` and an ERROR naming the path.
- The cursor survives, so the second attempt resumes past the committed prefix.
- The supervisor's respawn budget is unchanged. Five distinct poison segments can still exhaust it, which is stated in the docs.

**Test.** A mock sink panics on one specific payload. That segment is parked after 2 panics, all other segments deliver, and `drain_state` is never `stopped`.

---

## 5. Startup order and the Foundations seams

**This release (minimal motion in main):**

```
umask + SIGHUP=SIG_IGN (main.rs:447-450)
→ Config::load (452) → tracing → "weir starting"
→ **acquire_wab_dir_lock** (new) → Metrics::new → **lock-integrity thread (1 s)** (new; revised in review, was a task in block_on, and Metrics::new moved up for it)
→ **preflight**: validate_socket_path, socket probe (connect-only), metrics test-bind, [tls: TLS validate + tcp test-bind] (new)
→ drain channel (518)
→ wab::spawn [recovery] (559) → workers → drain (+ SegmentLedger, BusyGuard)
→ replay (885) → runtime (890)
→ **spawn reload_loop + SIGHUP task** (new, first in block_on)
→ metrics bind (899) → poll task (stall verdict before the scan) → TLS load + register_tls → SIGTERM task (1172)
→ socket::run [probe-and-unlink + BoundSocket] (1215)
→ joins (1252-1258) → exit (kernel releases the lock)
```

**Foundations target, with the lock and preflight as phase 0:**

```
lock → preflight → runtime + reload_loop/SIGHUP + lock-integrity + health/metrics (real bind, served) + admin listeners
→ recovery → replay handed to the drain as initial pending
→ producer socket bind (still after recovery: segment-counter safety, wab/mod.rs:671-702)
```

**Seams Foundations consumes:**

1. **Lock and preflight placement are part of the contract.** SP-F's restructure must keep `acquire_wab_dir_lock` before the first listener and before anything reads `wab_dir`, and must keep every refuse-to-start check before the drain starts delivering (G6). The two-daemon and refused-duplicate system tests are the regression gates. With fail-fast there is no observable `acquiring_lock` startup phase. If the owner later chooses a lock wait (D3), the phase becomes `waiting_for_wab_lock`, and the health listener would have to bind before the lock, trading away the clear "locked by" diagnosis.
2. **`StatusSnapshot` inputs, all atomics and wait-free:**
   - the `weir_wab_dir_lock_held` gauge;
   - `wab_ownership_lost`;
   - the cached holder record;
   - `LedgerCounts { confirm_pending, quarantine_pending, parked_by_reason[3], cooling_down, resume_cursors, dl_carry_bytes }`, `AtomicUsize`/`AtomicU64` fields updated by the drain on every ledger mutation;
   - heartbeat, `busy_kind`, `busy_since`.

   No reader ever touches the `HashMap`s.
3. **Admin reload** is the same function as SIGHUP: `ReloadRequest{Admin, reply}` through `reload_loop`, with `ReloadReport` as the JSON body.
4. **The ledger is mutated only on the drain thread.** Admin verbs (`drain retry-stranded [--include-parked]`, `drain pause`, `drain resume`) must reach it as commands on SP-F's `DrainControl` channel, processed at the loop head and in each 50 ms wait slice. That requires the three `recv_timeout` sites (drain/mod.rs:690, 788, 906) to become a `crossbeam_channel::Select` over `drain_rx` and the control receiver. No `Mutex` may be added around the ledger. A verb that arrives while the drain is inside a sink call or local I/O waits. SP-F's reply timeout should report `drain_busy{kind, since}` from the busy atomics rather than a bare timeout.
5. **`drain retry-stranded` semantics:**
   - It clears `not_before`.
   - With `--include-parked`, it also clears `parked` and `local_read_failures`. `unsupported_version` stays parked unless named explicitly.
   - It never re-queues `confirm_pending` or `quarantine_pending`, which are already delivered or preserved.
   - It never clears `resume_from` or `dl_carry`.
6. **`drain pause`** stops delivery at a segment boundary. Ledger disposition retries (sidecar, delete, quarantine move) may continue while paused, because they deliver nothing.
7. **The admin socket** uses `bind_hardened` / `BoundSocket` / `remove_own_socket`.
8. **`/livez`** applies the 4.8 verdict table. `SlowLocalIo` and `SlowSinkCall` never fail it.

---

## 6. Additions inventory (all additive)

| Kind | Name | Notes |
|---|---|---|
| On-disk | `<wab_dir>/.weir.lock` (0600, regular, never unlinked) | Ignored by every walker; old binaries ignore it |
| On-disk | transient `seg_X.wab.confirmed.tmp` | Exists only during a sidecar write; stale ones are deleted by the scan; ignored by every walker |
| Config | `wab_dir_lock = "required" \| "best-effort"` (env `WEIR_WAB_DIR_LOCK`) | Only affects ENOLCK/EOPNOTSUPP/ENOTSUP/ENOSYS/EINVAL; contention always refuses (D2) |
| Exit code | 3 = WAB ownership lost to another instance | Others unchanged (1 = startup refusal) |
| Metric | `weir_wab_dir_lock_held` (gauge 1/0) | 0 = best-effort fallback, or ownership lost just before exit |
| Metric | `weir_wab_dir_lock_integrity_failures_total` | Lock file deleted or replaced, then re-locked |
| Metric | `weir_config_reloads_total{trigger,outcome}` | `weir_tls_config_reloads_total` unchanged |
| Metric | `weir_drain_confirm_failures_total{stage}` | sidecar / delete |
| Metric | `weir_drain_quarantine_failures_total` | |
| Metric | `weir_drain_segments_parked{reason}` (gauge) | local_read_failed / unsupported_version / drain_panic |
| Metric | `weir_drain_segments_disposition_pending{kind}` (gauge) | confirm / quarantine |
| Metric | `weir_drain_local_io_strands_total{cause}` | read / write |
| Metric | `weir_drain_dead_letter_write_failures_total` | |
| Metric | `weir_drain_busy_seconds{kind}` (gauge) | replaces revision 1's `…sink_call_in_flight_seconds` (never shipped) |
| Behaviour | Refuses to start when: the lock is held; the FS cannot lock (required); a live socket is at socket_path; the metrics/TCP port is taken; the TLS material is invalid. All happen in preflight, before recovery | Release-noted |
| Behaviour | Exits (3) when another instance provably owns the `wab_dir` | Release-noted |

weir-ctl:
- No change. It must not lock.
- `segments --json` and `attest --json` output are unchanged, because the lock file is not a directory and `.tmp` matches no suffix (main.rs:491-493, 530-535; attest.rs:84). writ's contract is safe.

---

## 7. Verification gates

**Unit** (weir-server bin tests run with `--test-threads=1`):
- lock.rs, including integrity classification and the no-re-open rule
- preflight
- socket probe (startup only), `remove_own_socket`, and the full-run self-removal test
- each of 4.1-4.8, 4.11 and 4.12 as specified, including the admission-filter test: the same path is both in the channel and pushed by the rescan, the first delivery fails its sidecar, and the sink count must stay 1
- walker-ignores-lock-file and walker-ignores-`.tmp` tests for recovery, `scan_unconfirmed_sealed`, `compute_wab_bytes_on_disk`, `segment_counters` and weir-ctl `scan_segments` / `shard_dirs`

**System** (tests/system.rs; tls-feature binary; new testkit helper `spawn_expecting_exit`):
1. `second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking` (recording sink, 70 payloads)
2. `lock_file_stolen_by_second_daemon_fails_closed`
3. `refused_duplicate_never_delivers` (socket and metrics-port variants)
4. `second_daemon_refuses_a_live_socket` (replaces system.rs:2170-2256)
5. `shutdown_leaves_a_successor_socket_alone`
6. `sighup_is_a_counted_noop_on_a_default_daemon`
7. `sighup_reloads_tls_when_configured`
8. `sighup_during_startup_replay_is_ignored` (black-hole sink) plus the recovery-phase variant
9. All existing `restart_in_place` crash tests, as the stale-socket regression

Audit for tests that start a daemon on a `wab_dir` whose previous owner may still be exiting. `restart_in_place` SIGKILLs and waits first (harness.rs:199-222), which is fine; anything that skips the wait now fails with "locked by".

**Sink integration** (the `sink-integration` CI job): the four 4.7 tests per database. Per the CI discipline, run the real workflow locally with `act -W .github/workflows/ci.yml -j sink-integration` (and `-j test`) before pushing.

**Seccomp (CI, no sudo).** Add a CI step on the ubuntu runner, or a docs_drift-style test that skips without systemd: `systemd-analyze syscall-filter @system-service | grep -qw flock`. This guards the shipped weir.service (`SystemCallFilter=@system-service`, `SystemCallErrorNumber=EPERM`, weir.service:108-109). If flock were filtered, the lock would get EPERM and the daemon would refuse to start.

**Chaos harness (owner-run on beast with passwordless sudo; blocks the release tag, not CI):**
- **SP-A: new `second_instance` fault kind.** This is new harness code, not a schedule change: `fault_kind` accepts only `kill_random` and `power_loss` (chaos/orchestrator/run.py:67-70). Estimate about 80-120 lines: a kind, a spawn of a second `weir-server` on the same `wab_dir` with its own socket and metrics port, an exit-status and log assertion, and wiring into the episode report. Assert per episode:
  - B exits non-zero within 5 s with `locked by`;
  - A's oracle shows 0 violations.

  Also confirm the orchestrator never starts episode N+1's daemon before episode N's process has exited.
- **SP-B: no-regression gate** on the standard A6 batched profile, ≥2 runs at a comparable cut count (baseline 2026-09-27: 1,936 cuts, 0 violations, duplicate-delivery ratio 1.062). Gates:
  - 0 violations;
  - the canary fires every episode;
  - the duplicate ratio is within noise of 1.062.

  A6 injects only kill and power loss, and its duplicates come from crash replay. The profile never exercises a corrupt header, a sidecar failure, a full DL cap or plain-mode SQL, so it **cannot** demonstrate an improvement. Revision 1's "should only lower it" is withdrawn.
- Coverage gaps, stated:
  - The harness unlinks the socket itself before every start (run.py:711-718), so the probe path gets no chaos coverage. The system tests cover it.
  - A crash cut runs `stack.teardown()` without the WAB post-mortem, so evidence from crash episodes is destroyed (an existing open item). Rely on the oracle, not on post-mortem files.

**Manual or host gates:**
- `docker compose up --scale weir=2` on the shared `wab_data` volume: the second replica refuses in preflight. Compose's `restart: unless-stopped` will crash-loop it (safe, noisy).
- `kill -HUP` under systemd: the unit stays active.
- 4.3 tmpfs-full run on beast (sudo).

---

## 8. Compatibility and release

- **Version.** Recommend **4.1.0 (minor)** (D1). Reasons: a new config key, new metrics, a new on-disk artifact, a new exit code, and new refuse-to-start conditions that can stop a previously "working" (unsafe) deployment. Foundations follows as 4.2.0.
  - Follow RELEASING.md: version appears seven times in Cargo.toml; there are three lockfiles; use `--locked`; use `pipefail` on publish.
  - The first PR after a release trips the `docs_drift` vintage guards.
- **Wire protocol.** Untouched. Ownership-lost uses the existing `InternalError` NACK.
- **Ack path.** One additional `Relaxed` `AtomicBool` load per push. No lock and no I/O.
- **weir-ctl JSON.** Unchanged.
- **WAB format.** Unchanged. The sidecar format is unchanged; only the write procedure differs.
- **Metric semantics (D11).** `weir_drain_segments_stranded_total` no longer counts local-I/O strands. Those move to the new `weir_drain_local_io_strands_total`. This aligns the counter with its documented meaning (metrics/mod.rs:628-634), but it is called out in the release notes.
- **Downgrade to 4.0.x is safe.**
  - An old binary ignores `.weir.lock` (not a directory) and `.tmp` files.
  - Orphan deletion only removes sealed files with a fsynced, valid tombstone; the tombstone stays, so the old counter scan still advances past it.
  - Nothing new is persisted.
  - Segments parked as `unsupported_version` are not touched on disk.
- **Docs to update:**
  - docs/wab_format.md: the layout gets `.weir.lock` and the transient `.confirmed.tmp`; :196 orphan handling; atomic sidecar
  - docs/security/threat-model.md:132: replace the "operator's responsibility" row with "wab_dir exclusively locked; live socket never stolen; proven ownership loss fails closed"; add the probe side effect, the lock-file deletion residual window (≤1 s), and the NFS caveat
  - docs/security/socket-bind.md: the probe table, the macOS ECONNREFUSED caveat, the O_PATH pin, and the non-Linux identity limitation
  - docs/platform-support.md:133-135: the flock requirement; NFS may grant weaker locks and is not a supported guard
  - deploy/systemd/README.md:51-56, 83-85: SIGHUP semantics; still no ExecReload; exit code 3
  - docs/operations/tcp-mtls.md:171-198
  - docs/monitoring.md:203-213 and :589, plus the new metric rows and the verdict table
  - docs/operations/configuration.md: `wab_dir_lock`; the plain-mode `id_column` recommendation
  - Dockerfile / compose comments: the scale>1 refusal (the stale-socket note at Dockerfile:106-110 stays true)
  - README.md:190-192 and weir-client lib.rs:39-40 (ordering, D8)
  - CHANGELOG
- **Behaviour changes in the release notes:**
  - refuses a second instance, a live socket, a taken port, invalid TLS or a non-locking FS, all before recovery;
  - exits 3 when another instance provably owns the `wab_dir`;
  - SIGHUP no longer exits;
  - corrupt sealed segments are quarantined on the first attempt, and newer-format segments are parked rather than quarantined;
  - transient read errors no longer quarantine; persistent media errors quarantine after 3 cycles and at least 10 minutes;
  - the drain resume cursor now survives strands within a run;
  - orphaned sealed files are deleted;
  - plain-mode duplicates: with `id_column` they count as committed, without it they are dead-lettered;
  - strand timing: no dead final wait, plus a per-segment `not_before`;
  - the stranded-counter semantics change (D11).

## 9. Risks

Listed in the structured `risks` field. The headline ones:
- flock under the shipped seccomp filter (now CI-gated)
- NFS granting emulated locks with weaker semantics
- the ≤1 s lock-deletion window before fail-closed
- fail-closed exit 3 turning a stolen-lock incident into an outage (intended)
- probe side effects and the shutdown window
- `dl_carry` memory (bounded at 64 MiB)
- plain-mode DL growth without `id_column`
- a quarantine-on-open of the newest segment can rewind the segment counter (an existing hazard class)
- drain-side changes are not covered by the DST harness, so the unit, system and sink-integration gates carry them

## 10. Owner decisions

Listed in the structured `owner_decisions` field:
- D1 release vehicle
- D2 non-locking filesystem policy
- D3 contention: fail-fast vs wait
- D4 lock-integrity loss, split by evidence (revised)
- D5 escalation target by error class (revised)
- D6 plain-mode duplicates by `id_column` (revised)
- D7 orphan deletion
- D8 ordering wording
- D9 ExecReload timing
- D10 explicit-missing `--config` timing
- D11 stranded-counter semantics for local-I/O strands (new)
