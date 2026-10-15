//! Exclusive ownership of a `wab_dir`: an `flock` on `<wab_dir>/.weir.lock`.
//!
//! Two daemons on one `wab_dir` corrupt each other. The second one's startup
//! recovery seals the first one's ACTIVE segment, its drain delivers, confirms
//! and deletes it, and the first daemon then acks Durable records into an
//! unlinked inode: false acks. Nothing in the WAB's own safety argument
//! ("single writer per shard", `segment.rs`) defends against that, so the
//! daemon takes this lock before anything reads the WAB and holds it until the
//! process is gone (spec §3.1).
//!
//! # Rules this module keeps
//!
//! - **The lock file is opened once, at acquisition, and never re-opened
//!   in-process.** Where `flock` is emulated with POSIX record locks (Linux on
//!   NFS with lockd / NFSv4), closing ANY fd to the file anywhere in the
//!   process drops the lock. The daemon caches its own holder record in memory
//!   for anything that wants to show it.
//! - **The fd is never released explicitly.** It lives in [`WAB_DIR_LOCK`] for
//!   the life of the process; the kernel releases the lock at exit, which is
//!   strictly after the flusher and drain joins, and which also covers early
//!   `?` returns from `main` while detached threads still run. A stack guard
//!   would drop the lock on those returns.
//! - **The lock file is never unlinked.** Unlink-on-exit opens the classic
//!   two-inode race (a starter that opened the old inode locks it after the
//!   unlink, while a third starter creates and locks a new one).
//! - **The holder record is written only after `flock` succeeds**, so a refused
//!   contender never truncates the owner's record.
//! - **Contention is never overridable.** `wab_dir_lock = "best-effort"` only
//!   covers a filesystem that cannot lock at all.
//! - weir-ctl never calls `flock`, not even a momentary probe: a probe can make
//!   a starting daemon fail with "locked by".

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing::warn;

use crate::config::Config;

/// The lock file's name, at the root of `wab_dir`. A dotfile on purpose:
/// `rm -rf wab/*` does not match it under default shell globbing. Every WAB
/// walker skips it already, because it is not a directory.
pub(crate) const LOCK_FILE_NAME: &str = ".weir.lock";

/// What to do when the filesystem under `wab_dir` cannot take an `flock`
/// (`ENOLCK`, `EOPNOTSUPP`, `ENOTSUP`, `ENOSYS`, `EINVAL`). Config key
/// `wab_dir_lock`. Contention is refused under both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockPolicy {
    /// Refuse to start (the default).
    Required,
    /// WARN on every start and run without the guard.
    BestEffort,
}

/// A successful acquisition.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LockOutcome {
    /// The exclusive lock is held for the life of the process.
    Held,
    /// The filesystem cannot lock and the policy is `BestEffort`: the daemon
    /// runs with NO guard against a second instance.
    Unsupported { errno: i32 },
}

/// A refused acquisition. Every variant stops startup.
///
/// `path` is the lock file, `<wab_dir>/.weir.lock`.
pub(crate) enum LockError {
    /// Another open file description holds the lock: another weir-server, in
    /// this or another process. `holder` is its record, parsed best effort.
    Held {
        holder: Option<HolderInfo>,
        path: PathBuf,
    },
    /// The filesystem cannot lock and the policy is `Required`.
    Unsupported { errno: i32, path: PathBuf },
    /// Anything else: `EACCES` on a root-owned lock file, `EROFS`, `ELOOP` from
    /// a symlinked lock path, `EPERM` from a seccomp filter, a directory or
    /// other non-regular file at the lock path.
    Io {
        source: std::io::Error,
        path: PathBuf,
    },
    /// The file at the lock path is not one this daemon may lock and rewrite.
    /// Refused before `flock`, and nothing is written to it.
    Unsafe {
        problem: UnsafeLockFile,
        path: PathBuf,
    },
}

/// Why a lock file is refused although it could be opened (or, for
/// `ForeignOwner`, although only its owner is wrong).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnsafeLockFile {
    /// More than one hard link. Taking the lock truncates and rewrites the
    /// file, which would overwrite every other name for the same inode (a
    /// config file, a TLS key, a sealed segment).
    HardLinked { links: u64 },
    /// Owned by another uid. That user can re-loosen its mode and hold the
    /// lock while this daemon is down, locking it out; and a non-root daemon
    /// cannot open it at all (a `sudo weir-server` run leaves one behind).
    ForeignOwner { owner: u32, euid: u32 },
}

impl UnsafeLockFile {
    /// The errno an integrity check reports for it (`LockIntegrity::Error`).
    #[cfg(unix)]
    fn errno(self) -> i32 {
        match self {
            UnsafeLockFile::HardLinked { .. } => libc::EMLINK,
            UnsafeLockFile::ForeignOwner { .. } => libc::EPERM,
        }
    }
}

/// Who holds (or last held) the lock. Written into the lock file as plain
/// `key=value` lines, no serde. A pid alone means nothing across pid
/// namespaces; host plus start time plus boot id identify the holder.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct HolderInfo {
    pub pid: u32,
    /// `gethostname(2)`; the pod name in Kubernetes.
    pub host: String,
    pub started_unix: u64,
    /// Linux only: `/proc/sys/kernel/random/boot_id`.
    pub boot_id: Option<String>,
    pub version: String,
    pub socket_path: String,
    /// `<metrics_bind>:<metrics_port>`.
    pub metrics: String,
}

/// The held lock, for the life of the process. Touched only at acquisition and
/// by the integrity check; never by the ack path or the drain.
static WAB_DIR_LOCK: Mutex<Option<HeldLock>> = Mutex::new(None);

/// The open file description that holds the lock, plus every other fd the
/// integrity check ever opened on that same file.
pub(crate) struct HeldLock {
    file: File,
    /// `lstat` ids of the lock path that differ from `file`'s `fstat` id but
    /// were proven, by opening the path, to be the held file (overlayfs before
    /// Linux 4.19 reports the copy-up origin through `stat` and the upper inode
    /// through `fstat`). Each keeps the fd that proved it open for as long as
    /// the lock is held: where `flock` is emulated with POSIX locks, closing
    /// ANY fd to the file drops the lock. One per distinct id, so a stable
    /// disagreement costs one fd, once.
    #[cfg_attr(not(unix), allow(dead_code))]
    aliases: Vec<((u64, u64), File)>,
}

impl HeldLock {
    fn new(file: File) -> Self {
        Self {
            file,
            aliases: Vec::new(),
        }
    }
}

/// This daemon's own holder record, cached at acquisition so the integrity
/// check can write it into a re-created lock file without rebuilding it, and
/// so nothing in-process ever needs to read `.weir.lock` back.
static OWN_RECORD: Mutex<Option<HolderInfo>> = Mutex::new(None);

/// The process exit code when the daemon proves another instance owns its
/// `wab_dir` (exit 1 stays "refused to start").
pub(crate) const EXIT_WAB_OWNERSHIP_LOST: i32 = 3;

/// One integrity check's verdict ([`check_wab_dir_lock`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LockIntegrity {
    /// The lock file at the path is the one held, and the lock is still ours.
    Ok,
    /// The lock file was deleted or replaced and nobody else held the new one:
    /// it was re-created and re-locked (D4a). Worth an ERROR, not a stop.
    Relocked,
    /// Another open file description holds the lock: proof that a second
    /// writer owns the directory (D4b). The daemon must stop acking and exit.
    LostToOther { holder: Option<HolderInfo> },
    /// Anything else (an errno). Logged and retried on the next tick.
    Error(i32),
}

impl HolderInfo {
    /// This process's record.
    pub(crate) fn current(config: &Config) -> Self {
        Self {
            pid: std::process::id(),
            host: hostname(),
            started_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            boot_id: boot_id(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            socket_path: config.socket_path.display().to_string(),
            metrics: std::net::SocketAddr::new(config.metrics_bind, config.metrics_port)
                .to_string(),
        }
    }

    /// The on-disk form. A newline inside a value would forge a key, so it is
    /// replaced; the record is informational and nothing parses it for trust.
    pub(crate) fn render(&self) -> String {
        fn clean(s: &str) -> String {
            s.replace(['\n', '\r'], " ")
        }
        let mut out = format!(
            "pid={}\nhost={}\nstarted_unix={}\n",
            self.pid,
            clean(&self.host),
            self.started_unix
        );
        if let Some(b) = &self.boot_id {
            out.push_str(&format!("boot_id={}\n", clean(b)));
        }
        out.push_str(&format!(
            "version={}\nsocket_path={}\nmetrics={}\n",
            clean(&self.version),
            clean(&self.socket_path),
            clean(&self.metrics)
        ));
        out
    }

    /// Best-effort parse. `None` when there is no parseable `pid=` line (an
    /// empty file: a holder that crashed between `flock` and the write, or a
    /// file nobody has written yet). Unknown keys are ignored, so a newer
    /// record still parses.
    ///
    /// The file is untrusted: anyone who can write it can put anything there,
    /// and its values reach a refusal message on stderr and the exit-3 log
    /// line. So only the first [`RECORD_MAX_BYTES`] are read, control and bidi
    /// formatting characters are stripped from every value, and a value longer
    /// than [`FIELD_MAX_BYTES`] is dropped (left empty) rather than echoed.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut end = text.len().min(RECORD_MAX_BYTES);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let mut info = HolderInfo::default();
        let mut pid = None;
        for line in text[..end].lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            let v = sanitise_field(v);
            match k {
                "pid" => pid = v.as_deref().and_then(|v| v.parse().ok()),
                "host" => info.host = v.unwrap_or_default(),
                "started_unix" => info.started_unix = v.and_then(|v| v.parse().ok()).unwrap_or(0),
                "boot_id" => info.boot_id = v,
                "version" => info.version = v.unwrap_or_default(),
                "socket_path" => info.socket_path = v.unwrap_or_default(),
                "metrics" => info.metrics = v.unwrap_or_default(),
                _ => {}
            }
        }
        info.pid = pid?;
        Some(info)
    }
}

/// The most of a lock file [`HolderInfo::parse`] reads. A real record is a few
/// hundred bytes.
const RECORD_MAX_BYTES: usize = 4096;

/// The longest holder-record value echoed anywhere. Host names are at most 255
/// bytes and `sun_path` about 104, so nothing real is longer.
const FIELD_MAX_BYTES: usize = 256;

/// One untrusted record value, made safe to print: control characters
/// (newlines, ESC, BEL, C1) and Unicode bidi/zero-width formatting are removed;
/// `None` when what is left is still longer than [`FIELD_MAX_BYTES`].
fn sanitise_field(v: &str) -> Option<String> {
    let invisible = |c: char| {
        c.is_control()
            || matches!(c,
                '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}' | '\u{FEFF}')
    };
    // Bail out before building a huge string: anything this long is dropped.
    if v.len() > FIELD_MAX_BYTES * 4 {
        return None;
    }
    let clean: String = v.chars().filter(|&c| !invisible(c)).collect();
    (clean.len() <= FIELD_MAX_BYTES).then_some(clean)
}

/// Takes the exclusive lock on `<wab_dir>/.weir.lock` and keeps it for the
/// life of the process.
///
/// Called once, from `main`, after the `weir starting` log line and before
/// anything reads the WAB (recovery, replay, the drain) and before every other
/// preflight step, so a duplicate on the same `wab_dir` reports "locked by"
/// rather than "metrics port in use".
// `LockError::Held` carries the holder record by value, as the safety-floor
// plan's interface names it. This runs once, at startup, on the refusal path,
// so the size of the `Err` is irrelevant.
#[allow(clippy::result_large_err)]
pub(crate) fn acquire_wab_dir_lock(
    wab_dir: &Path,
    policy: LockPolicy,
    holder: &HolderInfo,
) -> Result<LockOutcome, LockError> {
    let path = wab_dir.join(LOCK_FILE_NAME);
    match try_lock_path(&path, holder) {
        Ok(file) => {
            *WAB_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner()) = Some(HeldLock::new(file));
            *OWN_RECORD.lock().unwrap_or_else(|e| e.into_inner()) = Some(holder.clone());
            Ok(LockOutcome::Held)
        }
        Err(LockError::Unsupported { errno, path }) if policy == LockPolicy::BestEffort => {
            warn!(
                lock = %path.display(),
                errno,
                error = %std::io::Error::from_raw_os_error(errno),
                "wab_dir_lock = \"best-effort\": the filesystem under wab_dir cannot take an \
                 exclusive lock, so NOTHING stops a second weir-server from recovering this \
                 daemon's live segments and turning its acks into false acks. Run the WAB on a \
                 local filesystem that supports flock"
            );
            Ok(LockOutcome::Unsupported { errno })
        }
        Err(e) => Err(e),
    }
}

/// Checks, once, that this daemon still owns `wab_dir` (spec §3.1, D4).
///
/// Called every second by the integrity thread in `main`, from right after
/// acquisition, only when the lock is [`LockOutcome::Held`]. It guards against the lock
/// being defeated by deleting or replacing `.weir.lock`: a daemon started
/// after that would create and lock a fresh file and recover this daemon's
/// live segments.
///
/// It never opens the lock path except to re-create it after proving the file
/// at the path is no longer the held one; the held fd is only `fstat`ed and
/// re-flocked.
pub(crate) fn check_wab_dir_lock(wab_dir: &Path) -> LockIntegrity {
    let own = OWN_RECORD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default();
    let mut slot = WAB_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    check_lock_slot(wab_dir, &mut slot, &own)
}

/// [`check_wab_dir_lock`] against an explicit slot, so tests can exercise it
/// without the process-wide one.
///
/// Each check:
/// 1. `lstat`s the path and compares (dev, ino) with `fstat` of the held fd
///    (and with any id already proven to name the held file, see
///    [`HeldLock::aliases`]);
/// 2. re-issues `flock(held, LOCK_EX | LOCK_NB)`. On a local filesystem that
///    is a no-op conversion that returns 0. Where flock is emulated with POSIX
///    locks (NFS) it returns `EWOULDBLOCK` once a lease was lost and another
///    client took the lock;
/// 3. classifies: match and 0 is `Ok`; contention on the held fd is
///    `LostToOther`; a mismatch or a missing file goes to [`recreate_lock`];
///    any other errno is `Error`.
#[cfg(unix)]
fn check_lock_slot(wab_dir: &Path, slot: &mut Option<HeldLock>, own: &HolderInfo) -> LockIntegrity {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;

    let Some(held) = slot.as_mut() else {
        // Nothing held (never acquired, or best-effort). The integrity thread is
        // not spawned then; report it rather than take a lock here.
        return LockIntegrity::Error(libc::EBADF);
    };
    let path = wab_dir.join(LOCK_FILE_NAME);
    let errno_of = |e: &std::io::Error| e.raw_os_error().unwrap_or(libc::EIO);

    // 1. What is at the path now. lstat: a symlink planted there is not the
    // held file, whatever it points at.
    #[allow(unused_mut)] // reassigned only by the test hook
    let mut at_path = match std::fs::symlink_metadata(&path) {
        Ok(m) => Some((m.dev(), m.ino())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return LockIntegrity::Error(errno_of(&e)),
    };
    #[cfg(test)]
    if let Some(fake) = tests::LSTAT_HOOK.with(|h| h.get()) {
        at_path = fake;
    }
    let held_id = match held.file.metadata() {
        Ok(m) => (m.dev(), m.ino()),
        Err(e) => return LockIntegrity::Error(errno_of(&e)),
    };

    // 2. Re-flock the held description.
    match flock_exclusive_nonblocking(held.file.as_raw_fd()) {
        Ok(()) => {}
        Err(errno) if errno == libc::EWOULDBLOCK => {
            return LockIntegrity::LostToOther {
                holder: read_holder(&held.file),
            };
        }
        Err(errno) => return LockIntegrity::Error(errno),
    }

    // 3. Classify.
    let ours = |id| id == held_id || held.aliases.iter().any(|(a, _)| *a == id);
    if at_path.is_some_and(ours) {
        return LockIntegrity::Ok;
    }
    match recreate_lock(&path, at_path.is_none(), held_id, own) {
        Recreated::SameFile(file) => {
            // conf-regressions-12: the path names the held file after all, and
            // lstat and fstat just disagree about its id. Keep the fd that
            // proved it (closing it could drop a POSIX-emulated lock) and
            // remember the id, so later ticks need no open.
            warn!(
                lock = %path.display(),
                "lstat of the wab_dir lock file reports a different (dev, ino) than the held \
                 descriptor, but opening the path reaches the same file; treating that id as \
                 this lock (an overlayfs on Linux before 4.19 does this)"
            );
            held.aliases.push((at_path.unwrap_or(held_id), file));
            LockIntegrity::Ok
        }
        Recreated::Locked(file) => {
            // The old fd (and any alias) refers to an unlinked or renamed-over
            // inode nobody can reach by this path any more; dropping it is
            // harmless.
            *slot = Some(HeldLock::new(file));
            LockIntegrity::Relocked
        }
        Recreated::Verdict(v) => v,
    }
}

/// What [`recreate_lock`] found.
#[cfg(unix)]
enum Recreated {
    /// The path, opened, is the held file: an `lstat`/`fstat` disagreement,
    /// not a replacement. Carries the fd that proved it, to keep.
    SameFile(File),
    /// A file at the path is now locked by this daemon and carries its record.
    Locked(File),
    /// Anything else, as the integrity verdict to report.
    Verdict(LockIntegrity),
}

/// The integrity check's re-create, once the path no longer shows the held
/// file. Through the one opener, and never truncating evidence:
///
/// - The path was **missing**: create a fresh file with `O_EXCL`, so a file
///   somebody put there after the `lstat` (including a hard link) is never
///   adopted (`EEXIST`, an error; the next tick inspects it as a replacement).
/// - The path names **another file**: open it without creating, and
///   1. if it is the held file after all (same `fstat` id), say so (`SameFile`);
///   2. vet it (regular, one link, our uid), so nothing unsafe is written;
///   3. `flock` it: contention is a second daemon holding it, `LostToOther`;
///   4. read its record BEFORE writing ours: a record naming another daemon is
///      proof that one held this wab_dir after our file went (it may have
///      recovered our live segments and exited since), so `LostToOther`, and
///      its record, the only evidence, is left in place (conf-durability-2).
///      An empty or unparseable file, or a copy of our own record, is no such
///      evidence and is re-locked as a deleted file would be.
#[cfg(unix)]
fn recreate_lock(path: &Path, missing: bool, held_id: (u64, u64), own: &HolderInfo) -> Recreated {
    use std::os::unix::fs::MetadataExt;
    let how = if missing {
        OpenHow::CreateNew
    } else {
        OpenHow::Existing
    };
    let file = match open_lock_path(path, how) {
        Ok(f) => f,
        Err(e) => return Recreated::Verdict(integrity_error(e)),
    };
    match file.metadata() {
        Ok(m) if (m.dev(), m.ino()) == held_id => return Recreated::SameFile(file),
        Ok(_) => {}
        Err(e) => {
            return Recreated::Verdict(LockIntegrity::Error(e.raw_os_error().unwrap_or(libc::EIO)));
        }
    }
    if let Err(e) = vet_lock_file(&file, path) {
        return Recreated::Verdict(integrity_error(e));
    }
    if let Err(e) = take_lock(&file, path) {
        return Recreated::Verdict(match e {
            LockError::Held { holder, .. } => LockIntegrity::LostToOther { holder },
            e => integrity_error(e),
        });
    }
    if let Some(found) = read_holder(&file)
        && !same_holder(&found, own)
    {
        return Recreated::Verdict(LockIntegrity::LostToOther {
            holder: Some(found),
        });
    }
    write_record(&file, path, own);
    Recreated::Locked(file)
}

/// Whether a record read back from the lock file names this daemon. Compared
/// on the identifying fields, after this daemon's own record went through the
/// same sanitising parse.
#[cfg(unix)]
fn same_holder(found: &HolderInfo, own: &HolderInfo) -> bool {
    let own = HolderInfo::parse(&own.render()).unwrap_or_else(|| own.clone());
    found.pid == own.pid
        && found.host == own.host
        && found.started_unix == own.started_unix
        && found.boot_id == own.boot_id
}

/// A refusal met by the re-create, as an integrity `Error` to retry. An unsafe
/// file is also logged with the refusal's own explanation, which an errno
/// alone would not carry.
#[cfg(unix)]
fn integrity_error(e: LockError) -> LockIntegrity {
    match e {
        LockError::Held { .. } => LockIntegrity::Error(libc::EWOULDBLOCK),
        LockError::Unsupported { errno, .. } => LockIntegrity::Error(errno),
        LockError::Unsafe { problem, .. } => {
            tracing::error!("{e}");
            LockIntegrity::Error(problem.errno())
        }
        LockError::Io { source, .. } => LockIntegrity::Error(source.raw_os_error().unwrap_or(
            if source.kind() == std::io::ErrorKind::InvalidInput {
                libc::EINVAL
            } else {
                libc::EIO
            },
        )),
    }
}

/// No flock on non-Unix builds: the lock is never `Held` there, so the
/// integrity thread is never spawned.
#[cfg(not(unix))]
fn check_lock_slot(
    _wab_dir: &Path,
    _slot: &mut Option<HeldLock>,
    _own: &HolderInfo,
) -> LockIntegrity {
    LockIntegrity::Ok
}

/// The held lock's fd. `None` before acquisition and under
/// `LockOutcome::Unsupported`.
#[cfg(unix)]
#[cfg_attr(not(test), allow(dead_code))] // the integrity check reads the slot directly
pub(crate) fn held_lock_fd() -> Option<std::os::fd::RawFd> {
    use std::os::fd::AsRawFd;
    WAB_DIR_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map(|h| h.file.as_raw_fd())
}

/// Opens, checks and locks one lock path, returning the open `File` that holds
/// the lock. Bypasses [`WAB_DIR_LOCK`], so tests can take and drop locks
/// without poisoning the process-wide slot.
///
/// Acquisition's path. The integrity check's re-create ([`recreate_lock`])
/// runs the same steps with a stricter open and a look at the old record. Both
/// go through [`open_lock_path`], the ONLY opener of the lock path (a
/// `lock_module_never_reopens_path` test pins this).
#[cfg(unix)]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn try_lock_path(path: &Path, holder: &HolderInfo) -> Result<File, LockError> {
    let file = open_lock_path(path, OpenHow::OpenOrCreate)?;
    vet_lock_file(&file, path)?;
    take_lock(&file, path)?;
    write_record(&file, path, holder);
    Ok(file)
}

/// How [`open_lock_path`] may treat the path.
#[cfg(unix)]
#[derive(Clone, Copy)]
enum OpenHow {
    /// Acquisition: the file normally exists from the previous run.
    OpenOrCreate,
    /// Integrity re-create of a missing file: `O_EXCL`, never adopt one.
    CreateNew,
    /// Integrity inspection of a file that replaced ours: never create.
    Existing,
}

/// The one `open` of the lock path. `O_NOFOLLOW`: a symlink at the lock path
/// fails with `ELOOP` rather than locking (and writing into) whatever it points
/// at. std adds `O_CLOEXEC`.
///
/// A permission error on a file another uid owns becomes
/// [`UnsafeLockFile::ForeignOwner`], so the refusal names the owner and the
/// fix instead of a bare "Permission denied" (a root-owned file left by a
/// `sudo weir-server` run is the common case).
#[cfg(unix)]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn open_lock_path(path: &Path, how: OpenHow) -> Result<File, LockError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    match how {
        OpenHow::OpenOrCreate => options.create(true),
        OpenHow::CreateNew => options.create_new(true),
        OpenHow::Existing => &mut options,
    };
    options.open(path).map_err(|source| {
        let denied = matches!(source.raw_os_error(), Some(libc::EACCES | libc::EPERM));
        // lstat, not an open: it never touches a lock.
        let owner = std::fs::symlink_metadata(path).ok().map(|m| m.uid());
        match owner {
            Some(owner) if denied && owner != euid() => LockError::Unsafe {
                problem: UnsafeLockFile::ForeignOwner {
                    owner,
                    euid: euid(),
                },
                path: path.to_path_buf(),
            },
            _ => LockError::Io {
                source,
                path: path.to_path_buf(),
            },
        }
    })
}

/// Checks an opened lock file BEFORE it is locked or written: a regular file,
/// one hard link, owned by this daemon's uid. Any failure leaves it untouched.
#[cfg(unix)]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn vet_lock_file(file: &File, path: &Path) -> Result<(), LockError> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata().map_err(|source| LockError::Io {
        source,
        path: path.to_path_buf(),
    })?;
    // A directory fails the open (EISDIR); a FIFO or a device opens fine and
    // must not be locked or written as if it were the record.
    if !meta.file_type().is_file() {
        return Err(LockError::Io {
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the lock path exists and is not a regular file",
            ),
            path: path.to_path_buf(),
        });
    }
    // O_NOFOLLOW stops symlinks, not hard links: a hard link planted at the
    // path is a regular file whose other names would be truncated below.
    if meta.nlink() != 1 {
        return Err(LockError::Unsafe {
            problem: UnsafeLockFile::HardLinked {
                links: meta.nlink(),
            },
            path: path.to_path_buf(),
        });
    }
    if meta.uid() != euid() {
        return Err(LockError::Unsafe {
            problem: UnsafeLockFile::ForeignOwner {
                owner: meta.uid(),
                euid: euid(),
            },
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// `flock(LOCK_EX | LOCK_NB)` on a vetted lock file, classified.
#[cfg(unix)]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn take_lock(file: &File, path: &Path) -> Result<(), LockError> {
    use std::os::fd::AsRawFd;
    flock_exclusive_nonblocking(file.as_raw_fd())
        .map_err(|errno| classify_flock_errno(errno, file, path))
}

/// Run only once the lock is held, so a refused contender never touches the
/// owner's file: tightens a loose mode to 0600 (another user who can open the
/// file could otherwise hold it while this daemon is down, locking it out), then
/// replaces the record. Informational, so no fsync, and a failure here does not
/// give the lock back.
#[cfg(unix)]
fn write_record(file: &File, path: &Path, holder: &HolderInfo) {
    use std::os::unix::fs::{FileExt, PermissionsExt};
    if let Ok(meta) = file.metadata() {
        let mode = meta.permissions().mode() & 0o7777;
        if mode != 0o600 {
            match file.set_permissions(std::fs::Permissions::from_mode(0o600)) {
                Ok(()) => warn!(
                    lock = %path.display(),
                    was = format_args!("{mode:o}"),
                    "wab_dir lock file was not mode 0600; set it to 0600"
                ),
                Err(e) => warn!(
                    lock = %path.display(),
                    mode = format_args!("{mode:o}"),
                    error = %e,
                    "wab_dir lock file is not mode 0600 and could not be set to 0600"
                ),
            }
        }
    }
    let record = holder.render();
    if let Err(e) = file
        .set_len(0)
        .and_then(|()| file.write_all_at(record.as_bytes(), 0))
    {
        warn!(
            lock = %path.display(),
            error = %e,
            "wab_dir lock is held, but the holder record could not be written; a refused \
             second instance will not be able to name this daemon"
        );
    }
}

/// `geteuid(2)`; a test hook stands in for another user.
#[cfg(unix)]
fn euid() -> u32 {
    #[cfg(test)]
    if let Some(uid) = tests::EUID_HOOK.with(|h| h.get()) {
        return uid;
    }
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn classify_flock_errno(errno: i32, file: &File, path: &Path) -> LockError {
    if errno == libc::EWOULDBLOCK {
        return LockError::Held {
            holder: read_holder(file),
            path: path.to_path_buf(),
        };
    }
    // ENOTSUP and EOPNOTSUPP are distinct on macOS (a shipped target) and equal
    // on Linux, so compare rather than match (a duplicate pattern would lint).
    let unsupported = [
        libc::ENOLCK,
        libc::EOPNOTSUPP,
        libc::ENOTSUP,
        libc::ENOSYS,
        libc::EINVAL,
    ];
    if unsupported.contains(&errno) {
        return LockError::Unsupported {
            errno,
            path: path.to_path_buf(),
        };
    }
    LockError::Io {
        source: std::io::Error::from_raw_os_error(errno),
        path: path.to_path_buf(),
    }
}

/// Reads the owner's record through the contender's own fd. Reading does not
/// disturb the owner's lock (it is the owner that must never close a second
/// fd, not a contender).
#[cfg(unix)]
fn read_holder(file: &File) -> Option<HolderInfo> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; RECORD_MAX_BYTES];
    let n = file.read_at(&mut buf, 0).ok()?;
    HolderInfo::parse(&String::from_utf8_lossy(&buf[..n]))
}

/// `flock(fd, LOCK_EX | LOCK_NB)`, returning the errno on failure.
#[cfg(unix)]
fn flock_exclusive_nonblocking(fd: std::os::fd::RawFd) -> Result<(), i32> {
    #[cfg(test)]
    if let Some(errno) = tests::FLOCK_HOOK.with(|h| h.get()) {
        return Err(errno);
    }
    // SAFETY: flock takes an fd and an operation flag and touches no memory.
    // `fd` is owned by a live `File` for the duration of the call.
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    }
}

/// weir-server has no ingest path on non-Unix builds (`mod socket` is
/// `#[cfg(unix)]`), but the module must still compile: report the filesystem
/// as unable to lock, which the policy then refuses or tolerates.
#[cfg(not(unix))]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn try_lock_path(path: &Path, _holder: &HolderInfo) -> Result<File, LockError> {
    Err(LockError::Unsupported {
        errno: 0,
        path: path.to_path_buf(),
    })
}

#[cfg(unix)]
fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the pointer and length describe `buf`, which outlives the call.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

#[cfg(not(unix))]
fn hostname() -> String {
    std::env::var("COMPUTERNAME").unwrap_or_default()
}

fn boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` for a unix timestamp. Howard Hinnant's
/// `civil_from_days` (public domain), as in `weir-sink-s3`'s `time.rs`; copied
/// rather than shared because that crate is an optional sink.
fn format_utc(unix: u64) -> String {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3_600,
        (secs / 60) % 60,
        secs % 60
    )
}

impl LockError {
    fn lock_path(&self) -> &Path {
        match self {
            LockError::Held { path, .. }
            | LockError::Unsupported { path, .. }
            | LockError::Io { path, .. }
            | LockError::Unsafe { path, .. } => path,
        }
    }

    fn wab_dir(&self) -> std::path::Display<'_> {
        self.lock_path()
            .parent()
            .unwrap_or_else(|| self.lock_path())
            .display()
    }
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Held { holder, .. } => {
                write!(
                    f,
                    "wab_dir {} is locked by another weir-server (",
                    self.wab_dir()
                )?;
                match holder {
                    Some(h) => write!(
                        f,
                        "pid {} on host {}, started {}, version {}, socket {}",
                        h.pid,
                        h.host,
                        format_utc(h.started_unix),
                        h.version,
                        h.socket_path
                    )?,
                    None => write!(f, "its holder record could not be read")?,
                }
                write!(
                    f,
                    "). Two daemons on one wab_dir corrupt each other's segments and can lose \
                     acked records. Stop the other instance or point this one at a different \
                     wab_dir."
                )
            }
            LockError::Unsupported { errno, path } => write!(
                f,
                "wab_dir {} is on a filesystem that cannot take an exclusive lock on {} ({}). \
                 Without the lock nothing stops a second weir-server on the same wab_dir from \
                 losing acked records. Move the WAB to a local filesystem that supports flock, \
                 or set wab_dir_lock = \"best-effort\" to run without this guard.",
                self.wab_dir(),
                path.display(),
                std::io::Error::from_raw_os_error(*errno)
            ),
            LockError::Io { source, path } => write!(
                f,
                "cannot lock wab_dir {}: {}: {source}. weir-server refuses to start without \
                 exclusive ownership of its wab_dir.",
                self.wab_dir(),
                path.display()
            ),
            LockError::Unsafe {
                problem: UnsafeLockFile::HardLinked { links },
                path,
            } => write!(
                f,
                "cannot lock wab_dir {}: {} has {links} hard links. Taking the lock rewrites \
                 the file, which would overwrite every other name for the same file, so \
                 weir-server refuses it and nothing was written. Something replaced the lock \
                 file with a hard link: find out what. If no weir-server is running on this \
                 wab_dir, remove {} (the other names keep their contents) and start again.",
                self.wab_dir(),
                path.display(),
                path.display()
            ),
            LockError::Unsafe {
                problem: UnsafeLockFile::ForeignOwner { owner, euid },
                path,
            } => write!(
                f,
                "cannot lock wab_dir {}: {} is owned by uid {owner}, not by this daemon's uid \
                 {euid}. Another user's lock file can be loosened and held by that user, \
                 locking this daemon out, so weir-server refuses it. A previous run as another \
                 user (for example `sudo weir-server`) leaves one behind. If no weir-server is \
                 running on this wab_dir, either run weir-server as uid {owner}, or give the \
                 file to this daemon's user as root: `chown {euid} {}`.",
                self.wab_dir(),
                path.display(),
                path.display()
            ),
        }
    }
}

/// `main` returns this through `Box<dyn Error>`, and the runtime prints a
/// returned error with `Debug`; delegate so stderr carries the sentence, not a
/// struct dump (as `ConfigError` does).
impl std::fmt::Debug for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        /// When set, `flock_exclusive_nonblocking` fails with this errno
        /// instead of calling `libc::flock`. Per thread, so parallel tests
        /// cannot see each other's injection.
        pub(super) static FLOCK_HOOK: Cell<Option<i32>> = const { Cell::new(None) };
    }

    thread_local! {
        /// When set, the lock module's euid reads this instead of geteuid(2),
        /// so a test can look like another user to its own files.
        pub(super) static EUID_HOOK: Cell<Option<u32>> = const { Cell::new(None) };
    }

    thread_local! {
        /// When set, the integrity check sees this as the lock path's `lstat`
        /// (dev, ino) (`None`: the path is missing) instead of the real one,
        /// to stand in for a filesystem whose stat and fstat disagree.
        pub(super) static LSTAT_HOOK: Cell<Option<Option<(u64, u64)>>> = const { Cell::new(None) };
    }

    struct LstatGuard;
    impl LstatGuard {
        fn set(id: Option<(u64, u64)>) -> Self {
            LSTAT_HOOK.with(|h| h.set(Some(id)));
            LstatGuard
        }
    }
    impl Drop for LstatGuard {
        fn drop(&mut self) {
            LSTAT_HOOK.with(|h| h.set(None));
        }
    }

    struct EuidGuard;
    impl EuidGuard {
        fn set(uid: u32) -> Self {
            EUID_HOOK.with(|h| h.set(Some(uid)));
            EuidGuard
        }
    }
    impl Drop for EuidGuard {
        fn drop(&mut self) {
            EUID_HOOK.with(|h| h.set(None));
        }
    }

    /// Clears the hook even if the test panics.
    struct HookGuard;
    impl HookGuard {
        fn set(errno: i32) -> Self {
            FLOCK_HOOK.with(|h| h.set(Some(errno)));
            HookGuard
        }
    }
    impl Drop for HookGuard {
        fn drop(&mut self) {
            FLOCK_HOOK.with(|h| h.set(None));
        }
    }

    /// A fresh, empty scratch dir standing in for a `wab_dir`.
    fn wab_dir(label: &str) -> PathBuf {
        let dir = crate::testutil::scratch_dir(&format!("lock_{label}"));
        let lock = dir.join(LOCK_FILE_NAME);
        if lock.is_dir() {
            std::fs::remove_dir_all(&lock).unwrap();
        } else {
            let _ = std::fs::remove_file(&lock);
        }
        dir
    }

    fn holder(host: &str) -> HolderInfo {
        HolderInfo {
            pid: std::process::id(),
            host: host.to_string(),
            started_unix: 1_759_572_000, // 2025-10-04T10:00:00Z
            boot_id: Some("2f1c0e6a-0000-4000-8000-000000000000".to_string()),
            version: "9.9.9".to_string(),
            socket_path: "/run/weir/weir.sock".to_string(),
            metrics: "127.0.0.1:9185".to_string(),
        }
    }

    #[test]
    fn second_acquire_in_same_process_reports_holder() {
        let dir = wab_dir("second_acquire");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("weir-0");
        // The first lock is a separate open file description that the test
        // keeps; flock conflicts between descriptions even in one process.
        let _held = try_lock_path(&lock, &owner).expect("first acquire must succeed");

        let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("contender"))
            .expect_err("a second acquire must be refused");
        match &err {
            LockError::Held {
                holder: Some(h),
                path,
            } => {
                assert_eq!(h.pid, std::process::id());
                assert_eq!(h, &owner, "the refusal names the OWNER, not the contender");
                assert_eq!(path, &lock);
            }
            other => panic!("expected Held with a parsed holder, got {other}"),
        }
        let msg = err.to_string();
        for needle in [
            "is locked by another weir-server",
            &format!("pid {}", std::process::id()),
            "on host weir-0",
            "started 2025-10-04T10:00:00Z",
            "version 9.9.9",
            "socket /run/weir/weir.sock",
            "Stop the other instance or point this one at a different wab_dir.",
            &dir.display().to_string(),
        ] {
            assert!(msg.contains(needle), "{needle:?} missing from: {msg}");
        }

        // best-effort only covers a filesystem that cannot lock; contention is
        // never overridable.
        let err = acquire_wab_dir_lock(&dir, LockPolicy::BestEffort, &holder("contender"))
            .expect_err("best-effort must not override contention");
        assert!(matches!(err, LockError::Held { .. }), "{err}");
    }

    #[test]
    fn lock_is_free_again_once_the_holder_closes_it() {
        let dir = wab_dir("reacquire");
        let lock = dir.join(LOCK_FILE_NAME);
        let first = try_lock_path(&lock, &holder("a")).unwrap();
        drop(first);
        let second = try_lock_path(&lock, &holder("b")).expect("released on close");
        drop(second);
    }

    #[test]
    fn symlinked_lock_file_is_refused() {
        let dir = wab_dir("symlink");
        let target = dir.join("elsewhere");
        let _ = std::fs::remove_file(&target);
        std::fs::write(&target, b"").unwrap();
        std::os::unix::fs::symlink(&target, dir.join(LOCK_FILE_NAME)).unwrap();

        let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h"))
            .expect_err("a symlinked lock path must be refused");
        match &err {
            LockError::Io { source, .. } => {
                assert_eq!(source.raw_os_error(), Some(libc::ELOOP), "{err}")
            }
            other => panic!("expected Io(ELOOP), got {other}"),
        }
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"",
            "the symlink target must not have been written"
        );
        // best-effort does not soften it either.
        assert!(
            acquire_wab_dir_lock(&dir, LockPolicy::BestEffort, &holder("h")).is_err(),
            "ELOOP is not 'the filesystem cannot lock'"
        );
        let _ = std::fs::remove_file(dir.join(LOCK_FILE_NAME));
    }

    #[test]
    fn directory_at_lock_path_is_refused() {
        let dir = wab_dir("is_dir");
        std::fs::create_dir(dir.join(LOCK_FILE_NAME)).unwrap();
        let err = acquire_wab_dir_lock(&dir, LockPolicy::BestEffort, &holder("h"))
            .expect_err("a directory at the lock path must be refused");
        assert!(matches!(err, LockError::Io { .. }), "{err}");
        assert!(err.to_string().contains("cannot lock wab_dir"), "{err}");
        std::fs::remove_dir(dir.join(LOCK_FILE_NAME)).unwrap();
    }

    #[test]
    fn fifo_at_lock_path_is_refused_as_not_a_regular_file() {
        let dir = wab_dir("fifo");
        let path = dir.join(LOCK_FILE_NAME);
        let c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: `c` is a valid NUL-terminated path for the call's duration.
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
        let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h"))
            .expect_err("a FIFO at the lock path must be refused");
        assert!(
            err.to_string().contains("not a regular file"),
            "expected the S_IFREG refusal, got: {err}"
        );
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn holder_record_round_trips() {
        let h = holder("weir-0");
        assert_eq!(HolderInfo::parse(&h.render()), Some(h.clone()));

        let no_boot = HolderInfo {
            boot_id: None,
            ..h.clone()
        };
        assert_eq!(HolderInfo::parse(&no_boot.render()), Some(no_boot));

        // An empty file (a holder that died between flock and the write) has
        // no record; unknown keys from a newer version are ignored.
        assert_eq!(HolderInfo::parse(""), None);
        let newer = format!("{}future_key=1\n", h.render());
        assert_eq!(HolderInfo::parse(&newer), Some(h.clone()));

        // A newline inside a value cannot forge a key.
        let hostile = HolderInfo {
            host: "evil\npid=1".to_string(),
            ..h.clone()
        };
        assert_eq!(HolderInfo::parse(&hostile.render()).unwrap().pid, h.pid);
    }

    #[test]
    fn refused_contender_does_not_modify_holder_record() {
        let dir = wab_dir("contender_untouched");
        let lock = dir.join(LOCK_FILE_NAME);
        let _held = try_lock_path(&lock, &holder("owner")).unwrap();
        let before = std::fs::read(&lock).unwrap();
        assert!(!before.is_empty(), "the owner wrote its record");

        let other = HolderInfo {
            pid: 1,
            ..holder("a-much-longer-contender-host-name")
        };
        assert!(acquire_wab_dir_lock(&dir, LockPolicy::Required, &other).is_err());
        assert_eq!(
            std::fs::read(&lock).unwrap(),
            before,
            "a refused contender must neither truncate nor overwrite the owner's record"
        );
    }

    #[test]
    fn holder_record_replaces_a_longer_stale_record() {
        let dir = wab_dir("stale_record");
        let lock = dir.join(LOCK_FILE_NAME);
        std::fs::write(&lock, "x".repeat(1000)).unwrap();
        let h = holder("short");
        let _held = try_lock_path(&lock, &h).unwrap();
        assert_eq!(std::fs::read_to_string(&lock).unwrap(), h.render());
    }

    #[test]
    fn unsupported_errno_refuses_when_required_and_warns_when_best_effort() {
        for errno in [libc::ENOLCK, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENOSYS] {
            let dir = wab_dir("unsupported");
            let _hook = HookGuard::set(errno);

            let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h"))
                .expect_err("required must refuse a filesystem that cannot lock");
            match &err {
                LockError::Unsupported { errno: e, .. } => assert_eq!(*e, errno),
                other => panic!("expected Unsupported({errno}), got {other}"),
            }
            assert!(err.to_string().contains("best-effort"), "{err}");

            let outcome = acquire_wab_dir_lock(&dir, LockPolicy::BestEffort, &holder("h"))
                .expect("best-effort must start without the lock");
            assert_eq!(outcome, LockOutcome::Unsupported { errno });
            assert!(
                held_lock_fd().is_none(),
                "best-effort without a lock must not record one as held"
            );
            assert_eq!(
                std::fs::read(dir.join(LOCK_FILE_NAME)).unwrap(),
                b"",
                "no record is written without the lock"
            );
        }
    }

    #[test]
    fn other_flock_errors_refuse_under_both_policies() {
        let dir = wab_dir("eperm");
        let _hook = HookGuard::set(libc::EPERM); // a seccomp filter
        for policy in [LockPolicy::Required, LockPolicy::BestEffort] {
            let err = acquire_wab_dir_lock(&dir, policy, &holder("h")).unwrap_err();
            match &err {
                LockError::Io { source, .. } => {
                    assert_eq!(source.raw_os_error(), Some(libc::EPERM))
                }
                other => panic!("expected Io(EPERM) under {policy:?}, got {other}"),
            }
        }
    }

    // ── Integrity check (spec §3.1 integrity, D4) ────────────────────────────
    //
    // These drive `check_lock_slot` with a LOCAL slot standing in for
    // `WAB_DIR_LOCK`, so they never populate the process-wide one (another
    // test asserts it stays empty under best-effort).

    /// Takes the lock on `dir`'s lock path, as `acquire_wab_dir_lock` would.
    fn held_slot(dir: &Path, h: &HolderInfo) -> Option<HeldLock> {
        Some(HeldLock::new(
            try_lock_path(&dir.join(LOCK_FILE_NAME), h).expect("acquire"),
        ))
    }

    /// (dev, ino) of an open file and of whatever is at a path now.
    fn fd_id(f: &File) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let m = f.metadata().unwrap();
        (m.dev(), m.ino())
    }
    fn path_id(p: &Path) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::symlink_metadata(p).unwrap();
        (m.dev(), m.ino())
    }

    #[test]
    fn intact_lock_checks_ok_and_leaves_the_record_alone() {
        let dir = wab_dir("integrity_ok");
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let before = fd_id(&slot.as_ref().unwrap().file);
        for _ in 0..3 {
            // Re-issuing LOCK_EX on the held description is a no-op conversion.
            assert_eq!(check_lock_slot(&dir, &mut slot, &owner), LockIntegrity::Ok);
        }
        assert_eq!(
            fd_id(&slot.as_ref().unwrap().file),
            before,
            "the held fd is kept"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join(LOCK_FILE_NAME)).unwrap(),
            owner.render()
        );
        // Still exclusive: the check must not have weakened the lock.
        assert!(matches!(
            try_lock_path(&dir.join(LOCK_FILE_NAME), &holder("x")),
            Err(LockError::Held { .. })
        ));
    }

    /// D4a: the file was deleted by hand and nobody else holds the directory.
    /// Re-create it, re-lock it, keep running.
    #[test]
    fn unlink_and_recreate_relocks() {
        let dir = wab_dir("integrity_relock");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let old = fd_id(&slot.as_ref().unwrap().file);

        std::fs::remove_file(&lock).unwrap();
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Relocked
        );

        let new = fd_id(&slot.as_ref().expect("the slot still holds a lock").file);
        assert_ne!(new, old, "the slot must now hold the RE-CREATED file");
        assert_eq!(new, path_id(&lock), "and it is the file at the lock path");
        assert_eq!(
            std::fs::read_to_string(&lock).unwrap(),
            owner.render(),
            "the re-created file carries this daemon's record"
        );
        assert!(
            matches!(
                try_lock_path(&lock, &holder("contender")),
                Err(LockError::Held { .. })
            ),
            "a newcomer is refused again once the lock is re-taken"
        );
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Ok,
            "the next tick sees an intact lock"
        );
    }

    /// The same, when the path is REPLACED (a rename over it) rather than
    /// removed: the inode differs, nobody holds the new one.
    #[test]
    fn replaced_lock_file_relocks() {
        let dir = wab_dir("integrity_replaced");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);

        let stray = dir.join("stray");
        std::fs::write(&stray, b"junk").unwrap();
        std::fs::rename(&stray, &lock).unwrap();
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Relocked
        );
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), path_id(&lock));
        assert_eq!(std::fs::read_to_string(&lock).unwrap(), owner.render());
    }

    /// D4b: after the unlink a second daemon created and locked a new file.
    /// That is proof another writer owns the directory.
    #[test]
    fn unlink_then_other_holder_gives_lost_to_other() {
        let dir = wab_dir("integrity_lost");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("weir-a");
        let mut slot = held_slot(&dir, &owner);
        let old = fd_id(&slot.as_ref().unwrap().file);

        std::fs::remove_file(&lock).unwrap();
        let other = HolderInfo {
            pid: 4242,
            ..holder("weir-b")
        };
        // A second, process-local open file description takes the new file.
        let _b = try_lock_path(&lock, &other).expect("B acquires the fresh lock file");

        match check_lock_slot(&dir, &mut slot, &owner) {
            LockIntegrity::LostToOther { holder: Some(h) } => {
                assert_eq!(h, other, "the verdict names the NEW owner")
            }
            v => panic!("expected LostToOther naming B, got {v:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(&lock).unwrap(),
            other.render(),
            "the losing daemon must not touch the new owner's record"
        );
        assert_eq!(
            fd_id(&slot.as_ref().unwrap().file),
            old,
            "the slot is not replaced on a lost race"
        );
    }

    /// conf-durability-2: the path was replaced by a file nobody holds, but
    /// which carries ANOTHER daemon's record. That daemon held this wab_dir
    /// (and may have recovered our live segments) and has since exited. It is
    /// proof of lost ownership, not a hand-deleted file: LostToOther, and its
    /// record (the only evidence) is left as it is.
    #[test]
    fn replaced_by_a_foreign_record_nobody_holds_is_lost_to_other() {
        let dir = wab_dir("integrity_foreign_record");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("weir-a");
        let mut slot = held_slot(&dir, &owner);
        let old = fd_id(&slot.as_ref().unwrap().file);

        // B created and locked a fresh file after the unlink, then exited.
        std::fs::remove_file(&lock).unwrap();
        let b = HolderInfo {
            pid: 4242,
            started_unix: owner.started_unix + 5,
            ..holder("weir-b")
        };
        drop(try_lock_path(&lock, &b).expect("B takes the fresh file"));

        match check_lock_slot(&dir, &mut slot, &owner) {
            LockIntegrity::LostToOther { holder: Some(h) } => assert_eq!(h, b),
            v => panic!("expected LostToOther naming B, got {v:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(&lock).unwrap(),
            b.render(),
            "B's record must not be overwritten"
        );
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), old);
    }

    /// A replacement carrying OUR record (a copy of our own file renamed over
    /// it) is no evidence of another daemon: re-lock as for a deleted file.
    #[test]
    fn replaced_by_a_copy_of_our_own_record_relocks() {
        let dir = wab_dir("integrity_own_copy");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let copy = dir.join("copy");
        std::fs::write(&copy, owner.render()).unwrap();
        std::fs::rename(&copy, &lock).unwrap();
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Relocked
        );
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), path_id(&lock));
    }

    /// conf-regressions-12: on a filesystem where `lstat(path)` and `fstat(fd)`
    /// report different ids for the SAME file (overlayfs before Linux 4.19),
    /// the re-create used to flock a second description of our own file, meet
    /// our own lock, and exit 3 naming this daemon's own pid. The newly opened
    /// file is now compared with the held one first.
    #[test]
    fn stat_fstat_disagreement_on_our_own_file_is_ok_not_lost() {
        let dir = wab_dir("integrity_false_mismatch");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let held = fd_id(&slot.as_ref().unwrap().file);
        let fake = (held.0, held.1 ^ 0x5a5a);
        let _lstat = LstatGuard::set(Some(fake));

        for tick in 0..5 {
            assert_eq!(
                check_lock_slot(&dir, &mut slot, &owner),
                LockIntegrity::Ok,
                "tick {tick}"
            );
        }
        let held_lock = slot.as_ref().unwrap();
        assert_eq!(fd_id(&held_lock.file), held, "the held fd is kept");
        assert_eq!(
            held_lock.aliases.len(),
            1,
            "the identity is proven once, not one extra fd per tick"
        );
        assert_eq!(std::fs::read_to_string(&lock).unwrap(), owner.render());
        assert!(
            matches!(
                try_lock_path(&lock, &holder("contender")),
                Err(LockError::Held { .. })
            ),
            "the lock is still exclusive"
        );
    }

    /// The path looked missing, but by the time of the re-create a file is
    /// there: somebody else created it. It is never adopted (O_EXCL); the next
    /// tick inspects it as a replacement.
    #[test]
    fn recreate_never_adopts_a_file_created_after_the_lstat() {
        let dir = wab_dir("integrity_excl");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let old = fd_id(&slot.as_ref().unwrap().file);
        std::fs::remove_file(&lock).unwrap();
        std::fs::write(&lock, b"someone else's").unwrap();
        {
            let _lstat = LstatGuard::set(None);
            assert_eq!(
                check_lock_slot(&dir, &mut slot, &owner),
                LockIntegrity::Error(libc::EEXIST)
            );
        }
        assert_eq!(std::fs::read(&lock).unwrap(), b"someone else's");
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), old);
    }

    /// Where flock is emulated with POSIX locks (NFS), a lease loss lets
    /// another client lock the SAME file; the held-fd re-flock then reports
    /// contention even though the inode still matches.
    #[test]
    fn contention_on_the_held_fd_gives_lost_to_other() {
        let dir = wab_dir("integrity_held_fd_contended");
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let _hook = HookGuard::set(libc::EWOULDBLOCK);
        assert!(
            matches!(
                check_lock_slot(&dir, &mut slot, &owner),
                LockIntegrity::LostToOther { .. }
            ),
            "contention on the held fd is proof of loss"
        );
    }

    #[test]
    fn other_flock_errno_is_an_error_and_keeps_the_held_fd() {
        let dir = wab_dir("integrity_eio");
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let before = fd_id(&slot.as_ref().unwrap().file);
        {
            let _hook = HookGuard::set(libc::EIO);
            assert_eq!(
                check_lock_slot(&dir, &mut slot, &owner),
                LockIntegrity::Error(libc::EIO)
            );
        }
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), before);
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Ok,
            "the next tick retries and recovers"
        );
    }

    /// A symlink planted where the lock file was is not "nobody holds it":
    /// the re-create refuses to follow it (ELOOP), which is an error to retry,
    /// and the target is never written.
    #[test]
    fn symlink_planted_at_the_lock_path_is_an_error_not_a_relock() {
        let dir = wab_dir("integrity_symlink");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let target = dir.join("planted_target");
        let _ = std::fs::remove_file(&target);
        std::fs::write(&target, b"").unwrap();
        std::fs::remove_file(&lock).unwrap();
        std::os::unix::fs::symlink(&target, &lock).unwrap();

        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Error(libc::ELOOP)
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"");
        std::fs::remove_file(&lock).unwrap();
    }

    #[test]
    fn check_without_a_held_lock_is_an_error() {
        let dir = wab_dir("integrity_unheld");
        let mut slot = None;
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &holder("h")),
            LockIntegrity::Error(libc::EBADF)
        );
        assert!(
            slot.is_none(),
            "the check never takes a lock it did not hold"
        );
    }

    /// The module's first rule: the lock path is opened in exactly one place
    /// (`open_lock_path`), which has exactly two callers, acquisition and the
    /// integrity check's re-create after a proven unlink/replace. Where flock
    /// is emulated with POSIX locks, closing ANY other fd to the file drops the
    /// lock, so no other code here may open it.
    #[test]
    fn lock_module_never_reopens_path() {
        let src = include_str!("lock.rs");
        let prod = src
            .split("#[cfg(all(test, unix))]\nmod tests")
            .next()
            .unwrap();
        assert!(prod.len() < src.len(), "the tests marker must be found");
        assert_eq!(
            prod.matches("OpenOptions::new(").count(),
            1,
            "exactly one opener of the lock path (open_lock_path)"
        );
        assert_eq!(
            prod.matches("open_lock_path(").count() - prod.matches("fn open_lock_path(").count(),
            2,
            "open_lock_path is called only by try_lock_path (acquisition) and recreate_lock"
        );
        let callers =
            prod.matches("try_lock_path(").count() - prod.matches("fn try_lock_path(").count();
        assert_eq!(callers, 1, "try_lock_path is called only by acquisition");
        for forbidden in [
            "File::open(",
            "File::create(",
            "fs::read(",
            "fs::write(",
            "remove_file(",
        ] {
            assert!(
                !prod.contains(forbidden),
                "lock.rs must not {forbidden} anything outside its tests"
            );
        }
    }

    // ── Lock-file hardening (conf-security-5, cont-security-2, conf-security-6) ──

    /// conf-security-5: a hard link planted at the lock path would have the
    /// daemon truncate and overwrite the file it shares an inode with (the
    /// config, a TLS key). Refused before flock, and nothing is written.
    #[test]
    fn hardlinked_lock_file_is_refused_and_the_victim_is_untouched() {
        let dir = wab_dir("hardlink_acquire");
        let lock = dir.join(LOCK_FILE_NAME);
        let victim = dir.join("victim.toml");
        let _ = std::fs::remove_file(&victim);
        let original = b"[server]\nsecret = \"do not truncate me\"\n".to_vec();
        std::fs::write(&victim, &original).unwrap();
        std::fs::hard_link(&victim, &lock).unwrap();

        for policy in [LockPolicy::Required, LockPolicy::BestEffort] {
            let err = acquire_wab_dir_lock(&dir, policy, &holder("h"))
                .expect_err("a hard-linked lock file must be refused");
            assert!(
                matches!(
                    err,
                    LockError::Unsafe {
                        problem: UnsafeLockFile::HardLinked { links: 2 },
                        ..
                    }
                ),
                "{err}"
            );
            let msg = err.to_string();
            for needle in [
                "hard links",
                "nothing was written",
                &lock.display().to_string(),
            ] {
                assert!(msg.contains(needle), "{needle:?} missing from: {msg}");
            }
        }
        assert_eq!(
            std::fs::read(&victim).unwrap(),
            original,
            "the hard-link target must be byte-identical"
        );
        std::fs::remove_file(&lock).unwrap();
        std::fs::remove_file(&victim).unwrap();
    }

    /// The same at runtime: the held file is deleted and a hard link to a
    /// victim planted in its place. The re-create refuses it (an error, retried
    /// next tick), keeps the held fd, and never writes the victim.
    #[test]
    fn hardlink_planted_at_runtime_is_an_error_and_the_victim_is_untouched() {
        let dir = wab_dir("hardlink_integrity");
        let lock = dir.join(LOCK_FILE_NAME);
        let owner = holder("owner");
        let mut slot = held_slot(&dir, &owner);
        let before = fd_id(&slot.as_ref().unwrap().file);
        let victim = dir.join("victim.key");
        let _ = std::fs::remove_file(&victim);
        std::fs::write(&victim, b"-----BEGIN PRIVATE KEY-----\n").unwrap();

        std::fs::remove_file(&lock).unwrap();
        std::fs::hard_link(&victim, &lock).unwrap();
        assert_eq!(
            check_lock_slot(&dir, &mut slot, &owner),
            LockIntegrity::Error(libc::EMLINK)
        );
        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"-----BEGIN PRIVATE KEY-----\n"
        );
        assert_eq!(fd_id(&slot.as_ref().unwrap().file), before);
        std::fs::remove_file(&lock).unwrap();
        std::fs::remove_file(&victim).unwrap();
    }

    /// cont-security-2: a lock file of ours with loose bits (restored from a
    /// backup, baked into an image at 0644) is tightened to 0600 once held, so
    /// another user who can read it cannot hold it across a restart.
    #[test]
    fn loose_mode_lock_file_is_tightened_to_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = wab_dir("loose_mode");
        let lock = dir.join(LOCK_FILE_NAME);
        std::fs::write(&lock, b"").unwrap();
        std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644)).unwrap();
        let _held = try_lock_path(&lock, &holder("h")).expect("our own file is adopted");
        assert_eq!(
            std::fs::metadata(&lock).unwrap().permissions().mode() & 0o7777,
            0o600
        );
    }

    /// cont-security-2 and the conf-regressions-11 upgrade case: a lock file
    /// another uid owns is refused, naming the owner and the exact fix. The
    /// file is ours here; the euid hook makes this process look like another
    /// user. Opened fine (a root daemon, or a loose mode): refused at fstat.
    #[test]
    fn foreign_owned_lock_file_is_refused_naming_the_owner_and_the_fix() {
        use std::os::unix::fs::MetadataExt;
        let dir = wab_dir("foreign_owner");
        let lock = dir.join(LOCK_FILE_NAME);
        std::fs::write(&lock, b"pid=1\n").unwrap();
        let real = std::fs::metadata(&lock).unwrap().uid();
        let fake = real + 1;
        let _euid = EuidGuard::set(fake);

        let err = acquire_wab_dir_lock(&dir, LockPolicy::BestEffort, &holder("h"))
            .expect_err("a lock file another uid owns must be refused");
        match &err {
            LockError::Unsafe {
                problem: UnsafeLockFile::ForeignOwner { owner, euid },
                ..
            } => assert_eq!((*owner, *euid), (real, fake)),
            other => panic!("expected ForeignOwner, got {other}"),
        }
        let msg = err.to_string();
        for needle in [
            &format!("owned by uid {real}"),
            &format!("this daemon's uid {fake}"),
            &format!("chown {fake} {}", lock.display()),
            &format!("run weir-server as uid {real}"),
            "If no weir-server is running on this wab_dir",
        ] {
            assert!(msg.contains(needle), "{needle:?} missing from: {msg}");
        }
        assert_eq!(std::fs::read(&lock).unwrap(), b"pid=1\n", "nothing written");
    }

    /// The upgrade case proper (conf-regressions-11 b): a root-owned 0600 lock
    /// file left by a `sudo weir-server` run. A non-root daemon cannot even open
    /// it (EACCES); the refusal must still name the owner and the fix rather
    /// than a bare "Permission denied". Mode 0000 stands in for "not ours".
    #[test]
    fn unopenable_foreign_lock_file_names_the_owner_and_the_fix() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return; // root opens a 0000 file; the EACCES path is unreachable
        }
        let dir = wab_dir("foreign_eacces");
        let lock = dir.join(LOCK_FILE_NAME);
        std::fs::write(&lock, b"").unwrap();
        std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o000)).unwrap();
        let real = std::fs::metadata(&lock).unwrap().uid();
        let fake = real + 1;
        let err = {
            let _euid = EuidGuard::set(fake);
            acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h")).unwrap_err()
        };
        assert!(
            matches!(
                err,
                LockError::Unsafe {
                    problem: UnsafeLockFile::ForeignOwner { .. },
                    ..
                }
            ),
            "{err}"
        );
        assert!(
            err.to_string()
                .contains(&format!("chown {fake} {}", lock.display())),
            "{err}"
        );
        // Our own unopenable file stays a plain Io error (nothing to chown).
        let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h")).unwrap_err();
        match &err {
            LockError::Io { source, .. } => {
                assert_eq!(source.raw_os_error(), Some(libc::EACCES), "{err}")
            }
            other => panic!("expected Io(EACCES), got {other}"),
        }
        std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    /// conf-security-6: the record is untrusted file content. Control
    /// characters (ANSI/OSC escapes, newlines) are stripped, and an oversized
    /// field is dropped rather than echoed.
    #[test]
    fn hostile_holder_record_is_sanitised() {
        let huge = "A".repeat(1 << 20);
        let text = format!(
            "pid=7\nhost=\x1b]0;pwned\x07\x1b[2Jdb-primary\nversion=1.0\r\n\
             metrics=\u{202e}1.0.0.721\nsocket_path={huge}\nboot_id=past-the-cap\n"
        );
        let h = HolderInfo::parse(&text).expect("pid parses");
        assert_eq!(h.pid, 7);
        assert_eq!(h.host, "]0;pwned[2Jdb-primary");
        assert_eq!(h.version, "1.0");
        assert_eq!(h.socket_path, "", "an oversized field is dropped");
        assert_eq!(h.metrics, "1.0.0.721", "bidi overrides are stripped");
        assert_eq!(h.boot_id, None, "nothing past the 4 KiB record cap is read");
        assert_eq!(
            HolderInfo::parse("pid=\x1b[31m7\n"),
            None,
            "a bad pid is no record"
        );
    }

    /// The same, end to end: a contender refused by a holder whose record is
    /// hostile prints a bounded message with no control characters.
    #[test]
    fn refusal_message_never_echoes_a_hostile_record() {
        use std::os::fd::AsRawFd;
        let dir = wab_dir("hostile_refusal");
        let lock = dir.join(LOCK_FILE_NAME);
        let huge = "B".repeat(1 << 20);
        std::fs::write(
            &lock,
            format!("pid=1\nhost=evil\x1b[2J\nline\nsocket_path=/s\x07\nversion={huge}\n"),
        )
        .unwrap();
        // Hold it without writing a record, as a hostile holder would.
        let f = std::fs::OpenOptions::new().read(true).open(&lock).unwrap();
        // SAFETY: flock on an fd owned by the live `f`.
        assert_eq!(
            unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );

        let err = acquire_wab_dir_lock(&dir, LockPolicy::Required, &holder("h")).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                LockError::Held {
                    holder: Some(_),
                    ..
                }
            ),
            "{msg}"
        );
        assert!(
            !msg.chars().any(char::is_control),
            "control chars in: {msg:?}"
        );
        assert!(
            msg.len() < 2048,
            "the message must be bounded, got {} bytes",
            msg.len()
        );
        assert!(msg.contains("pid 1 on host evil[2J,"), "{msg}");
        assert!(msg.contains("socket /s)"), "{msg}");
        drop(f);
    }

    #[test]
    fn format_utc_matches_known_instants() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc(1_759_572_000), "2025-10-04T10:00:00Z");
        assert_eq!(format_utc(951_782_400), "2000-02-29T00:00:00Z");
    }
}
