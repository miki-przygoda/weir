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
static WAB_DIR_LOCK: Mutex<Option<File>> = Mutex::new(None);

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
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut info = HolderInfo::default();
        let mut pid = None;
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k {
                "pid" => pid = v.parse().ok(),
                "host" => info.host = v.to_string(),
                "started_unix" => info.started_unix = v.parse().unwrap_or(0),
                "boot_id" => info.boot_id = Some(v.to_string()),
                "version" => info.version = v.to_string(),
                "socket_path" => info.socket_path = v.to_string(),
                "metrics" => info.metrics = v.to_string(),
                _ => {}
            }
        }
        info.pid = pid?;
        Some(info)
    }
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
            *WAB_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner()) = Some(file);
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

/// The held lock's fd, for the integrity check. `None` before acquisition and
/// under `LockOutcome::Unsupported`.
#[cfg(unix)]
#[cfg_attr(not(test), allow(dead_code))] // consumed by the lock-integrity check
pub(crate) fn held_lock_fd() -> Option<std::os::fd::RawFd> {
    use std::os::fd::AsRawFd;
    WAB_DIR_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map(|f| f.as_raw_fd())
}

/// Opens, checks and locks one lock path, returning the open `File` that holds
/// the lock. Bypasses [`WAB_DIR_LOCK`], so tests can take and drop locks
/// without poisoning the process-wide slot.
///
/// This and the integrity check's re-create are the only places that open the
/// lock path.
#[cfg(unix)]
#[allow(clippy::result_large_err)] // see acquire_wab_dir_lock
fn try_lock_path(path: &Path, holder: &HolderInfo) -> Result<File, LockError> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};

    let io_err = |source: std::io::Error| LockError::Io {
        source,
        path: path.to_path_buf(),
    };

    // O_NOFOLLOW: a symlink at the lock path fails with ELOOP rather than
    // locking (and writing into) whatever it points at. std adds O_CLOEXEC.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(io_err)?;

    // A directory fails the open above (EISDIR); a FIFO or a device opens fine
    // and must not be locked or written as if it were the record.
    let meta = file.metadata().map_err(io_err)?;
    if !meta.file_type().is_file() {
        return Err(io_err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the lock path exists and is not a regular file",
        )));
    }
    let mode = meta.permissions().mode() & 0o7777;
    if mode != 0o600 {
        // Mirrors audit_segment_modes: loose bits are worth a WARN, not a
        // refusal. The record holds nothing secret.
        warn!(
            lock = %path.display(),
            mode = format_args!("{mode:o}"),
            "wab_dir lock file is not mode 0600"
        );
    }

    if let Err(errno) = flock_exclusive_nonblocking(file.as_raw_fd()) {
        return Err(classify_flock_errno(errno, &file, path));
    }

    // Held. Only now replace the record, so a refused contender never touches
    // the owner's. Informational, so no fsync, and a failure to write it does
    // not give the lock back.
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
    Ok(file)
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
    let mut buf = vec![0u8; 4096];
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
            | LockError::Io { path, .. } => path,
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

    #[test]
    fn format_utc_matches_known_instants() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc(1_759_572_000), "2025-10-04T10:00:00Z");
        assert_eq!(format_utc(951_782_400), "2000-02-29T00:00:00Z");
    }
}
