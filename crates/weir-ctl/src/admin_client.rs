//! Client for the daemon's admin socket (spec §11), with the client-side
//! identity checks: weir-ctl never trusts a payload from a socket it has not
//! authenticated.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

/// The packaged `wab_dir` default (weir-server config/mod.rs).
pub(crate) const DEFAULT_WAB_DIR: &str = "/var/lib/weir/wab";
/// `default_socket_path(DEFAULT_WAB_DIR)`.
pub(crate) const DEFAULT_ADMIN_SOCKET: &str = "/var/lib/weir/wab.admin.sock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminPathOrigin {
    /// `--admin-socket` or `WEIR_ADMIN_SOCKET_PATH` (clap merges the two).
    Explicit,
    /// Derived from `--wab-dir` / `WEIR_WAB_DIR`.
    DerivedFromWabDir,
    /// Nothing given: the packaged default.
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdminTarget {
    pub socket: PathBuf,
    pub origin: AdminPathOrigin,
    /// The wab_dir whose (dev, ino) the daemon's `instance` must match.
    pub wab_dir: Option<PathBuf>,
    /// The operator named it: a wab_dir that cannot be stat'ed then fails the
    /// identity check instead of skipping it.
    pub wab_dir_strict: bool,
}

/// The admin socket path chain of spec §14: an explicit socket, else the path
/// derived from a named wab_dir, else the packaged default. A wab_dir the
/// operator named is strict; the packaged default is compared only when it
/// exists. An explicit socket with no wab_dir has nothing to compare: a
/// best-effort default compare would wrongly flag a daemon with a custom
/// wab_dir.
pub(crate) fn resolve_admin_target(
    admin_socket: Option<&Path>,
    wab_dir: Option<&Path>,
) -> AdminTarget {
    // Canonicalized like the daemon's wab_dir (config/mod.rs), so a symlinked
    // --wab-dir derives the same socket path the daemon bound. A wab_dir that
    // does not exist falls back to the lexical absolute path.
    let named = wab_dir.map(|d| {
        std::fs::canonicalize(d)
            .or_else(|_| std::path::absolute(d))
            .unwrap_or_else(|_| d.to_path_buf())
    });
    if let Some(sock) = admin_socket {
        return AdminTarget {
            socket: sock.to_path_buf(),
            origin: AdminPathOrigin::Explicit,
            wab_dir_strict: named.is_some(),
            wab_dir: named,
        };
    }
    match named {
        Some(d) => AdminTarget {
            socket: weir_status::admin::default_socket_path(&d)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_ADMIN_SOCKET)),
            origin: AdminPathOrigin::DerivedFromWabDir,
            wab_dir: Some(d),
            wab_dir_strict: true,
        },
        None => AdminTarget {
            socket: PathBuf::from(DEFAULT_ADMIN_SOCKET),
            origin: AdminPathOrigin::Default,
            wab_dir: Some(PathBuf::from(DEFAULT_WAB_DIR)),
            wab_dir_strict: false,
        },
    }
}

/// `lstat` facts of the socket and `stat` of its parent (spec §11): a socket,
/// mode 0600, in a directory nobody but its owner can write to.
pub(crate) fn check_socket_modes(
    is_socket: bool,
    mode: u32,
    parent_mode: u32,
) -> Result<(), String> {
    if !is_socket {
        return Err("not a socket".to_string());
    }
    if mode & 0o777 != 0o600 {
        return Err(format!("mode {:o}, expected 600", mode & 0o777));
    }
    if parent_mode & 0o022 != 0 {
        return Err(format!(
            "its parent directory is group- or world-writable (mode {:o}), so anyone could \
             have put this socket there",
            parent_mode & 0o7777
        ));
    }
    Ok(())
}

/// The daemon runs as weir-ctl's euid, as the wab_dir's owner, or as root.
pub(crate) fn peer_uid_allowed(peer: u32, euid: u32, wab_owner: Option<u32>) -> bool {
    peer == euid || peer == 0 || wab_owner == Some(peer)
}

/// Why a peer uid was refused (exit 2). With no wab_dir owner to accept (only
/// --admin-socket given, or the packaged default wab_dir absent), a daemon
/// running as its own service user is refused until the operator names the
/// wab_dir, so the message says so.
pub(crate) fn peer_uid_refusal(
    sock: &Path,
    peer: u32,
    euid: u32,
    wab_owner: Option<u32>,
) -> String {
    format!(
        "{} is served by uid {peer}, which is not weir-ctl's uid {euid}, the wab_dir owner, or \
         root{}",
        sock.display(),
        if wab_owner.is_none() {
            " (no wab_dir was named: pass --wab-dir so its owner can be accepted)"
        } else {
            ""
        }
    )
}

/// The reply's `instance` must name the same wab_dir inode the operator meant.
/// `expected` is `None` when there is no wab_dir to compare.
pub(crate) fn check_instance(instance: &Value, expected: Option<(u64, u64)>) -> Result<(), String> {
    let Some((dev, ino)) = expected else {
        return Ok(());
    };
    let got_dev = instance.get("wab_dir_dev").and_then(Value::as_u64);
    let got_ino = instance.get("wab_dir_ino").and_then(Value::as_u64);
    match (got_dev, got_ino) {
        (Some(d), Some(i)) if d == dev && i == ino => Ok(()),
        (Some(d), Some(i)) => Err(format!(
            "the daemon serves wab_dir (dev {d}, ino {i}), but --wab-dir is (dev {dev}, ino {ino})"
        )),
        _ => Err("the reply carries no wab_dir identity".to_string()),
    }
}

/// Bounded so a wedged daemon runtime cannot hang the CLI (a blocking
/// AF_UNIX connect to a full backlog hangs on Linux).
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// An admin status document carries the full ConfigView and up to 256 shards;
/// 4 MiB is far above that and far below anything that would hurt.
pub(crate) const MAX_REPLY_BYTES: u64 = 4 * 1024 * 1024;
#[cfg(unix)]
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) enum CallError {
    /// Nothing usable at the path: absent, refused, permission denied, timed out.
    Unreachable(String),
    /// The socket or the daemon failed a client-side identity check (exit 2).
    Identity(String),
    /// The daemon answered `ok: false`.
    Remote {
        code: String,
        message: String,
        retryable: bool,
    },
    /// The reply could not be parsed; it is never trusted.
    Protocol(String),
}

#[derive(Debug)]
pub(crate) struct AdminReply {
    pub instance: Value,
    pub result: Value,
}

/// `daemon pid 4242, wab_dir /var/lib/weir/wab, socket /run/weir/weir.sock`.
pub(crate) fn instance_line(instance: &Value) -> String {
    let s = |k: &str| {
        instance.get(k).map_or_else(
            || "?".to_string(),
            |v| v.as_str().map_or_else(|| v.to_string(), str::to_string),
        )
    };
    format!(
        "daemon pid {}, wab_dir {}, socket {}",
        s("pid"),
        s("wab_dir"),
        s("socket_path")
    )
}

#[cfg(not(unix))]
pub(crate) fn call(
    _t: &AdminTarget,
    _verb: &str,
    _args: Value,
    _d: Duration,
) -> Result<AdminReply, CallError> {
    Err(CallError::Unreachable(
        "the admin socket is Unix-only".to_string(),
    ))
}

/// One request and one reply on the admin socket, after the client-side
/// identity checks of spec §11: lstat (a 0600 socket in a private parent),
/// the wab_dir to compare, a bounded connect, the peer uid, then one JSON
/// line each way within `reply_timeout`, and the reply's `instance`.
#[cfg(unix)]
pub(crate) fn call(
    target: &AdminTarget,
    verb: &str,
    args: Value,
    reply_timeout: Duration,
) -> Result<AdminReply, CallError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let sock = &target.socket;
    // 1. lstat: absent is "not running here", anything else must be our socket.
    let meta = match std::fs::symlink_metadata(sock) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(CallError::Unreachable(format!(
                "no admin socket at {}",
                sock.display()
            )));
        }
        Err(e) => {
            return Err(CallError::Unreachable(format!(
                "stat {}: {e}",
                sock.display()
            )));
        }
    };
    let parent = sock
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent_mode = std::fs::metadata(parent)
        .map_err(|e| CallError::Identity(format!("stat {}: {e}", parent.display())))?
        .mode();
    check_socket_modes(meta.file_type().is_socket(), meta.mode(), parent_mode)
        .map_err(|why| CallError::Identity(format!("{}: {why}", sock.display())))?;

    // 2. What the daemon must prove: the wab_dir inode, and who may serve it.
    let (expected, wab_owner) = match &target.wab_dir {
        None => (None, None),
        Some(d) => match std::fs::metadata(d) {
            Ok(m) => (Some((m.dev(), m.ino())), Some(m.uid())),
            Err(e) if target.wab_dir_strict => {
                return Err(CallError::Identity(format!(
                    "cannot stat --wab-dir {} to check the daemon's identity: {e}",
                    d.display()
                )));
            }
            Err(_) => (None, None),
        },
    };

    // 3. Bounded connect, 4. peer uid.
    let stream = connect_with_timeout(sock, CONNECT_TIMEOUT)?;
    let peer = peer_uid(&stream)
        .map_err(|e| CallError::Identity(format!("peer credentials unavailable: {e}")))?;
    // SAFETY: geteuid has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    if !peer_uid_allowed(peer, euid, wab_owner) {
        return Err(CallError::Identity(peer_uid_refusal(
            sock, peer, euid, wab_owner,
        )));
    }

    exchange(&stream, verb, args, reply_timeout, expected)
}

/// Steps 5 and 6 of [`call`], on a stream whose peer is authenticated: one
/// request line out, one bounded reply line back within `reply_timeout`.
#[cfg(unix)]
fn exchange(
    stream: &std::os::unix::net::UnixStream,
    verb: &str,
    args: Value,
    reply_timeout: Duration,
    expected: Option<(u64, u64)>,
) -> Result<AdminReply, CallError> {
    use crate::deadline::{DeadlineReader, is_timeout};
    use std::io::{BufRead, BufReader, Read, Write};

    // 5. One request line.
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(|e| CallError::Unreachable(format!("set timeout: {e}")))?;
    let req = serde_json::json!({"v": 1, "id": 1, "verb": verb, "args": args});
    let mut line = req.to_string();
    line.push('\n');
    // The daemon answers `busy` (and `forbidden`) at accept, then closes
    // without reading: a send that fails because the peer hung up still
    // reads that line, so the refusal is reported instead of a broken pipe.
    let sent = (&*stream).write_all(line.as_bytes());
    if let Err(e) = &sent
        && !matches!(
            e.kind(),
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
        )
    {
        return Err(CallError::Unreachable(format!("send {verb}: {e}")));
    }

    // 6. One bounded reply line, within one deadline for the whole reply.
    let deadline = DeadlineReader::new(stream, reply_timeout);
    let mut reader = BufReader::new(deadline.take(MAX_REPLY_BYTES + 1));
    let mut reply = String::new();
    let n = reader.read_line(&mut reply).map_err(|e| {
        if e.kind() == std::io::ErrorKind::InvalidData {
            CallError::Protocol(format!("the {verb} reply is not UTF-8"))
        } else if is_timeout(&e) {
            CallError::Unreachable(format!(
                "no reply to {verb} within {:.1}s",
                reply_timeout.as_secs_f64()
            ))
        } else {
            CallError::Unreachable(format!("read the {verb} reply: {e}"))
        }
    })?;
    if n == 0 {
        return Err(CallError::Unreachable(match sent {
            Err(e) => format!("send {verb}: {e}"),
            Ok(()) => format!("the daemon closed the admin connection without answering {verb}"),
        }));
    }
    if !reply.ends_with('\n') {
        // Never parsed: a cut-off line may still be valid JSON.
        return Err(CallError::Protocol(if n as u64 > MAX_REPLY_BYTES {
            format!("the {verb} reply is not one line under {MAX_REPLY_BYTES} bytes")
        } else {
            format!("the {verb} reply ended before its newline")
        }));
    }
    parse_reply(&reply, expected)
}

/// Parses one reply line. An `ok` reply must carry an `instance` that matches
/// `expected`; an `ok: false` reply without one (`forbidden`, `busy`, sent
/// before identification) reports only its code.
pub(crate) fn parse_reply(
    line: &str,
    expected: Option<(u64, u64)>,
) -> Result<AdminReply, CallError> {
    let v: Value = serde_json::from_str(line.trim_end())
        .map_err(|e| CallError::Protocol(format!("the reply is not JSON: {e}")))?;
    if v.get("v").and_then(Value::as_u64) != Some(1) {
        return Err(CallError::Protocol(
            "the reply is not admin protocol v1".to_string(),
        ));
    }
    let ok = v
        .get("ok")
        .and_then(Value::as_bool)
        .ok_or_else(|| CallError::Protocol("the reply has no ok flag".to_string()))?;
    let remote = |v: &Value| {
        let e = v.get("error").cloned().unwrap_or(Value::Null);
        CallError::Remote {
            code: e
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            message: e
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            retryable: e.get("retryable").and_then(Value::as_bool).unwrap_or(false),
        }
    };
    match v.get("instance") {
        Some(inst) if inst.is_object() => {
            check_instance(inst, expected).map_err(CallError::Identity)?;
            if ok {
                Ok(AdminReply {
                    instance: inst.clone(),
                    result: v.get("result").cloned().unwrap_or(Value::Null),
                })
            } else {
                Err(remote(&v))
            }
        }
        // `forbidden` / `busy` can precede identification: report the code only.
        _ if !ok => Err(remote(&v)),
        _ => Err(CallError::Protocol(
            "the reply carries no instance".to_string(),
        )),
    }
}

#[cfg(unix)]
fn connect_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<std::os::unix::net::UnixStream, CallError> {
    let (tx, rx) = std::sync::mpsc::channel();
    let p = path.to_path_buf();
    // A thread, because std has no UnixStream::connect_timeout. On timeout the
    // thread is abandoned; the CLI exits soon after.
    std::thread::spawn(move || {
        let _ = tx.send(std::os::unix::net::UnixStream::connect(&p));
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(CallError::Unreachable(match e.kind() {
            // On macOS and the BSDs a live listener with a full accept backlog
            // also refuses (weir-server socket/mod.rs), so only Linux may call
            // a refusal stale. Either way it is "nothing answered".
            std::io::ErrorKind::ConnectionRefused => format!(
                "{}: connection refused ({})",
                path.display(),
                if cfg!(target_os = "linux") {
                    "nothing is listening: a stale socket"
                } else {
                    "nothing is listening, or its accept backlog is full"
                }
            ),
            std::io::ErrorKind::PermissionDenied => format!(
                "{}: permission denied (run weir-ctl as the daemon's user or root)",
                path.display()
            ),
            _ => format!("connect {}: {e}", path.display()),
        })),
        Err(_) => Err(CallError::Unreachable(format!(
            "connect {}: no answer within {}s",
            path.display(),
            timeout.as_secs()
        ))),
    }
}

/// Mirrors weir-server's socket/peer.rs (not reachable from here: pub(crate)
/// in another crate). Fails closed where there are no peer credentials.
#[cfg(target_os = "linux")]
fn peer_uid(s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: ucred is plain old data; all-zero is a valid value.
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: cred/len point at a correctly sized ucred for SO_PEERCRED.
    let ret = unsafe {
        libc::getsockopt(
            s.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(cred.uid)
}

#[cfg(target_os = "macos")]
fn peer_uid(s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    use std::os::unix::io::AsRawFd;
    let (mut uid, mut gid): (libc::uid_t, libc::gid_t) = (0, 0);
    // SAFETY: both out-pointers are valid for the call.
    if unsafe { libc::getpeereid(s.as_raw_fd(), &mut uid, &mut gid) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(uid)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn peer_uid(_s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "peer credentials are not implemented on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn explicit_socket_wins_and_is_not_compared_to_a_default_wab_dir() {
        let t = resolve_admin_target(Some(Path::new("/run/x/a.sock")), None);
        assert_eq!(t.socket, PathBuf::from("/run/x/a.sock"));
        assert_eq!(t.origin, AdminPathOrigin::Explicit);
        assert_eq!(t.wab_dir, None);
        let t = resolve_admin_target(
            Some(Path::new("/run/x/a.sock")),
            Some(Path::new("/data/wab")),
        );
        assert_eq!(t.wab_dir, Some(PathBuf::from("/data/wab")));
        assert!(t.wab_dir_strict);
    }

    #[test]
    fn derived_default_matches_the_daemon_rule() {
        // <parent(wab_dir)>/<basename(wab_dir)>.admin.sock (spec §5.9)
        let t = resolve_admin_target(None, Some(Path::new("/data/weir/wab")));
        assert_eq!(t.socket, PathBuf::from("/data/weir/wab.admin.sock"));
        assert_eq!(t.origin, AdminPathOrigin::DerivedFromWabDir);
        assert!(t.wab_dir_strict);
        let t = resolve_admin_target(None, Some(Path::new("/data/weir/wab/")));
        assert_eq!(
            t.socket,
            PathBuf::from("/data/weir/wab.admin.sock"),
            "a trailing slash is the same dir"
        );
    }

    #[test]
    fn nothing_given_uses_the_packaged_default_best_effort() {
        let t = resolve_admin_target(None, None);
        assert_eq!(t.socket, PathBuf::from(DEFAULT_ADMIN_SOCKET));
        assert_eq!(t.origin, AdminPathOrigin::Default);
        assert_eq!(t.wab_dir, Some(PathBuf::from(DEFAULT_WAB_DIR)));
        assert!(!t.wab_dir_strict);
        assert_eq!(
            weir_status::admin::default_socket_path(Path::new(DEFAULT_WAB_DIR)),
            Some(PathBuf::from(DEFAULT_ADMIN_SOCKET)),
            "the two defaults must agree"
        );
    }

    #[test]
    fn socket_modes_must_be_a_0600_socket_in_a_private_parent() {
        assert!(check_socket_modes(true, 0o140600, 0o040700).is_ok());
        assert!(check_socket_modes(true, 0o140600, 0o040755).is_ok());
        assert!(
            check_socket_modes(false, 0o100600, 0o040700)
                .unwrap_err()
                .contains("not a socket")
        );
        assert!(
            check_socket_modes(true, 0o140660, 0o040700)
                .unwrap_err()
                .contains("600")
        );
        assert!(
            check_socket_modes(true, 0o140600, 0o040770)
                .unwrap_err()
                .contains("writable")
        );
        assert!(
            check_socket_modes(true, 0o140600, 0o041777)
                .unwrap_err()
                .contains("writable"),
            "sticky /tmp is still world-writable"
        );
    }

    #[test]
    fn peer_uid_must_be_us_the_wab_owner_or_root() {
        assert!(peer_uid_allowed(1000, 1000, None));
        assert!(peer_uid_allowed(0, 1000, None), "root");
        assert!(
            peer_uid_allowed(10001, 1000, Some(10001)),
            "the wab_dir owner (operator is root or in sudo)"
        );
        assert!(
            !peer_uid_allowed(4242, 1000, Some(10001)),
            "a stranger is refused: the wrong-uid identity check"
        );
        assert!(!peer_uid_allowed(4242, 1000, None));
    }

    /// With only --admin-socket there is no wab_dir owner to accept, so the
    /// refusal says how to name one.
    #[test]
    fn a_refused_peer_uid_without_a_wab_dir_hints_at_wab_dir() {
        let sock = Path::new("/run/weir/wab.admin.sock");
        let e = peer_uid_refusal(sock, 10001, 1000, None);
        assert_eq!(
            e,
            "/run/weir/wab.admin.sock is served by uid 10001, which is not weir-ctl's uid 1000, \
             the wab_dir owner, or root (no wab_dir was named: pass --wab-dir so its owner can \
             be accepted)"
        );
        let e = peer_uid_refusal(sock, 4242, 1000, Some(10001));
        assert!(
            !e.contains("--wab-dir"),
            "a named wab_dir needs no hint: {e}"
        );
        assert!(e.ends_with("or root"), "{e}");
        let mut err = Vec::new();
        assert_eq!(
            crate::admin_cmds::report_call_error(
                "reload",
                &CallError::Identity(peer_uid_refusal(sock, 10001, 1000, None)),
                false,
                &mut err,
                None,
            ),
            2,
            "still an identity mismatch"
        );
        assert!(String::from_utf8(err).unwrap().contains("pass --wab-dir"));
    }

    #[test]
    fn instance_must_carry_the_wab_dir_dev_and_ino() {
        let inst = json!({"pid": 7, "wab_dir_dev": 64769, "wab_dir_ino": 131074});
        assert!(check_instance(&inst, None).is_ok());
        assert!(check_instance(&inst, Some((64769, 131074))).is_ok());
        let e = check_instance(&inst, Some((64769, 999))).unwrap_err();
        assert!(e.contains("131074") && e.contains("999"), "{e}");
        assert!(
            check_instance(&json!({"pid": 7}), Some((1, 2))).is_err(),
            "no identity is a mismatch"
        );
    }

    #[cfg(unix)]
    mod call_tests {
        use super::super::test_support::*;
        use super::super::*;
        use serde_json::{Value, json};
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::{UnixListener, UnixStream};
        use std::time::{Duration, Instant};

        fn target(dir: &Path, wab: &Path) -> AdminTarget {
            AdminTarget {
                socket: dir.join("a.sock"),
                origin: AdminPathOrigin::Explicit,
                wab_dir: Some(wab.to_path_buf()),
                wab_dir_strict: true,
            }
        }

        /// A 0600 listener whose one connection `serve` drives byte by byte;
        /// write errors are the test's to ignore (the client may hang up).
        fn raw_admin(
            sock: &Path,
            serve: impl FnOnce(UnixStream) + Send + 'static,
        ) -> std::thread::JoinHandle<()> {
            let l = UnixListener::bind(sock).unwrap();
            std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::thread::spawn(move || {
                let (s, _) = l.accept().unwrap();
                serve(s);
            })
        }

        #[test]
        fn ok_reply_returns_the_result_and_the_request_is_a_v1_envelope() {
            let dir = private_dir("ok");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true,
                       "result": {"schema": "weir.status/v1"}})
            });
            let r = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap();
            assert_eq!(r.result["schema"], "weir.status/v1");
            assert_eq!(r.instance["pid"], 4242);
            let req = h.join().unwrap();
            assert_eq!(req["v"], 1);
            assert_eq!(req["verb"], "status");
            assert_eq!(req["args"], json!({}));
            assert!(req.get("id").is_some());
            assert_eq!(
                instance_line(&r.instance),
                "daemon pid 4242, wab_dir ".to_string()
                    + &wab.display().to_string()
                    + ", socket /run/weir/weir.sock"
            );
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_wab_dir_ino_mismatch_is_an_identity_error() {
            let dir = private_dir("ino");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let mut inst = instance_for(&wab);
            inst["wab_dir_ino"] = json!(inst["wab_dir_ino"].as_u64().unwrap() + 1);
            let _h = fake_admin(
                &dir.join("a.sock"),
                move |req| json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true, "result": {}}),
            );
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(matches!(e, CallError::Identity(_)), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_group_writable_parent_is_refused_before_connecting() {
            let dir = private_dir("gw");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let sock = dir.join("a.sock");
            let _l = UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o770)).unwrap();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Identity(ref w) if w.contains("writable")),
                "{e:?}"
            );
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_world_accessible_socket_is_refused() {
            let dir = private_dir("mode");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let sock = dir.join("a.sock");
            let _l = UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o666)).unwrap();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(matches!(e, CallError::Identity(_)), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        /// lstat, not stat: a symlink at the socket path is refused even when
        /// it points at a good 0600 socket.
        #[test]
        fn a_symlink_at_the_socket_path_is_refused() {
            let dir = private_dir("link");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let real = dir.join("real.sock");
            let _l = UnixListener::bind(&real).unwrap();
            std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::os::unix::fs::symlink(&real, dir.join("a.sock")).unwrap();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Identity(ref w) if w.contains("not a socket")),
                "{e:?}"
            );
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn absent_and_stale_sockets_are_unreachable() {
            let dir = private_dir("gone");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(matches!(e, CallError::Unreachable(_)), "absent: {e:?}");
            {
                let _l = UnixListener::bind(dir.join("a.sock")).unwrap();
                std::fs::set_permissions(
                    dir.join("a.sock"),
                    std::fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            } // dropped: the file stays, nobody listens
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(matches!(e, CallError::Unreachable(_)), "stale: {e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_strict_wab_dir_that_does_not_exist_fails_identity() {
            let dir = private_dir("nowab");
            // A real 0600 socket, so the only failing check is the wab_dir stat.
            let sock = dir.join("a.sock");
            let _l = UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            let e = call(
                &target(&dir, &dir.join("missing")),
                "status",
                json!({}),
                Duration::from_secs(1),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Identity(ref w) if w.contains("cannot stat --wab-dir")),
                "{e:?}"
            );
            std::fs::remove_dir_all(&dir).ok();
        }

        /// A listener whose accept backlog is full makes a blocking AF_UNIX
        /// connect hang on Linux; the connect is bounded by CONNECT_TIMEOUT.
        #[cfg(target_os = "linux")]
        #[test]
        fn a_connect_to_a_full_backlog_is_bounded() {
            use std::os::unix::io::AsRawFd;
            let dir = private_dir("full");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let sock = dir.join("a.sock");
            let l = UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            // SAFETY: re-listening on our own listening fd only shrinks its backlog.
            assert_eq!(unsafe { libc::listen(l.as_raw_fd(), 0) }, 0);
            // Backlog 0 admits one pending connection; never accepted.
            let _held = UnixStream::connect(&sock).unwrap();
            let start = Instant::now();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            let took = start.elapsed();
            assert!(
                matches!(e, CallError::Unreachable(ref w) if w.contains("no answer")),
                "{e:?}"
            );
            assert!(
                took >= CONNECT_TIMEOUT && took < CONNECT_TIMEOUT + Duration::from_secs(2),
                "{took:?}"
            );
            drop(l);
            std::fs::remove_dir_all(&dir).ok();
        }

        /// The reply timeout bounds the whole reply, not each read: a peer that
        /// dribbles one byte at a time cannot keep weir-ctl waiting.
        #[test]
        fn a_dribbled_reply_is_bounded_by_the_reply_timeout() {
            let dir = private_dir("drib");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let _h = raw_admin(&dir.join("a.sock"), |mut s| {
                for _ in 0..100 {
                    if s.write_all(b" ").is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
            let start = Instant::now();
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_millis(600),
            )
            .unwrap_err();
            let took = start.elapsed();
            assert!(
                matches!(e, CallError::Unreachable(ref w) if w.contains("no reply")),
                "{e:?}"
            );
            assert!(
                took < Duration::from_secs(2),
                "the deadline is per reply, not per read: {took:?}"
            );
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn an_oversized_or_non_utf8_reply_is_a_protocol_error() {
            let dir = private_dir("big");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let _h = raw_admin(&dir.join("a.sock"), |mut s| {
                let _ = s.write_all(&vec![b'a'; MAX_REPLY_BYTES as usize + 16]);
            });
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Protocol(ref w) if w.contains("one line")),
                "{e:?}"
            );
            std::fs::remove_file(dir.join("a.sock")).ok();

            // A complete JSON object without its newline (the daemon died
            // mid-write) is never parsed. The request is read first: Linux
            // resets a peer that closes with unread data, which would be an
            // Unreachable, not this case.
            let inst = instance_for(&wab);
            let _h = raw_admin(&dir.join("a.sock"), move |mut s| {
                let mut req = String::new();
                let _ = std::io::BufRead::read_line(&mut std::io::BufReader::new(&s), &mut req);
                let line = json!({"v": 1, "id": 1, "instance": inst, "ok": true, "result": {}});
                let _ = s.write_all(line.to_string().as_bytes());
            });
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Protocol(ref w) if w.contains("before its newline")),
                "{e:?}"
            );
            std::fs::remove_file(dir.join("a.sock")).ok();

            let _h = raw_admin(&dir.join("a.sock"), |mut s| {
                let _ = s.write_all(b"{\"v\":1,\"x\":\"\xff\xfe\"}\n");
            });
            let e = call(
                &target(&dir, &wab),
                "status",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(matches!(e, CallError::Protocol(_)), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        fn busy_line(inst: &Value) -> String {
            let line = json!({"v": 1, "id": null, "instance": inst, "ok": false,
                "error": {"code": "busy", "message": "at most 4", "retryable": true}});
            format!("{line}\n")
        }

        /// The daemon writes `busy` at accept and closes without reading the
        /// request. When it has closed before weir-ctl sends, the send fails
        /// with a broken pipe, and the refusal already queued is still what
        /// weir-ctl reports (deterministic: the peer is gone before the send).
        #[test]
        fn a_refusal_queued_before_a_failed_send_is_still_reported() {
            let (a, mut b) = UnixStream::pair().unwrap();
            b.write_all(
                busy_line(&json!({"pid": 1, "wab_dir_dev": 1, "wab_dir_ino": 2})).as_bytes(),
            )
            .unwrap();
            drop(b);
            let e = exchange(
                &a,
                "drain.pause",
                json!({}),
                Duration::from_secs(5),
                Some((1, 2)),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Remote { ref code, retryable: true, .. } if code == "busy"),
                "{e:?}"
            );
            // With nothing queued, the broken pipe is what is reported.
            let (a, b) = UnixStream::pair().unwrap();
            drop(b);
            let e = exchange(&a, "status", json!({}), Duration::from_secs(5), None).unwrap_err();
            assert!(
                matches!(e, CallError::Unreachable(ref w) if w.contains("send status")),
                "{e:?}"
            );
        }

        /// The same refusal through the whole of `call`, whichever side of
        /// the race the send lands on.
        #[test]
        fn a_busy_daemon_is_reported_as_busy() {
            let dir = private_dir("busy");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let line = busy_line(&instance_for(&wab));
            let h = raw_admin(&dir.join("a.sock"), move |mut s| {
                let _ = s.write_all(line.as_bytes());
                let _ = s.shutdown(std::net::Shutdown::Both);
            });
            let e = call(
                &target(&dir, &wab),
                "drain.pause",
                json!({}),
                Duration::from_secs(5),
            )
            .unwrap_err();
            assert!(
                matches!(e, CallError::Remote { ref code, retryable: true, .. } if code == "busy"),
                "{e:?}"
            );
            h.join().unwrap();
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn parse_reply_maps_errors_and_never_trusts_an_unidentified_success() {
            let inst = json!({"pid": 1, "wab_dir_dev": 1, "wab_dir_ino": 2});
            let err = format!(
                "{}",
                json!({"v":1,"id":1,"instance":inst,"ok":false,
                       "error":{"code":"shutting_down","message":"daemon is shutting down","retryable":false}})
            );
            match parse_reply(&err, Some((1, 2))).unwrap_err() {
                CallError::Remote {
                    code,
                    message,
                    retryable,
                } => {
                    assert_eq!(code, "shutting_down");
                    assert_eq!(message, "daemon is shutting down");
                    assert!(!retryable);
                }
                other => panic!("{other:?}"),
            }
            // An error from a daemon that serves another wab_dir is an identity error.
            assert!(matches!(
                parse_reply(&err, Some((1, 3))),
                Err(CallError::Identity(_))
            ));
            // forbidden before identification: no instance, the code is still reported.
            let forbidden = r#"{"v":1,"id":null,"ok":false,"error":{"code":"forbidden","message":"peer uid","retryable":false}}"#;
            assert!(matches!(
                parse_reply(forbidden, None),
                Err(CallError::Remote { .. })
            ));
            // ok without instance: refused.
            let bare = r#"{"v":1,"id":1,"ok":true,"result":{}}"#;
            assert!(matches!(
                parse_reply(bare, None),
                Err(CallError::Protocol(_))
            ));
            assert!(matches!(
                parse_reply("not json", None),
                Err(CallError::Protocol(_))
            ));
            assert!(matches!(
                parse_reply(r#"{"v":2,"ok":true}"#, None),
                Err(CallError::Protocol(_))
            ));
        }
    }
}

#[cfg(all(test, unix))]
pub(crate) mod test_support {
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};

    /// A 0700 directory: the admin parent rule refuses /tmp itself.
    /// Kept short: macOS sun_path is 104 bytes.
    pub(crate) fn private_dir(label: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("wctl_{label}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
        d
    }

    pub(crate) fn instance_for(wab: &Path) -> Value {
        let m = std::fs::metadata(wab).unwrap();
        json!({"pid": 4242, "started_at_ms": 1, "wab_dir": wab.display().to_string(),
               "socket_path": "/run/weir/weir.sock", "wab_dir_dev": m.dev(), "wab_dir_ino": m.ino()})
    }

    /// Binds a 0600 admin socket, answers ONE request with `reply(request)`,
    /// and returns the request it read.
    pub(crate) fn fake_admin(
        sock: &Path,
        reply: impl Fn(&Value) -> Value + Send + 'static,
    ) -> std::thread::JoinHandle<Value> {
        let l = UnixListener::bind(sock).unwrap();
        std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut line = String::new();
            BufReader::new(&s).read_line(&mut line).unwrap();
            let req: Value = serde_json::from_str(&line).unwrap();
            let mut out = serde_json::to_string(&reply(&req)).unwrap();
            out.push('\n');
            (&s).write_all(out.as_bytes()).unwrap();
            req
        })
    }
}
