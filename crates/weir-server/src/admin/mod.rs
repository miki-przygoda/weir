//! The admin Unix socket (Foundations spec §5.9, §11): a JSON-lines control
//! channel for the daemon's own uid and root, separate from the producer
//! socket and never on the producer-shared directory.

pub(crate) mod framing;
pub(crate) mod identity;
pub(crate) mod verbs;

use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    sync::{Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::{error, info, warn};
use weir_status::{
    AdminReason, LivezCheck, Thresholds,
    admin::{AdminError, ErrorCode, Response},
};

use crate::{
    config::{AdminSocketSetting, Config},
    drain::control::DrainControl,
    lifecycle::ShutdownLevel,
    metrics::{AdminOutcomeLabel, AdminRequestLabel, AdminVerbLabel, Metrics},
    reload::ReloadRequest,
    socket::{self, BoundSocket, ParentPolicy},
    status::StatusBoard,
};
use framing::{Dispatch, Limits};
use verbs::AdminState;

/// Why the admin socket could not be bound. Codes only, from code: they go
/// into log fields and error messages. The status board carries PR 1's closed
/// `weir_status::AdminReason` instead, mapped from these codes (plus the
/// config layer's `no_parent`, `path_too_long`, `under_wab_dir`) by
/// [`admin_reason`].
pub(crate) mod reason {
    pub(crate) const PARENT_INSECURE: &str = "parent_insecure";
    pub(crate) const IN_USE: &str = "in_use";
    pub(crate) const BIND_FAILED: &str = "bind_failed";
    pub(crate) const COLLIDES_WITH_SOCKET_PATH: &str = "collides_with_socket_path";
}

/// A bound admin listener and the path it was bound at.
pub(crate) struct BoundAdmin {
    pub(crate) path: PathBuf,
    pub(crate) socket: BoundSocket,
}

/// A refused or failed admin bind, with a code from [`reason`].
#[derive(Debug)]
pub(crate) struct AdminBindError {
    pub(crate) reason_code: &'static str,
    pub(crate) source: io::Error,
}

impl fmt::Display for AdminBindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.source, self.reason_code)
    }
}

impl std::error::Error for AdminBindError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Binds the admin socket (spec §11 "Bind"): format check, the (dev, ino)
/// collision check against `socket_path`, PR 3's `refuse_if_live` (a live
/// socket is refused with the admin socket's own wording, spec §5.7/§5.9),
/// then `bind_hardened_with` under `RequirePrivate`, whose stale-socket step
/// probes again harmlessly (connect-only) and whose `umask(0o177)` bind
/// creates the inode at 0600 with an inode re-check.
///
/// The collision check runs BEFORE the bind, not after both binds as the
/// spec words it: the admin socket binds at startup step 7, before the
/// producer socket (step 11), and a colliding producer bind would otherwise
/// be refused by the admin socket's live listener with a message that names
/// the wrong cause.
pub(crate) fn bind_admin(path: &Path, socket_path: &Path) -> Result<BoundAdmin, AdminBindError> {
    socket::validate_socket_path(path).map_err(|source| AdminBindError {
        reason_code: reason::BIND_FAILED,
        source,
    })?;
    if names_same_file(path, socket_path) {
        return Err(AdminBindError {
            reason_code: reason::COLLIDES_WITH_SOCKET_PATH,
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "admin socket path '{}' names the same file as socket_path '{}'",
                    path.display(),
                    socket_path.display()
                ),
            ),
        });
    }
    // spec §5.9: refuse a live socket with the admin socket's own wording
    // before touching the path (PR 3's helper over the safety floor's
    // connect-only probe; `bind_hardened_with` probes again, harmlessly).
    socket::refuse_if_live(path, "admin socket").map_err(|source| AdminBindError {
        reason_code: classify_bind_error(&source),
        source,
    })?;
    let socket =
        socket::bind_hardened_with(path, ParentPolicy::RequirePrivate).map_err(|source| {
            AdminBindError {
                reason_code: classify_bind_error(&source),
                source,
            }
        })?;
    Ok(BoundAdmin {
        path: path.to_path_buf(),
        socket,
    })
}

fn classify_bind_error(e: &io::Error) -> &'static str {
    if socket::is_insecure_parent(e) {
        reason::PARENT_INSECURE
    } else if e.kind() == io::ErrorKind::AddrInUse {
        // `refuse_if_live`, `live_socket_refusal`, or EADDRINUSE from bind(2).
        reason::IN_USE
    } else {
        reason::BIND_FAILED
    }
}

/// True when the two paths are the same lexically, or have the same basename
/// in the same parent directory by (dev, ino), which catches a symlinked
/// parent such as `/var/run` → `/run`.
pub(crate) fn names_same_file(admin: &Path, socket_path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    use weir_status::admin::lexical_normalize;
    if lexical_normalize(admin) == lexical_normalize(socket_path) {
        return true;
    }
    if admin.file_name() != socket_path.file_name() {
        return false;
    }
    match (
        admin.parent().map(std::fs::metadata),
        socket_path.parent().map(std::fs::metadata),
    ) {
        (Some(Ok(a)), Some(Ok(b))) => (a.dev(), a.ino()) == (b.dev(), b.ino()),
        _ => false,
    }
}

/// PR 1's closed `AdminReason` (it renders on the public `/status`) for one
/// of this module's reason codes or the config layer's. A collision with
/// `socket_path` is `in_use` (the producer socket holds that name);
/// `no_parent` and `under_wab_dir`, which a derived path cannot produce, fall
/// to `bind_failed`.
fn admin_reason(code: &str) -> AdminReason {
    match code {
        reason::PARENT_INSECURE => AdminReason::ParentInsecure,
        "path_too_long" => AdminReason::PathTooLong,
        reason::IN_USE | reason::COLLIDES_WITH_SOCKET_PATH => AdminReason::InUse,
        _ => AdminReason::BindFailed,
    }
}

/// What startup step 7 hands the admin socket: the shared state its verbs
/// read and the handles they act through.
pub(crate) struct StartDeps {
    pub(crate) board: Arc<StatusBoard>,
    pub(crate) drain_ctl: DrainControl,
    /// A clone of the reload queue SIGHUP feeds (spec §5.10: one path).
    pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
    pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
    pub(crate) thresholds: Arc<Thresholds>,
    pub(crate) livez_checks: Arc<[LivezCheck]>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) started_at_ms: u64,
}

/// Startup step 7 (spec §6): resolve, bind and serve the admin socket. Must
/// run inside a tokio runtime (`main` holds `rt.enter()`).
///
/// `Err` only for an explicitly configured path (fatal, before recovery); a
/// derived default that cannot be used disables admin with an ERROR and
/// `AdminCell::set_unavailable`, which raises `admin_unavailable`.
pub(crate) fn start(cfg: &Config, deps: StartDeps) -> io::Result<Option<AdminHandle>> {
    let (path, explicit) = match cfg.admin_socket() {
        AdminSocketSetting::Disabled => {
            deps.board.admin.set_disabled();
            info!("admin socket disabled (admin_socket_enabled = false)");
            return Ok(None);
        }
        AdminSocketSetting::Explicit(p) => (p, true),
        AdminSocketSetting::Derived(p) => (p, false),
        AdminSocketSetting::DerivedUnusable { reason_code, path } => {
            error!(
                reason_code,
                path = ?path,
                "admin socket unavailable: the default path derived from wab_dir cannot be \
                 used; set admin_socket_path to a file in a directory writable only by the \
                 daemon user, or admin_socket_enabled = false"
            );
            deps.board.admin.set_unavailable(admin_reason(reason_code));
            return Ok(None);
        }
    };
    let bound = match bind_admin(&path, &cfg.socket_path) {
        Ok(b) => b,
        Err(e) if explicit => {
            return Err(io::Error::new(
                e.source.kind(),
                format!("admin_socket_path '{}': {}", path.display(), e.source),
            ));
        }
        Err(e) => {
            error!(
                path = %path.display(),
                reason_code = e.reason_code,
                error = %e.source,
                "admin socket unavailable; the daemon runs without it. Set admin_socket_path to \
                 a file in a directory writable only by the daemon user, or \
                 admin_socket_enabled = false"
            );
            deps.board
                .admin
                .set_unavailable(admin_reason(e.reason_code));
            return Ok(None);
        }
    };
    let instance = match identity::instance_id(&cfg.wab_dir, &cfg.socket_path, deps.started_at_ms) {
        Ok(i) => i,
        Err(e) => {
            // Bound but not served: do not leave the file behind.
            socket::remove_own_socket(&bound.path, &bound.socket);
            return Err(e);
        }
    };
    let state = Arc::new(AdminState {
        board: Arc::clone(&deps.board),
        drain_ctl: deps.drain_ctl,
        reload_tx: deps.reload_tx,
        level_rx: deps.level_rx,
        thresholds: deps.thresholds,
        livez_checks: deps.livez_checks,
        instance,
        metrics: deps.metrics,
        wab_max_bytes: cfg.wab_max_bytes,
        // Safety: geteuid has no preconditions.
        daemon_euid: unsafe { libc::geteuid() },
        mutate: tokio::sync::Mutex::new(()),
    });
    deps.board.admin.set_bound(bound.path.display().to_string());
    info!(path = %bound.path.display(), "admin socket listening");
    Ok(Some(spawn(bound, state)))
}

/// spec §11: at most 4 concurrent admin connections per socket.
pub(crate) const MAX_CONNECTIONS: usize = 4;

/// A refusal is one short line into a fresh socket; it must never hold the
/// accept loop for long.
const REFUSAL_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

/// The serving admin socket. Owns the listener (and, on Linux, its `O_PATH`
/// pin) until [`AdminHandle::close`], so the inode-checked removal at exit
/// compares against the inode this daemon bound.
pub(crate) struct AdminHandle {
    path: PathBuf,
    socket: Arc<BoundSocket>,
    task: JoinHandle<()>,
    metrics: Arc<Metrics>,
}

impl AdminHandle {
    /// T3 (spec §7 "unlink the admin socket (inode-checked)"): stop serving,
    /// remove the socket file only if it is still the inode this daemon
    /// bound, and mark the socket down. Synchronous, and safe after the
    /// runtime has shut down: aborting a task the runtime already cancelled
    /// is a no-op.
    pub(crate) fn close(self) {
        self.task.abort();
        socket::remove_own_socket(&self.path, &self.socket);
        self.metrics.admin_socket_up.set(0.0);
    }
}

/// Starts serving a bound admin socket. Must run inside a tokio runtime.
pub(crate) fn spawn(bound: BoundAdmin, state: Arc<AdminState>) -> AdminHandle {
    let socket = Arc::new(bound.socket);
    let metrics = Arc::clone(&state.metrics);
    let euid = state.daemon_euid;
    metrics.admin_socket_up.set(1.0);
    let task = tokio::spawn(accept_loop(
        Arc::clone(&socket),
        state,
        euid,
        Arc::clone(&metrics),
        Limits::PRODUCTION,
    ));
    AdminHandle {
        path: bound.path,
        socket,
        task,
        metrics,
    }
}

fn count(m: &Metrics, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) {
    m.admin_requests
        .get_or_create(&AdminRequestLabel { verb, outcome })
        .inc();
}

/// Serves until aborted (T3) or the runtime shuts down. The admin socket
/// stays up through shutdown so `status` and `reload` keep answering.
///
/// Every accepted connection is checked against the peer predicate (always
/// on, independent of `peer_uid_check`), then needs one of
/// [`MAX_CONNECTIONS`] permits. A refused peer gets one error line and EOF:
/// `forbidden` without `instance`, `busy` with it. Dropping this future
/// (abort) drops the `JoinSet`, which aborts every connection it serves.
async fn accept_loop<D>(
    socket: Arc<BoundSocket>,
    d: Arc<D>,
    daemon_euid: u32,
    metrics: Arc<Metrics>,
    limits: Limits,
) where
    D: Dispatch + Send + Sync + 'static,
{
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut conns: JoinSet<()> = JoinSet::new();
    loop {
        let stream = match socket.listener.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                warn!(error = %e, "admin socket: accept error");
                if socket::is_accept_resource_exhaustion(&e) {
                    // The pending connection stays queued: back off rather
                    // than spin (same rule as the producer accept loop).
                    tokio::time::sleep(socket::ACCEPT_BACKOFF_ON_EXHAUSTION).await;
                }
                continue;
            }
        };
        // Reap finished connections so the set does not grow without bound.
        while conns.try_join_next().is_some() {}
        // spec §11: on EVERY accept, independent of peer_uid_check.
        let peer_uid =
            match identity::authorize_peer(socket::peer::peer_uid_of(&stream), daemon_euid) {
                Ok(uid) => uid,
                Err(why) => {
                    warn!(
                        ?why,
                        daemon_euid,
                        "admin socket: refusing a peer that is neither the daemon's uid nor root"
                    );
                    count(
                        &metrics,
                        AdminVerbLabel::Connect,
                        AdminOutcomeLabel::refused,
                    );
                    // No `instance`: an unauthorized peer learns nothing
                    // (plan reconciliation 4).
                    refuse(
                        stream,
                        Response::failure(
                            Value::Null,
                            None,
                            AdminError::new(ErrorCode::Forbidden, "forbidden"),
                        ),
                    )
                    .await;
                    continue;
                }
            };
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            count(&metrics, AdminVerbLabel::Connect, AdminOutcomeLabel::busy);
            refuse(
                stream,
                Response::failure(
                    Value::Null,
                    d.instance(),
                    AdminError::new(
                        ErrorCode::Busy,
                        format!(
                            "the admin socket serves at most {MAX_CONNECTIONS} connections at once"
                        ),
                    ),
                ),
            )
            .await;
            continue;
        };
        let d = Arc::clone(&d);
        conns.spawn(async move {
            let _permit = permit;
            framing::serve_connection(stream, peer_uid, &*d, &limits).await;
        });
    }
}

/// Writes one refusal line (bounded by [`REFUSAL_WRITE_TIMEOUT`]) and closes.
async fn refuse(mut stream: UnixStream, r: Response) {
    let _ = framing::write_response(&mut stream, &r, REFUSAL_WRITE_TIMEOUT).await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn private_dir(label: &str) -> PathBuf {
        crate::testutil::scratch_dir(label)
    }

    // Every test here is a tokio test: `bind_admin` reaches tokio's
    // `UnixListener::bind`, which panics outside a runtime with IO enabled.
    // No umask lock: `umask_test_lock` is private to socket::tests, and the
    // bin tests run with --test-threads=1.
    #[tokio::test]
    async fn binds_mode_0600_in_a_private_parent() {
        let dir = private_dir("admin_bind_ok");
        let path = dir.join("wab.admin.sock");
        let b = bind_admin(&path, Path::new("/run/weir/weir.sock")).expect("binds");
        assert_eq!(
            std::fs::symlink_metadata(&path).unwrap().mode() & 0o777,
            0o600
        );
        assert_eq!(b.path, path);
        socket::remove_own_socket(&b.path, &b.socket);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn refuses_a_group_writable_parent() {
        let dir = private_dir("admin_bind_grp");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o770)).unwrap();
        let path = dir.join("wab.admin.sock");
        let e = bind_admin(&path, Path::new("/run/weir/weir.sock"))
            .err()
            .expect("refused");
        assert_eq!(e.reason_code, reason::PARENT_INSECURE);
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A second daemon (or anything) already listening keeps its socket.
    #[tokio::test]
    async fn refuses_when_a_live_socket_listens() {
        let dir = private_dir("admin_bind_live");
        let path = dir.join("wab.admin.sock");
        let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let e = bind_admin(&path, Path::new("/run/weir/weir.sock"))
            .err()
            .expect("refused");
        assert_eq!(e.reason_code, reason::IN_USE, "{}", e.source);
        assert!(
            e.source.to_string().contains("admin socket"),
            "the refusal names the admin socket: {}",
            e.source
        );
        std::os::unix::net::UnixStream::connect(&path).expect("the live socket is untouched");
        drop(live);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn replaces_a_stale_socket() {
        let dir = private_dir("admin_bind_stale");
        let path = dir.join("wab.admin.sock");
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap()); // file stays, nobody listens
        let b = bind_admin(&path, Path::new("/run/weir/weir.sock")).expect("stale is replaced");
        socket::remove_own_socket(&b.path, &b.socket);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `/var/run` → `/run`: different strings, same file (spec §11).
    #[tokio::test]
    async fn collides_with_socket_path_through_a_symlinked_parent() {
        let dir = private_dir("admin_bind_alias");
        let real = dir.join("real");
        crate::testutil::mkdir_p(&real);
        let alias = dir.join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let admin = alias.join("x.sock");
        let producer = real.join("x.sock");
        assert!(names_same_file(&admin, &producer));
        assert!(names_same_file(
            Path::new("/run/weir//x.sock"),
            Path::new("/run/weir/x.sock")
        ));
        assert!(!names_same_file(&alias.join("y.sock"), &producer));
        let e = bind_admin(&admin, &producer).err().expect("refused");
        assert_eq!(e.reason_code, reason::COLLIDES_WITH_SOCKET_PATH);
        assert!(!admin.exists(), "a refused bind creates nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    mod accept {
        use super::super::*;
        use crate::admin::test_support::{ReloadMode, harness, requests};
        use crate::metrics::{AdminOutcomeLabel, AdminVerbLabel};
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixStream as StdStream;
        use std::time::Duration;

        fn roundtrip(s: &StdStream, line: &str) -> serde_json::Value {
            let mut w = s.try_clone().unwrap();
            w.write_all(line.as_bytes()).unwrap();
            w.write_all(b"\n").unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut out = String::new();
            r.read_line(&mut out).unwrap();
            serde_json::from_str(&out).unwrap()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn serves_status_and_sheds_the_fifth_connection() {
            let h = harness(ReloadMode::Live);
            let dir = crate::testutil::scratch_dir("admin_accept_busy");
            let path = dir.join("a.sock");
            let bound = bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap();
            let handle = spawn(bound, Arc::clone(&h.st));
            assert_eq!(h.st.metrics.admin_socket_up.get(), 1.0);

            let held: Vec<StdStream> = (0..MAX_CONNECTIONS)
                .map(|_| {
                    let s = StdStream::connect(&path).unwrap();
                    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    // A full round trip proves the server accepted and holds a permit.
                    let r = roundtrip(&s, r#"{"v":1,"id":1,"verb":"status"}"#);
                    assert_eq!(r["ok"], true, "{r}");
                    assert_eq!(r["instance"]["pid"], std::process::id());
                    s
                })
                .collect();

            let fifth = StdStream::connect(&path).unwrap();
            fifth
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut line = String::new();
            BufReader::new(fifth.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let r: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(r["error"]["code"], "busy");
            assert_eq!(r["error"]["retryable"], true);
            assert_eq!(
                r["instance"]["pid"],
                std::process::id(),
                "an authorized peer: {r}"
            );
            assert_eq!(
                requests(
                    &h.st.metrics,
                    AdminVerbLabel::Connect,
                    AdminOutcomeLabel::busy
                ),
                1
            );

            drop(held);
            handle.close();
            assert!(!path.exists(), "close removes our own socket");
            assert_eq!(h.st.metrics.admin_socket_up.get(), 0.0);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// A successor that replaced the file keeps it (own-inode-only removal).
        #[tokio::test]
        async fn close_leaves_a_socket_it_did_not_bind() {
            let h = harness(ReloadMode::Live);
            let dir = crate::testutil::scratch_dir("admin_accept_successor");
            let path = dir.join("a.sock");
            let handle = spawn(
                bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap(),
                Arc::clone(&h.st),
            );
            std::fs::remove_file(&path).unwrap();
            let successor = std::os::unix::net::UnixListener::bind(&path).unwrap();
            handle.close();
            assert!(path.exists(), "the successor's socket stays");
            drop(successor);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// spec §11 + Q8: a peer that is neither the daemon's uid nor root gets
        /// one `forbidden` line with no `instance`, then EOF, and the refusal is
        /// counted. Driven in-process by handing `accept_loop` a daemon euid
        /// that is not the test's own.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_foreign_uid_gets_one_forbidden_line_without_identity_then_eof() {
            let me = crate::admin::test_support::uid();
            if me == 0 {
                return; // root is always admitted: the branch is unreachable as root
            }
            let h = harness(ReloadMode::Live);
            let dir = crate::testutil::scratch_dir("admin_accept_forbidden");
            let path = dir.join("a.sock");
            let bound = bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap();
            let socket = Arc::new(bound.socket);
            let task = tokio::spawn(accept_loop(
                Arc::clone(&socket),
                Arc::clone(&h.st),
                me + 1,
                Arc::clone(&h.st.metrics),
                framing::Limits::PRODUCTION,
            ));
            let s = StdStream::connect(&path).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut r = BufReader::new(s);
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(v["error"]["code"], "forbidden");
            assert!(
                v.get("instance").is_none(),
                "an unauthorized peer learns nothing: {v}"
            );
            line.clear();
            assert_eq!(
                r.read_line(&mut line).unwrap(),
                0,
                "then the connection closes"
            );
            assert_eq!(
                requests(
                    &h.st.metrics,
                    AdminVerbLabel::Connect,
                    AdminOutcomeLabel::refused
                ),
                1
            );
            task.abort();
            socket::remove_own_socket(&path, &socket);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// Test scaffolding shared by the verb, dispatcher and accept-loop tests: an
/// [`AdminState`](verbs::AdminState) over a test board, a detached drain and
/// a live or stalled reload loop.
#[cfg(test)]
pub(crate) mod test_support {
    use std::{path::Path, sync::Arc};

    use serde_json::{Map, Value};
    use tokio::sync::{Mutex, mpsc, watch};

    use super::{identity, verbs::AdminState};
    use crate::{
        drain::control::DrainControl,
        lifecycle::ShutdownLevel,
        metrics::{AdminOutcomeLabel, AdminRequestLabel, AdminVerbLabel, Metrics},
        reload::{self, ReloadRequest, Reloader},
        status::StatusBoard,
    };

    pub(crate) enum ReloadMode {
        /// A real reload loop (no TLS: every reload is a `noop`).
        Live,
        /// A queue nobody serves.
        Stalled,
    }

    pub(crate) struct Harness {
        pub(crate) st: Arc<AdminState>,
        pub(crate) level_tx: watch::Sender<ShutdownLevel>,
        pub(crate) _stalled: Option<mpsc::Receiver<ReloadRequest>>,
    }

    /// Must run inside a tokio runtime (the live loop is spawned).
    pub(crate) fn harness(mode: ReloadMode) -> Harness {
        let metrics = Arc::new(Metrics::new().0);
        let (level_tx, level_rx) = watch::channel(ShutdownLevel::Running);
        let (reload_tx, stalled) = match mode {
            ReloadMode::Live => (
                reload::spawn_reload_loop(Reloader::new(Arc::clone(&metrics), false)),
                None,
            ),
            ReloadMode::Stalled => {
                let (tx, rx) = reload::reload_channel();
                (tx, Some(rx))
            }
        };
        let wab = crate::testutil::scratch_dir("admin_harness");
        let st = Arc::new(AdminState {
            board: StatusBoard::for_test(),
            drain_ctl: DrainControl::detached(),
            reload_tx,
            level_rx,
            thresholds: Arc::new(weir_status::Thresholds::default()),
            livez_checks: Arc::from(Vec::<weir_status::LivezCheck>::new()),
            instance: identity::instance_id(&wab, Path::new("/run/weir/weir.sock"), 1).unwrap(),
            metrics,
            wab_max_bytes: 0,
            daemon_euid: uid(),
            mutate: Mutex::new(()),
        });
        Harness {
            st,
            level_tx,
            _stalled: stalled,
        }
    }

    /// One request line through the real dispatcher, as an authorized peer
    /// with the test's own uid.
    pub(crate) async fn call(st: &AdminState, line: &str) -> weir_status::admin::Response {
        use super::framing::Dispatch;
        st.dispatch(line.as_bytes(), uid()).await
    }

    pub(crate) fn uid() -> u32 {
        // Safety: geteuid has no preconditions.
        unsafe { libc::geteuid() }
    }

    pub(crate) fn args(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    pub(crate) fn requests(m: &Metrics, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) -> u64 {
        m.admin_requests
            .get_or_create(&AdminRequestLabel { verb, outcome })
            .get()
    }
}
