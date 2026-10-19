//! The admin Unix socket (Foundations spec §5.9, §11): a JSON-lines control
//! channel for the daemon's own uid and root, separate from the producer
//! socket and never on the producer-shared directory.

// Unreachable from `main` until Task 6.14 wires it in. Unconditional, so it
// also covers the test target that `--all-targets` lints; Task 6.14 deletes
// this line and clippy must then pass without it.
#![allow(dead_code, reason = "wired into main by Task 6.14")]

pub(crate) mod identity;

use std::{
    fmt, io,
    path::{Path, PathBuf},
};

use crate::socket::{self, BoundSocket, ParentPolicy};

/// Why the admin socket could not be bound. Codes only, from code: they go
/// into log fields and error messages. The status board carries PR 1's closed
/// `weir_status::AdminReason` instead, mapped from these codes (plus the
/// config layer's `no_parent`, `path_too_long`, `under_wab_dir`) by Task
/// 6.14's `admin_reason`.
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
}
