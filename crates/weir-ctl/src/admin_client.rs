//! Client for the daemon's admin socket (spec §11), with the client-side
//! identity checks: weir-ctl never trusts a payload from a socket it has not
//! authenticated.

use std::path::{Path, PathBuf};

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
}
