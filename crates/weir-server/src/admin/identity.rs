//! Who may use the admin socket, and who the daemon says it is.

use std::{io, path::Path};

use weir_status::admin::InstanceId;

/// The admin peer predicate (owner decision, spec §20): the daemon's own
/// effective uid, or root. A deliberate divergence from the producer rule
/// (root is not implicitly a producer), documented in threat-model.md.
pub(crate) fn peer_allowed(peer_uid: u32, daemon_euid: u32) -> bool {
    peer_uid == daemon_euid || peer_uid == 0
}

/// Why a peer was refused the admin socket.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PeerRefusal {
    /// The peer's uid is neither the daemon's euid nor root.
    WrongUid(u32),
    /// The platform or socket gave no peer credentials. Fails closed.
    NoCredentials,
}

/// Applies [`peer_allowed`] to a `peer_uid_of` result. Always on; does not
/// read `peer_uid_check`, which governs producers only.
pub(crate) fn authorize_peer(
    lookup: io::Result<u32>,
    daemon_euid: u32,
) -> Result<u32, PeerRefusal> {
    match lookup {
        Ok(uid) if peer_allowed(uid, daemon_euid) => Ok(uid),
        Ok(uid) => Err(PeerRefusal::WrongUid(uid)),
        Err(_) => Err(PeerRefusal::NoCredentials),
    }
}

/// The identity stamped on every response to an authorized peer, so a client
/// can check it acted on the daemon it meant (weir-ctl compares
/// `wab_dir_dev/ino` with `stat(--wab-dir)`, spec §11).
pub(crate) fn instance_id(
    wab_dir: &Path,
    socket_path: &Path,
    started_at_ms: u64,
) -> io::Result<InstanceId> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(wab_dir)?;
    Ok(InstanceId {
        pid: std::process::id(),
        started_at_ms,
        wab_dir: wab_dir.display().to_string(),
        socket_path: socket_path.display().to_string(),
        wab_dir_dev: m.dev(),
        wab_dir_ino: m.ino(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owner decision (spec §20): daemon euid or root, nobody else. This is
    /// independent of `peer_uid_check`, which governs producers only.
    #[test]
    fn the_peer_predicate_is_daemon_uid_or_root() {
        assert!(peer_allowed(1000, 1000));
        assert!(peer_allowed(0, 1000));
        assert!(!peer_allowed(1001, 1000));
        assert!(!peer_allowed(10001, 1000), "a producer uid is not an admin");
        assert!(peer_allowed(0, 0));
    }

    #[test]
    fn authorization_fails_closed() {
        assert_eq!(authorize_peer(Ok(1000), 1000), Ok(1000));
        assert_eq!(authorize_peer(Ok(0), 1000), Ok(0));
        assert_eq!(authorize_peer(Ok(7), 1000), Err(PeerRefusal::WrongUid(7)));
        let no_creds = Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "x"));
        assert_eq!(
            authorize_peer(no_creds, 1000),
            Err(PeerRefusal::NoCredentials)
        );
    }

    #[test]
    fn instance_carries_the_wab_dir_inode() {
        use std::os::unix::fs::MetadataExt;
        let dir = crate::testutil::scratch_dir("admin_identity");
        let id = instance_id(&dir, Path::new("/run/weir/weir.sock"), 123).unwrap();
        let m = std::fs::metadata(&dir).unwrap();
        assert_eq!((id.wab_dir_dev, id.wab_dir_ino), (m.dev(), m.ino()));
        assert_eq!(id.pid, std::process::id());
        assert_eq!(id.started_at_ms, 123);
        assert_eq!(id.wab_dir, dir.display().to_string());
        assert_eq!(id.socket_path, "/run/weir/weir.sock");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
