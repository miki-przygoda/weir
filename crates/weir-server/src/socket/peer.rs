//! Peer-credential check (`SO_PEERCRED` on Linux, `getpeereid` on macOS).
//!
//! Defense-in-depth on top of the socket file's `0o600` mode: if an operator
//! ever loosens those bits, or a producer manages to reach the socket inode
//! some other way, the connection still gets refused unless the peer's
//! effective uid matches the daemon's.
//!
//! The kernel attaches peer credentials to the connecting socket at
//! `connect(2)` time, so the value cannot be spoofed by the peer process.

use std::io;
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
pub(crate) fn peer_uid(fd: i32) -> io::Result<u32> {
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(cred.uid)
}

#[cfg(target_os = "macos")]
pub(crate) fn peer_uid(fd: i32) -> io::Result<u32> {
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    let ret = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(uid)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn peer_uid(_fd: i32) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "peer credential check not implemented on this platform",
    ))
}

/// Convenience: extract the peer uid from a `UnixStream`.
pub(crate) fn peer_uid_of<S: AsRawFd>(stream: &S) -> io::Result<u32> {
    peer_uid(stream.as_raw_fd())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixStream;

    #[tokio::test]
    async fn peer_uid_of_socketpair_returns_current_euid() {
        // Both ends of a socketpair are owned by the test process. peer_uid
        // on either side must therefore equal our own euid.
        let (a, b) = UnixStream::pair().unwrap();
        let our_uid = unsafe { libc::geteuid() };
        assert_eq!(peer_uid_of(&a).unwrap(), our_uid);
        assert_eq!(peer_uid_of(&b).unwrap(), our_uid);
    }

    /// S32: peer_uid must FAIL CLOSED. A non-socket fd carries no peer
    /// credentials, so the credential syscall errors — peer_uid must surface that
    /// error rather than ever returning a uid the accept loop would accept. This
    /// pins the fail-closed contract the accept-loop refusal branch relies on.
    #[test]
    fn peer_uid_fails_closed_on_a_non_socket_fd() {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open("/dev/null").unwrap();
        assert!(
            peer_uid(f.as_raw_fd()).is_err(),
            "peer_uid on a non-socket fd must fail closed, never return a uid"
        );
    }
}
