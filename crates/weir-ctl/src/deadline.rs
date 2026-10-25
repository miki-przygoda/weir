//! One deadline for a whole read, shared by the admin-socket client and the
//! metrics-listener GET, and the timeout setup both share.

use std::io::{self, Read};
use std::time::{Duration, Instant};

/// A connected stream whose read timeout can be re-armed and that can be
/// made non-blocking.
pub(crate) trait TimedSocket {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
}

impl TimedSocket for std::net::TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::net::TcpStream::set_read_timeout(self, timeout)
    }
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        std::net::TcpStream::set_nonblocking(self, nonblocking)
    }
}

#[cfg(unix)]
impl TimedSocket for std::os::unix::net::UnixStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, timeout)
    }
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_nonblocking(self, nonblocking)
    }
}

/// Whether a failed `set_read_timeout` / `set_write_timeout` means the peer
/// has already closed. macOS (and the BSDs) refuse every setsockopt with
/// EINVAL once both directions of a socket are shut, which a peer's close
/// does: the daemon answers `busy` or `forbidden` at accept and closes
/// (weir-testkit's a_hanging_sink_never_answers_and_closes_on_drop met the
/// same). Linux never fails these calls with EINVAL, and std's own refusal
/// of a zero timeout carries no OS error, so neither matches.
pub(crate) fn peer_already_closed(e: &io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(libc::EINVAL)
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

/// The result of setting a timeout on `stream`, with a closed peer's EINVAL
/// (see [`peer_already_closed`]) taken as success: a socket whose peer has
/// closed cannot block, a read returns what was queued and then EOF and a
/// write fails at once, so the reply or refusal already sent is still read.
/// The stream is also made non-blocking, so that even an EINVAL with another
/// cause cannot leave a read or write without a bound. Any other error is
/// returned unchanged.
pub(crate) fn tolerate_closed_peer<S: TimedSocket>(
    stream: &S,
    set: io::Result<()>,
) -> io::Result<()> {
    match set {
        Err(e) if peer_already_closed(&e) => {
            // Best effort: when this fails too, the closed socket still
            // cannot block.
            let _ = stream.set_nonblocking(true);
            Ok(())
        }
        other => other,
    }
}

/// Re-arms the stream's read timeout to what is left of one deadline before
/// every read, so the deadline bounds the whole read: a per-read timeout
/// alone would let a peer that dribbles bytes hold weir-ctl for as many
/// reads as the size cap allows. Past the deadline a read fails `TimedOut`;
/// a read the socket timeout cuts short fails `WouldBlock` (or `TimedOut`).
pub(crate) struct DeadlineReader<'a, S> {
    stream: &'a S,
    deadline: Instant,
}

impl<'a, S> DeadlineReader<'a, S> {
    pub(crate) fn new(stream: &'a S, timeout: Duration) -> DeadlineReader<'a, S> {
        DeadlineReader {
            stream,
            deadline: Instant::now() + timeout,
        }
    }
}

impl<S: TimedSocket> Read for DeadlineReader<'_, S>
where
    for<'s> &'s S: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the deadline passed",
            ));
        }
        tolerate_closed_peer(self.stream, self.stream.set_read_timeout(Some(left)))?;
        let mut stream = self.stream;
        stream.read(buf)
    }
}

/// A read past the deadline, or one the socket timeout cut short.
pub(crate) fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    /// A socket whose timeout calls fail with one raw OS error, and that
    /// serves `queued` and then EOF, as a socket whose peer has closed does.
    struct ClosedPeer {
        timeout_errno: Option<i32>,
        queued: RefCell<&'static [u8]>,
        nonblocking: Cell<bool>,
    }

    impl ClosedPeer {
        fn new(timeout_errno: Option<i32>, queued: &'static [u8]) -> ClosedPeer {
            ClosedPeer {
                timeout_errno,
                queued: RefCell::new(queued),
                nonblocking: Cell::new(false),
            }
        }
    }

    impl TimedSocket for ClosedPeer {
        fn set_read_timeout(&self, _: Option<Duration>) -> io::Result<()> {
            self.timeout_errno
                .map_or(Ok(()), |n| Err(io::Error::from_raw_os_error(n)))
        }
        fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.nonblocking.set(nonblocking);
            Ok(())
        }
    }

    impl Read for &ClosedPeer {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut q = self.queued.borrow_mut();
            let n = buf.len().min(q.len());
            buf[..n].copy_from_slice(&q[..n]);
            *q = &q[n..];
            Ok(n)
        }
    }

    /// macOS refuses setsockopt with EINVAL once the peer has closed: the
    /// reply it queued first (a `busy` line) must still be read, not turned
    /// into "set timeout: Invalid argument".
    #[test]
    fn a_closed_peer_s_einval_still_reads_what_it_queued() {
        let s = ClosedPeer::new(Some(libc::EINVAL), b"busy\n");
        let mut got = Vec::new();
        DeadlineReader::new(&s, Duration::from_secs(5))
            .read_to_end(&mut got)
            .unwrap();
        assert_eq!(got, b"busy\n");
        assert!(s.nonblocking.get(), "and nothing can wait without a bound");
    }

    #[test]
    fn only_an_os_einval_is_a_closed_peer() {
        assert!(peer_already_closed(&io::Error::from_raw_os_error(
            libc::EINVAL
        )));
        assert!(!peer_already_closed(&io::Error::from_raw_os_error(
            libc::EBADF
        )));
        // std refuses a zero timeout itself, as InvalidInput with no OS error.
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let s = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let zero = s.set_read_timeout(Some(Duration::ZERO)).unwrap_err();
        assert_eq!(zero.kind(), io::ErrorKind::InvalidInput);
        assert!(!peer_already_closed(&zero));
    }

    #[test]
    fn tolerate_closed_peer_passes_every_other_result_through() {
        let s = ClosedPeer::new(None, b"");
        assert!(tolerate_closed_peer(&s, Ok(())).is_ok());
        let e =
            tolerate_closed_peer(&s, Err(io::Error::from_raw_os_error(libc::EBADF))).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EBADF));
        assert!(
            !s.nonblocking.get(),
            "only the closed-peer case changes the socket"
        );
        assert!(tolerate_closed_peer(&s, Err(io::Error::from_raw_os_error(libc::EINVAL))).is_ok());
        assert!(s.nonblocking.get());
        // Any other failure still fails the read.
        let s = ClosedPeer::new(Some(libc::EBADF), b"busy\n");
        let mut got = Vec::new();
        let e = DeadlineReader::new(&s, Duration::from_secs(5))
            .read_to_end(&mut got)
            .unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EBADF));
        assert!(got.is_empty());
    }
}
