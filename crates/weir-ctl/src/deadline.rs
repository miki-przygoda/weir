//! One deadline for a whole read, shared by the admin-socket client and the
//! metrics-listener GET.

use std::io::{self, Read};
use std::time::{Duration, Instant};

/// A connected stream whose read timeout can be re-armed.
pub(crate) trait ReadTimeout {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
}

impl ReadTimeout for std::net::TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::net::TcpStream::set_read_timeout(self, timeout)
    }
}

#[cfg(unix)]
impl ReadTimeout for std::os::unix::net::UnixStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, timeout)
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

impl<S: ReadTimeout> Read for DeadlineReader<'_, S>
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
        self.stream.set_read_timeout(Some(left))?;
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
