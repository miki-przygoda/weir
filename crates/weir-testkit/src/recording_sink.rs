//! A tiny HTTP/1.1 sink endpoint that records what weir delivers.
//!
//! Point a daemon at it with `sink_type = "http"` and
//! `WEIR_SINK_URL = sink.url()`. It answers `200` to every request (the HTTP
//! sink's `HEAD` health probe included) and keeps the body of each `POST`, so a
//! test can assert on the payloads that actually left weir rather than on WAB
//! bytes, which graceful shutdown deletes once they are delivered.
//!
//! [`RecordingSink::start_hanging`] is the black-hole variant: it accepts every
//! connection and never answers, so a sink call waits for its timeout. Dropping
//! it closes every held connection and the listener, so the daemon's next
//! attempt is refused. [`RecordingSink::start_slow`] is the slow-but-healthy
//! variant: it answers and records every request, each after a fixed delay, so
//! a test can make sink latency (not availability) the bottleneck.
//!
//! Deliberately minimal: one thread per connection, keep-alive, and only the
//! request framing the HTTP sink produces (`Content-Length`, plus `chunked` for
//! safety). It is a test fixture, not a server.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// How the fixture answers.
#[derive(Clone, Copy)]
enum Mode {
    /// Answer every request, after `delay`.
    Answer { delay: std::time::Duration },
    /// Accept and never answer (the black hole).
    Hang,
}

/// A running recording endpoint. Dropping it stops accepting new connections.
pub struct RecordingSink {
    addr: SocketAddr,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    connections: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl RecordingSink {
    /// Binds `127.0.0.1:0` and starts serving on a background thread.
    pub fn start() -> RecordingSink {
        Self::start_mode(Mode::Answer {
            delay: std::time::Duration::ZERO,
        })
    }

    /// A black hole: binds `127.0.0.1:0`, accepts every connection (counted by
    /// [`connections`](Self::connections)) and never reads or answers on it,
    /// so the daemon's sink call hangs until its `sink_timeout_secs`. Nothing
    /// is recorded. Dropping the sink closes the held connections and the
    /// listener: the hung call fails at once and later ones are refused.
    pub fn start_hanging() -> RecordingSink {
        Self::start_mode(Mode::Hang)
    }

    /// A slow but healthy endpoint: every request (the `HEAD` probe
    /// included) is answered `200` after `per_request`, and POST bodies are
    /// recorded as with [`start`](Self::start). Models a sink whose commit
    /// latency, not its availability, is the bottleneck.
    pub fn start_slow(per_request: std::time::Duration) -> RecordingSink {
        Self::start_mode(Mode::Answer { delay: per_request })
    }

    /// Like [`start`](Self::start), on a fixed loopback port: for a test that
    /// points the daemon at a sink that is down at first and comes up later.
    /// Panics if the port was taken in between.
    pub fn start_on(port: u16) -> RecordingSink {
        Self::start_mode_at(
            ("127.0.0.1", port),
            Mode::Answer {
                delay: std::time::Duration::ZERO,
            },
        )
    }

    fn start_mode(mode: Mode) -> RecordingSink {
        Self::start_mode_at(("127.0.0.1", 0), mode)
    }

    fn start_mode_at(addr: (&str, u16), mode: Mode) -> RecordingSink {
        let listener = TcpListener::bind(addr).expect("bind recording sink");
        let addr = listener.local_addr().expect("recording sink local_addr");
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let connections = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let (b, c, s) = (
            Arc::clone(&bodies),
            Arc::clone(&connections),
            Arc::clone(&stop),
        );
        std::thread::Builder::new()
            .name("recording-sink-accept".into())
            .spawn(move || {
                for conn in listener.incoming() {
                    if s.load(Ordering::Relaxed) {
                        break;
                    }
                    let Ok(conn) = conn else { continue };
                    c.fetch_add(1, Ordering::SeqCst);
                    let b = Arc::clone(&b);
                    let s = Arc::clone(&s);
                    let _ = std::thread::Builder::new()
                        .name("recording-sink-conn".into())
                        .spawn(move || match mode {
                            Mode::Hang => hold(conn, &s),
                            Mode::Answer { delay } => serve(conn, &b, delay),
                        });
                }
            })
            .expect("spawn recording sink thread");

        RecordingSink {
            addr,
            bodies,
            connections,
            stop,
        }
    }

    /// The URL to configure as the daemon's `sink_url`.
    pub fn url(&self) -> String {
        format!("http://{}/sink", self.addr)
    }

    /// Every POST body received so far, in arrival order. Includes retries:
    /// delivery is at-least-once, so the same payload can appear twice.
    pub fn bodies(&self) -> Vec<Vec<u8>> {
        self.bodies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// How many TCP connections the endpoint has accepted, whatever was (or
    /// was not) sent on them. Stricter than [`bodies`](Self::bodies) for a
    /// "nothing reached the sink" assertion: it also counts the HTTP sink's
    /// `HEAD` health probe and a connection dropped before its first request.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

impl Drop for RecordingSink {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wake the blocking accept so the thread sees `stop`.
        let _ = TcpStream::connect(self.addr);
    }
}

/// Holds a connection open, unread and unanswered, until the sink is dropped;
/// returning drops (closes) it.
fn hold(_conn: TcpStream, stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Serves one keep-alive connection until the peer closes it or sends
/// something this fixture does not understand. Each answer waits `delay`
/// first (zero for [`RecordingSink::start`]); the body is recorded before the
/// wait, as a real sink commits before it acknowledges.
fn serve(conn: TcpStream, bodies: &Mutex<Vec<Vec<u8>>>, delay: std::time::Duration) {
    let Ok(mut writer) = conn.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(conn);
    loop {
        let mut request_line = String::new();
        match reader.read_line(&mut request_line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let method = request_line
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        if method.is_empty() {
            return;
        }

        let mut content_length = 0usize;
        let mut chunked = false;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                let value = value.trim();
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.parse().unwrap_or(0);
                } else if name.eq_ignore_ascii_case("transfer-encoding")
                    && value.eq_ignore_ascii_case("chunked")
                {
                    chunked = true;
                }
            }
        }

        let body = if chunked {
            match read_chunked(&mut reader) {
                Some(b) => b,
                None => return,
            }
        } else {
            let mut b = vec![0u8; content_length];
            if reader.read_exact(&mut b).is_err() {
                return;
            }
            b
        };

        if method == "POST" {
            bodies.lock().unwrap_or_else(|e| e.into_inner()).push(body);
        }
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        if writer
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
            .is_err()
        {
            return;
        }
    }
}

fn read_chunked(reader: &mut BufReader<TcpStream>) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line).ok()?;
        let size_hex = size_line.trim().split(';').next()?.trim();
        let size = usize::from_str_radix(size_hex, 16).ok()?;
        if size == 0 {
            // Trailers (none expected) end with an empty line.
            loop {
                let mut trailer = String::new();
                reader.read_line(&mut trailer).ok()?;
                if trailer.trim().is_empty() {
                    return Some(body);
                }
            }
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..]).ok()?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).ok()?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn request(stream: &mut TcpStream, raw: &[u8]) -> String {
        stream.write_all(raw).unwrap();
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    #[test]
    fn records_post_bodies_over_one_keep_alive_connection() {
        let sink = RecordingSink::start();
        let mut s = TcpStream::connect(sink.addr).unwrap();
        let head = request(&mut s, b"HEAD /sink HTTP/1.1\r\nhost: x\r\n\r\n");
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let r = request(
            &mut s,
            b"POST /sink HTTP/1.1\r\ncontent-length: 5\r\n\r\nhello",
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        let r = request(
            &mut s,
            b"POST /sink HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        assert_eq!(sink.bodies(), vec![b"hello".to_vec(), b"abcde".to_vec()]);
        assert_eq!(sink.connections(), 1, "one keep-alive connection");
        assert!(sink.url().starts_with("http://127.0.0.1:"));
    }

    #[test]
    fn a_hanging_sink_never_answers_and_closes_on_drop() {
        let sink = RecordingSink::start_hanging();
        let addr = sink.addr;
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(b"POST /sink HTTP/1.1\r\ncontent-length: 5\r\n\r\nhello")
            .unwrap();
        s.set_read_timeout(Some(std::time::Duration::from_millis(300)))
            .unwrap();
        let mut buf = [0u8; 16];
        let err = s.read(&mut buf).expect_err("a black hole never answers");
        assert!(
            matches!(
                err.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "{err:?}"
        );
        assert_eq!(sink.connections(), 1);
        assert!(sink.bodies().is_empty());

        // Set the timeout BEFORE the sink goes away: once the peer has reset
        // the connection, macOS can reject setsockopt on this socket with
        // EINVAL, which made this test flaky there.
        s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        drop(sink);
        // Closed: EOF, or a reset if the unread request was still buffered.
        match s.read(&mut buf) {
            Ok(0) => {}
            Ok(n) => panic!("a black hole must not answer, got {n} bytes"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset, "{e:?}"),
        }
        // And the listener is gone. The accept thread exits after its wake-up
        // connect, so allow it a moment.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while TcpStream::connect(addr).is_ok() {
            assert!(std::time::Instant::now() < deadline, "listener still open");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    #[test]
    fn start_on_serves_and_records_on_the_given_port() {
        let port = crate::free_port();
        let sink = RecordingSink::start_on(port);
        assert_eq!(sink.url(), format!("http://127.0.0.1:{port}/sink"));
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let r = request(
            &mut s,
            b"POST /sink HTTP/1.1\r\ncontent-length: 2\r\n\r\nhi",
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        assert_eq!(sink.bodies(), vec![b"hi".to_vec()]);
    }

    #[test]
    fn start_slow_delays_every_answer_and_still_records() {
        let sink = RecordingSink::start_slow(Duration::from_millis(200));
        let mut s = TcpStream::connect(sink.addr).unwrap();
        let t0 = Instant::now();
        s.write_all(b"POST /sink HTTP/1.1\r\ncontent-length: 3\r\n\r\nabc")
            .unwrap();
        let mut buf = [0u8; 64];
        let n = s.read(&mut buf).unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        assert!(
            t0.elapsed() >= Duration::from_millis(200),
            "the answer must wait the configured delay, took {:?}",
            t0.elapsed()
        );
        assert_eq!(sink.bodies(), vec![b"abc".to_vec()]);
    }
}
