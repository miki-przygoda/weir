//! A tiny HTTP/1.1 sink endpoint that records what weir delivers.
//!
//! Point a daemon at it with `sink_type = "http"` and
//! `WEIR_SINK_URL = sink.url()`. It answers `200` to every request (the HTTP
//! sink's `HEAD` health probe included) and keeps the body of each `POST`, so a
//! test can assert on the payloads that actually left weir rather than on WAB
//! bytes, which graceful shutdown deletes once they are delivered.
//!
//! Deliberately minimal: one thread per connection, keep-alive, and only the
//! request framing the HTTP sink produces (`Content-Length`, plus `chunked` for
//! safety). It is a test fixture, not a server.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A running recording endpoint. Dropping it stops accepting new connections.
pub struct RecordingSink {
    addr: SocketAddr,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    stop: Arc<AtomicBool>,
}

impl RecordingSink {
    /// Binds `127.0.0.1:0` and starts serving on a background thread.
    pub fn start() -> RecordingSink {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind recording sink");
        let addr = listener.local_addr().expect("recording sink local_addr");
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));

        let (b, s) = (Arc::clone(&bodies), Arc::clone(&stop));
        std::thread::Builder::new()
            .name("recording-sink-accept".into())
            .spawn(move || {
                for conn in listener.incoming() {
                    if s.load(Ordering::Relaxed) {
                        break;
                    }
                    let Ok(conn) = conn else { continue };
                    let b = Arc::clone(&b);
                    let _ = std::thread::Builder::new()
                        .name("recording-sink-conn".into())
                        .spawn(move || serve(conn, &b));
                }
            })
            .expect("spawn recording sink thread");

        RecordingSink { addr, bodies, stop }
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
}

impl Drop for RecordingSink {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wake the blocking accept so the thread sees `stop`.
        let _ = TcpStream::connect(self.addr);
    }
}

/// Serves one keep-alive connection until the peer closes it or sends
/// something this fixture does not understand.
fn serve(conn: TcpStream, bodies: &Mutex<Vec<Vec<u8>>>) {
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
        assert!(sink.url().starts_with("http://127.0.0.1:"));
    }
}
