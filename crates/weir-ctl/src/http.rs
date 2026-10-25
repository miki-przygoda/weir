//! A minimal, bounded HTTP/1.0 GET for the daemon's metrics listener, so
//! weir-ctl needs no HTTP client dependency. The request bytes for `/metrics`
//! are exactly the ones `scrape` always sent.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Connect (per resolved address), read and write deadline.
pub(crate) const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// A /status document is bounded at 256 shards and 40 conditions (spec §10),
/// and a /metrics body is a few MiB at most; anything larger is refused.
pub(crate) const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    /// `None` when the peer sent no parseable status line.
    pub status: Option<u16>,
    pub content_type: Option<String>,
    pub body: String,
}

pub(crate) fn http_get(addr: &str, path: &str) -> Result<HttpResponse, String> {
    // Try every resolved address in turn, as `TcpStream::connect(addr)` in the
    // old `scrape` did: `localhost` may resolve to ::1 first while the daemon
    // listens on 127.0.0.1 only.
    let addrs: Vec<std::net::SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| format!("connect {addr}: {e}"))?
        .collect();
    let mut last_err: Option<std::io::Error> = None;
    let mut found = None;
    for a in &addrs {
        match TcpStream::connect_timeout(a, HTTP_TIMEOUT) {
            Ok(s) => {
                found = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = found.ok_or_else(|| match last_err {
        Some(e) => format!("connect {addr}: {e}"),
        None => format!("connect {addr}: no address"),
    })?;
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(HTTP_TIMEOUT)))
        .map_err(|e| format!("set timeout: {e}"))?;
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n").as_bytes())
        .map_err(|e| format!("write GET: {e}"))?;
    let mut raw = Vec::new();
    (&mut stream)
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|e| format!("read {path}: {e}"))?;
    if raw.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(format!(
            "read {path}: response larger than {MAX_RESPONSE_BYTES} bytes"
        ));
    }
    Ok(parse_http_response(&String::from_utf8_lossy(&raw)))
}

pub(crate) fn parse_http_response(raw: &str) -> HttpResponse {
    let Some((head, body)) = raw.split_once("\r\n\r\n") else {
        // No head at all: hand back everything, as `scrape` always did.
        return HttpResponse {
            status: None,
            content_type: None,
            body: raw.to_string(),
        };
    };
    let mut lines = head.split("\r\n");
    let status = lines.next().and_then(|l| {
        let mut parts = l.split_whitespace();
        let version = parts.next()?;
        if !version.starts_with("HTTP/") {
            return None;
        }
        parts.next()?.parse().ok()
    });
    let content_type = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.trim().to_string());
    HttpResponse {
        status,
        content_type,
        body: body.to_string(),
    }
}

/// `application/json` with any parameters. A pre-4.1 daemon answers every path,
/// `/status` included, with `application/openmetrics-text`.
pub(crate) fn is_json(content_type: Option<&str>) -> bool {
    content_type
        .and_then(|c| c.split(';').next())
        .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    /// Serves one canned response and returns the exact request bytes it read.
    fn one_shot(response: &'static str) -> (String, std::thread::JoinHandle<Vec<u8>>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut r = BufReader::new(&s);
            let mut req = Vec::new();
            loop {
                let mut line = Vec::new();
                r.read_until(b'\n', &mut line).unwrap();
                req.extend_from_slice(&line);
                if line == b"\r\n" || line.is_empty() {
                    break;
                }
            }
            (&s).write_all(response.as_bytes()).unwrap();
            req
        });
        (addr, h)
    }

    #[test]
    fn metrics_request_bytes_are_unchanged() {
        let (addr, h) = one_shot(
            "HTTP/1.0 200 OK\r\nContent-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\r\nweir_queue_depth 3\n",
        );
        let r = http_get(&addr, "/metrics").unwrap();
        assert_eq!(
            h.join().unwrap(),
            b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n"
        );
        assert_eq!(r.status, Some(200));
        assert_eq!(r.body, "weir_queue_depth 3\n");
        assert!(!is_json(r.content_type.as_deref()));
    }

    #[test]
    fn status_json_is_recognised_by_content_type() {
        let (addr, h) = one_shot(
            "HTTP/1.0 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nCache-Control: no-store\r\n\r\n{\"schema\":\"weir.status/v1\"}",
        );
        let r = http_get(&addr, "/status").unwrap();
        assert!(h.join().unwrap().starts_with(b"GET /status HTTP/1.0\r\n"));
        assert!(is_json(r.content_type.as_deref()));
        assert_eq!(r.body, "{\"schema\":\"weir.status/v1\"}");
    }

    #[test]
    fn parse_tolerates_a_missing_head_and_odd_header_case() {
        let r = parse_http_response("weir_queue_depth 1\n");
        assert_eq!(
            r,
            HttpResponse {
                status: None,
                content_type: None,
                body: "weir_queue_depth 1\n".into()
            }
        );
        let r = parse_http_response(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-TYPE: text/plain\r\n\r\nnot ready: starting\n",
        );
        assert_eq!(r.status, Some(503));
        assert_eq!(r.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn connect_refused_is_an_error_naming_the_address() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        drop(l);
        let e = http_get(&addr, "/status").unwrap_err();
        assert!(e.starts_with(&format!("connect {addr}")), "{e}");
    }
}
