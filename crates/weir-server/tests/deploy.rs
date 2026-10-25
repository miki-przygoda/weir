//! The deploy artefacts, executed: the container HEALTHCHECK command and
//! `deploy/systemd/weir-readiness.sh`.
//!
//! Both are shell, so nothing in the Rust build ever ran them, and both
//! shipped with semantics nobody had executed. The HEALTHCHECK passed for
//! anything that accepted a TCP connection on 9185. The readiness script
//! stayed NOT READY forever after one recovered flusher panic, because it
//! gated on lifetime counters. Every test here runs the shipped text, read
//! from the file operators install, never a copy of it.
//!
//! Needs `bash`, and for the readiness script `curl` and `awk` (the script's
//! documented dependencies).

#![cfg(unix)]

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use weir_testkit::weir_server;

const DOCKERFILE: &str = include_str!("../../../deploy/docker/Dockerfile");
const TEXT: &str = "text/plain; charset=utf-8";
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

// ── A canned HTTP server ──────────────────────────────────────────────────────

#[derive(Clone)]
struct Route {
    path: &'static str,
    status: u16,
    content_type: &'static str,
    body: String,
}

/// `path` matches exactly after the query is stripped; `"*"` matches any path.
fn route(
    path: &'static str,
    status: u16,
    content_type: &'static str,
    body: impl Into<String>,
) -> Route {
    Route {
        path,
        status,
        content_type,
        body: body.into(),
    }
}

/// A loopback HTTP/1.x server answering from a fixed route table, one
/// connection at a time, closing after each response. `None` routes means
/// accept and close at once without writing a byte.
struct Stub {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Stub {
    fn start(routes: Vec<Route>) -> Stub {
        Stub::spawn(Some(routes))
    }

    /// Accepts every connection and closes it without a response: a port that
    /// is open but is not an HTTP server.
    fn silent() -> Stub {
        Stub::spawn(None)
    }

    fn spawn(routes: Option<Vec<Route>>) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the stub");
        listener.set_nonblocking(true).expect("non-blocking accept");
        let port = listener.local_addr().expect("stub address").port();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => match &routes {
                        Some(table) => serve_one(stream, table),
                        None => drop(stream),
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => break,
                }
            }
        });
        Stub {
            port,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn serve_one(mut stream: TcpStream, routes: &[Route]) {
    // An accepted socket inherits O_NONBLOCK from the listener on macOS.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < 8192 {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let text = String::from_utf8_lossy(&head);
    let target = text.split_whitespace().nth(1).unwrap_or("/");
    let path = target.split('?').next().unwrap_or(target);
    let hit = routes
        .iter()
        .find(|r| r.path == path)
        .or_else(|| routes.iter().find(|r| r.path == "*"));
    let (status, ct, body) = match hit {
        Some(r) => (r.status, r.content_type, r.body.as_str()),
        None => (404, TEXT, "not found\n"),
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

/// Runs `cmd`, killing it at `deadline`. Returns its status (`None` when it
/// had to be killed) and its stdout followed by its stderr.
fn run_with_deadline(cmd: &mut Command, deadline: Duration) -> (Option<ExitStatus>, String) {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let end = Instant::now() + deadline;
    let status = loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            break Some(s);
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut out = String::new();
    if let Some(mut o) = child.stdout.take() {
        let _ = o.read_to_string(&mut out);
    }
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut out);
    }
    (status, out)
}

// ── The image HEALTHCHECK ─────────────────────────────────────────────────────

/// Runs the Dockerfile's HEALTHCHECK command, as shipped, against `port`
/// instead of 9185. A check that hangs past Docker's own `--timeout=3s`
/// counts as failed, as it does in Docker.
fn image_healthcheck(port: u16) -> (bool, String) {
    let from = DOCKERFILE
        .find("\nHEALTHCHECK ")
        .expect("the Dockerfile has a HEALTHCHECK instruction");
    let marker = "\"/bin/bash\", \"-c\", \"";
    let start = DOCKERFILE[from..]
        .find(marker)
        .expect("the HEALTHCHECK is the exec form [\"CMD\", \"/bin/bash\", \"-c\", ...]")
        + from
        + marker.len();
    let end = DOCKERFILE[start..]
        .find("\"]")
        .expect("unterminated HEALTHCHECK")
        + start;
    // JSON string escapes: `\\r` in the Dockerfile is `\r` for bash's printf.
    let cmd = DOCKERFILE[start..end]
        .replace("\\\\", "\\")
        .replace("/127.0.0.1/9185", &format!("/127.0.0.1/{port}"));
    let (status, out) = run_with_deadline(
        Command::new("bash").arg("-c").arg(cmd),
        Duration::from_secs(3),
    );
    (status.is_some_and(|s| s.success()), out)
}

#[test]
fn the_image_healthcheck_passes_against_a_live_daemon() {
    let srv = weir_server!("hc_livez").start();
    let (ok, out) = image_healthcheck(srv.metrics_port);
    assert!(
        ok,
        "the Dockerfile HEALTHCHECK failed against a healthy daemon:\n{out}"
    );
}

#[test]
fn the_image_healthcheck_fails_when_livez_says_not_live() {
    let stub = Stub::start(vec![
        route("/livez", 503, TEXT, "not live: no_live_shard\n"),
        route("*", 200, OPENMETRICS, "# EOF\n"),
    ]);
    let (ok, out) = image_healthcheck(stub.port);
    assert!(
        !ok,
        "the HEALTHCHECK passed although /livez answered 503:\n{out}"
    );
}

#[test]
fn the_image_healthcheck_fails_against_a_port_that_only_accepts() {
    let stub = Stub::silent();
    let (ok, out) = image_healthcheck(stub.port);
    assert!(
        !ok,
        "the HEALTHCHECK passed against a port that accepts and says nothing; \n\
         that is exactly what the TCP-connect check it replaced could not tell:\n{out}"
    );
}
