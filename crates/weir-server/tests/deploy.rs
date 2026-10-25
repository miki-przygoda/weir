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
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
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

// ── weir-readiness.sh ─────────────────────────────────────────────────────────

const READINESS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/systemd/weir-readiness.sh"
);

/// Runs the shipped readiness script against `127.0.0.1:<port>` with no
/// weir-ctl (`--ctl` points nowhere; a later `--ctl` in `extra` wins). Returns
/// the exit code and stdout+stderr.
fn readiness(port: u16, extra: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    assert!(
        Command::new("curl")
            .arg("--version")
            .stdout(Stdio::null())
            .status()
            .is_ok_and(|s| s.success()),
        "these tests need curl on PATH: it is the script's documented dependency"
    );
    let mut cmd = Command::new("bash");
    cmd.arg(READINESS)
        .arg("--addr")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--ctl")
        .arg("/nonexistent/weir-ctl")
        .args(extra);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let (status, out) = run_with_deadline(&mut cmd, Duration::from_secs(20));
    let status = status.unwrap_or_else(|| panic!("weir-readiness.sh ran past 20 s:\n{out}"));
    (
        status
            .code()
            .expect("weir-readiness.sh was killed by a signal"),
        out,
    )
}

/// A metrics body with `active` as `(code, severity)` conditions at 1, one
/// inactive condition at 0, and a sink type.
fn exposition(active: &[(&str, &str)]) -> String {
    let mut s = String::from("# TYPE weir_condition_active gauge\n");
    for (code, severity) in active {
        s.push_str(&format!(
            "weir_condition_active{{code=\"{code}\",severity=\"{severity}\"}} 1\n"
        ));
    }
    s.push_str("weir_condition_active{code=\"starting\",severity=\"info\"} 0\n");
    s.push_str("# TYPE weir_sink_info gauge\nweir_sink_info{sink_type=\"http\"} 1\n# EOF\n");
    s
}

/// A daemon whose /readyz says ok, with `active` conditions on /metrics.
fn ready_daemon(active: &[(&str, &str)]) -> Stub {
    Stub::start(vec![
        route("/readyz", 200, TEXT, "ok\n"),
        route("/metrics", 200, OPENMETRICS, exposition(active)),
    ])
}

#[test]
fn readiness_is_ready_when_readyz_answers_200() {
    let stub = ready_daemon(&[]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("READY: /readyz ok, sink=http"), "{out}");
}

#[test]
fn readiness_is_not_ready_exactly_when_readyz_answers_503() {
    let stub = Stub::start(vec![
        route(
            "/readyz",
            503,
            TEXT,
            "not ready: wab_cap_rejecting, no_live_shard\n",
        ),
        route("/metrics", 200, OPENMETRICS, exposition(&[])),
    ]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("NOT READY: wab_cap_rejecting, no_live_shard"),
        "{out}"
    );
}

/// The behaviour change: sink, drain and history conditions are reported, but
/// a daemon that can take ingest is READY. The old script exited 1 on each of
/// these, and stayed at 1 forever after one recovered flusher panic.
#[test]
fn a_sink_outage_is_a_warning_not_a_readiness_failure() {
    let stub = ready_daemon(&[
        ("sink_down", "warning"),
        ("drain_blocked_dead_letter_full", "critical"),
        ("flusher_respawned_recent", "warning"),
    ]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    for c in [
        "sink_down",
        "drain_blocked_dead_letter_full",
        "flusher_respawned_recent",
    ] {
        assert!(
            out.contains(&format!("WARN: {c} (")) && out.contains(&format!("#cond-{c}")),
            "no WARN line with a runbook link for {c}:\n{out}"
        );
    }
}

#[test]
fn a_critical_condition_turns_ready_into_exit_3_only_on_request() {
    let stub = ready_daemon(&[("shard_offline", "critical")]);
    assert_eq!(readiness(stub.port, &[], &[]).0, 0);
    let (code, out) = readiness(stub.port, &["--fail-on-critical"], &[]);
    assert_eq!(code, 3, "{out}");

    let warning_only = ready_daemon(&[("sink_down", "warning")]);
    let (code, out) = readiness(warning_only.port, &["--fail-on-critical"], &[]);
    assert_eq!(code, 0, "a warning must never yield exit 3:\n{out}");
}

#[test]
fn allow_noop_silences_only_the_noop_warning() {
    let stub = ready_daemon(&[("noop_sink", "warning"), ("sink_down", "warning")]);
    let (_, loud) = readiness(stub.port, &[], &[]);
    assert!(loud.contains("WARN: noop_sink"), "{loud}");
    let (code, quiet) = readiness(stub.port, &["--allow-noop"], &[]);
    assert_eq!(code, 0, "{quiet}");
    assert!(!quiet.contains("noop_sink"), "{quiet}");
    assert!(quiet.contains("WARN: sink_down"), "{quiet}");
}

#[test]
fn readiness_is_dead_when_nothing_listens() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        l.local_addr().expect("addr").port()
    };
    let (code, out) = readiness(port, &[], &[]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("DEAD"), "{out}");
}

#[test]
fn readiness_is_dead_for_an_http_server_that_is_not_weir() {
    let html = Stub::start(vec![route("*", 200, "text/html", "<html>hello</html>")]);
    let (code, out) = readiness(html.port, &[], &[]);
    assert_eq!(code, 2, "{out}");

    // text/plain, but not a /readyz body.
    let plain = Stub::start(vec![route("*", 200, TEXT, "hello\n")]);
    let (code, out) = readiness(plain.port, &[], &[]);
    assert_eq!(code, 2, "{out}");
}

#[test]
fn a_daemon_without_readyz_counts_as_ready_when_it_answers_with_weir_metrics() {
    // Before the health routes, every path returned the metrics.
    let older = Stub::start(vec![route("*", 200, OPENMETRICS, exposition(&[]))]);
    let (code, out) = readiness(older.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("has no /readyz"), "{out}");

    let not_weir = Stub::start(vec![route(
        "*",
        200,
        OPENMETRICS,
        "# TYPE up gauge\nup 1\n# EOF\n",
    )]);
    let (code, out) = readiness(not_weir.port, &[], &[]);
    assert_eq!(code, 2, "{out}");
}

/// A fake `weir-ctl` that echoes its argv and then prints `$STUB_OUT` and
/// exits `$STUB_RC` for `status`. `has_status: false` imitates a weir-ctl
/// from before the subcommand existed: clap exits 2 on `status --help`.
fn stub_ctl(tag: &str, has_status: bool) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("weir-deploy-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("stub dir");
    let path = dir.join("weir-ctl");
    let help_rc = if has_status { 0 } else { 2 };
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = status ] && [ \"$2\" = --help ]; then exit {help_rc}; fi\n\
             if [ \"$1\" = status ]; then echo \"args: $*\"; printf '%s\\n' \"$STUB_OUT\"; exit \"$STUB_RC\"; fi\n\
             exit 64\n"
        ),
    )
    .expect("write stub weir-ctl");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

#[test]
fn weir_ctl_status_decides_the_verdict_when_installed() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("verdict", true);
    let ctl = ctl.to_str().expect("utf-8 temp path");
    let addr = format!("127.0.0.1:{}", stub.port);
    // (weir-ctl exit, script exit, script exit with --fail-on-critical)
    for (rc, plain, strict) in [
        ("0", 0, 0),
        ("1", 1, 1),
        ("2", 2, 2),
        ("3", 0, 3),
        ("4", 2, 2),
    ] {
        let env = [("STUB_RC", rc), ("STUB_OUT", "verdict from weir-ctl")];
        let (code, out) = readiness(stub.port, &["--ctl", ctl], &env);
        assert_eq!(code, plain, "weir-ctl exited {rc}:\n{out}");
        assert!(
            out.contains(&format!("args: status --addr {addr}")),
            "{out}"
        );
        assert!(
            out.contains("verdict from weir-ctl"),
            "weir-ctl's output was not relayed:\n{out}"
        );
        let (code, out) = readiness(stub.port, &["--ctl", ctl, "--fail-on-critical"], &env);
        assert_eq!(
            code, strict,
            "weir-ctl exited {rc}, --fail-on-critical:\n{out}"
        );
    }
}

#[test]
fn weir_ctl_gets_the_admin_socket_and_wab_dir_it_was_given() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("args", true);
    let (code, out) = readiness(
        stub.port,
        &[
            "--ctl",
            ctl.to_str().unwrap(),
            "--admin-socket",
            "/x/wab.admin.sock",
            "--wab-dir",
            "/x/wab",
        ],
        &[("STUB_RC", "0"), ("STUB_OUT", "READY")],
    );
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains(&format!(
            "args: status --addr 127.0.0.1:{} --admin-socket /x/wab.admin.sock --wab-dir /x/wab",
            stub.port
        )),
        "{out}"
    );
}

#[test]
fn allow_noop_filters_weir_ctl_output_too() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("noop", true);
    let env = [
        ("STUB_RC", "0"),
        (
            "STUB_OUT",
            "READY\nwarning noop_sink: records are acked and DISCARDED",
        ),
    ];
    let (code, out) = readiness(
        stub.port,
        &["--ctl", ctl.to_str().unwrap(), "--allow-noop"],
        &env,
    );
    assert_eq!(code, 0, "{out}");
    // The /readyz path also prints a READY line, so pin that this one is
    // weir-ctl's output, relayed and filtered.
    assert!(
        out.contains("args: status"),
        "weir-ctl was not consulted:\n{out}"
    );
    assert!(!out.contains("noop_sink"), "{out}");
    assert!(out.lines().any(|l| l == "READY"), "{out}");
}

#[test]
fn an_unreachable_daemon_is_dead_without_asking_weir_ctl() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        l.local_addr().expect("addr").port()
    };
    let ctl = stub_ctl("dead", true);
    // A weir-ctl that would answer "not ready" from disk must not turn a dead
    // daemon into exit 1.
    let (code, out) = readiness(
        port,
        &["--ctl", ctl.to_str().unwrap()],
        &[
            ("STUB_RC", "1"),
            ("STUB_OUT", "NOT READY: daemon_unreachable"),
        ],
    );
    assert_eq!(code, 2, "{out}");
    assert!(
        !out.contains("args: status"),
        "weir-ctl was consulted for a dead daemon:\n{out}"
    );
}

#[test]
fn an_older_weir_ctl_without_status_falls_back_to_readyz() {
    let stub = Stub::start(vec![
        route("/readyz", 503, TEXT, "not ready: starting\n"),
        route("/metrics", 200, OPENMETRICS, exposition(&[])),
    ]);
    let ctl = stub_ctl("old", false);
    let (code, out) = readiness(
        stub.port,
        &["--ctl", ctl.to_str().unwrap()],
        &[("STUB_RC", "0"), ("STUB_OUT", "unused")],
    );
    assert_eq!(code, 1, "the verdict must come from /readyz:\n{out}");
    assert!(out.contains("NOT READY: starting"), "{out}");
}

#[test]
fn the_readiness_script_reports_a_live_daemon_ready() {
    let srv = weir_server!("readiness_live").start();
    // wait_ready returns once the socket accepts; `serving` is stored a moment later.
    let deadline = Instant::now() + Duration::from_secs(5);
    let (code, out) = loop {
        let (code, out) = readiness(srv.metrics_port, &[], &[]);
        if code == 0 || Instant::now() >= deadline {
            break (code, out);
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("READY: /readyz ok"), "{out}");
}
