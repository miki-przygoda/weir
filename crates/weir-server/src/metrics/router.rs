//! Routing and rendering for the metrics/health listener (spec §5.8, §10).
//!
//! Pure: `route` maps a request head to a [`Route`], and the `render_*`
//! functions map a probe answer to a `Response`. No I/O, no clock, no
//! lock. `server.rs` does the socket work, and a `StatusSource` does the
//! board loads.

/// The four reserved, read-only routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Endpoint {
    Livez,
    Readyz,
    Startupz,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// Every other path, and every head that does not parse: today's metrics
    /// response, byte for byte (weir-ctl and every scrape config rely on it).
    Metrics,
    Probe {
        endpoint: Endpoint,
        head_only: bool,
        verbose: bool,
    },
    /// A reserved path with a method other than GET or HEAD: 405.
    MethodNotAllowed,
}

/// Routes on the request line only: `METHOD SP target SP HTTP/x`, the query
/// stripped, the path matched exactly and case-sensitively.
pub(crate) fn route(head: &[u8]) -> Route {
    let line_end = head
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(head.len());
    let Ok(line) = std::str::from_utf8(&head[..line_end]) else {
        return Route::Metrics;
    };
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Route::Metrics;
    };
    if method.is_empty() || !version.starts_with("HTTP/") {
        return Route::Metrics;
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (target, None),
    };
    let endpoint = match path {
        "/livez" => Endpoint::Livez,
        "/readyz" => Endpoint::Readyz,
        "/startupz" => Endpoint::Startupz,
        "/status" => Endpoint::Status,
        _ => return Route::Metrics,
    };
    let head_only = match method {
        "GET" => false,
        "HEAD" => true,
        _ => return Route::MethodNotAllowed,
    };
    let verbose = query.is_some_and(|q| {
        q.split('&')
            .any(|kv| kv.split_once('=').map_or(kv, |(k, _)| k) == "verbose")
    });
    Route::Probe {
        endpoint,
        head_only,
        verbose,
    }
}

use std::fmt::Write as _;
use std::time::SystemTime;

use weir_status::{ConditionCode, LivezCheck, ProbeVerdict, PublicStatusDoc, Thresholds};

/// What `/startupz` prints while startup is not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartupFacts {
    pub phase: &'static str,
    pub phase_age_secs: u64,
    pub replay_remaining: Option<u64>,
}

/// One probe's verdict plus the public summaries of its failing codes.
#[derive(Debug, Clone)]
pub(crate) struct ProbeAnswer {
    pub verdict: ProbeVerdict,
    /// Public, template-rendered summaries (spec §9.1), only for failing codes.
    pub summaries: Vec<(ConditionCode, String)>,
    pub startup: StartupFacts,
}

/// Where the router reads status from. Implemented by `StatusBoard`
/// (Task 5.9). Implementations do loads only: no lock, no I/O, no await
/// (spec §9.2 items 4 and 8), so a probe never waits on the pipeline.
pub(crate) trait StatusSource: Send + Sync + 'static {
    fn probe(
        &self,
        endpoint: Endpoint,
        now: SystemTime,
        checks: &[LivezCheck],
        thresholds: &Thresholds,
    ) -> ProbeAnswer;
    fn public_doc(
        &self,
        now: SystemTime,
        checks: &[LivezCheck],
        thresholds: &Thresholds,
    ) -> PublicStatusDoc;
}

/// A route response. The metrics response never goes through here: its
/// head stays byte-identical to today's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Response {
    pub status: u16,
    pub reason: &'static str,
    pub content_type: &'static str,
    /// Adds `Allow: GET, HEAD` (405 only).
    pub allow: bool,
    pub body: Vec<u8>,
}

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json; charset=utf-8";

impl Response {
    fn text(status: u16, reason: &'static str, body: String) -> Self {
        Self {
            status,
            reason,
            content_type: TEXT,
            allow: false,
            body: body.into_bytes(),
        }
    }

    /// Route responses carry `Cache-Control: no-store` and `Connection:
    /// close` (spec §10). A HEAD request gets the GET head, Content-Length
    /// included, and no body.
    pub(crate) fn encode(&self, head_only: bool) -> Vec<u8> {
        let mut head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
            self.status,
            self.reason,
            self.content_type,
            self.body.len()
        );
        if self.allow {
            head.push_str("Allow: GET, HEAD\r\n");
        }
        head.push_str("\r\n");
        let mut out = head.into_bytes();
        if !head_only {
            out.extend_from_slice(&self.body);
        }
        out
    }
}

fn join_codes(codes: &[ConditionCode], sep: &str) -> String {
    codes
        .iter()
        .map(|c| c.as_str())
        .collect::<Vec<_>>()
        .join(sep)
}

/// `[+]<code> ok` / `[-]<code> failed: <public summary>`, one line per check.
/// Control characters in a summary become spaces, so a summary can never
/// forge a second probe line.
fn verbose_lines(a: &ProbeAnswer) -> String {
    let mut s = String::new();
    for check in &a.verdict.checks {
        if check.ok {
            let _ = writeln!(s, "[+]{} ok", check.code.as_str());
        } else {
            let summary = a
                .summaries
                .iter()
                .find(|(c, _)| *c == check.code)
                .map(|(_, s)| s.replace(|ch: char| ch.is_control(), " "))
                .unwrap_or_default();
            let _ = writeln!(s, "[-]{} failed: {summary}", check.code.as_str());
        }
    }
    s
}

fn render_probe(a: &ProbeAnswer, verbose: bool, failed: &str, sep: &str) -> Response {
    let mut body = if verbose {
        verbose_lines(a)
    } else {
        String::new()
    };
    if a.verdict.ok {
        body.push_str("ok\n");
        Response::text(200, "OK", body)
    } else {
        let _ = writeln!(body, "{failed}: {}", join_codes(&a.verdict.failing, sep));
        Response::text(503, "Service Unavailable", body)
    }
}

/// `200 ok` or `503 not live: <code>[,<code>]` (spec §10, separator as
/// written there; Decision Q12).
pub(crate) fn render_livez(a: &ProbeAnswer, verbose: bool) -> Response {
    render_probe(a, verbose, "not live", ",")
}

/// `200 ok` or `503 not ready: <code>[, <code>]` (spec §10).
pub(crate) fn render_readyz(a: &ProbeAnswer, verbose: bool) -> Response {
    render_probe(a, verbose, "not ready", ", ")
}

/// `200 ok` once the daemon has ever served (sticky), else
/// `503 starting: phase=<p> since=<s>s replay=<n>`.
pub(crate) fn render_startupz(a: &ProbeAnswer) -> Response {
    if a.verdict.ok {
        return Response::text(200, "OK", "ok\n".into());
    }
    let replay = a
        .startup
        .replay_remaining
        .map_or_else(|| "unknown".to_string(), |n| n.to_string());
    Response::text(
        503,
        "Service Unavailable",
        format!(
            "starting: phase={} since={}s replay={replay}\n",
            a.startup.phase, a.startup.phase_age_secs
        ),
    )
}

/// Always 200 with the public document; 500 with an empty body if it
/// cannot be serialized (spec §10).
pub(crate) fn render_status(doc: &PublicStatusDoc) -> Response {
    match serde_json::to_vec(doc) {
        Ok(body) => Response {
            status: 200,
            reason: "OK",
            content_type: JSON,
            allow: false,
            body,
        },
        Err(_) => Response {
            status: 500,
            reason: "Internal Server Error",
            content_type: TEXT,
            allow: false,
            body: Vec::new(),
        },
    }
}

/// A reserved path with a method other than GET or HEAD (spec §10).
pub(crate) fn render_method_not_allowed() -> Response {
    Response {
        status: 405,
        reason: "Method Not Allowed",
        content_type: TEXT,
        allow: true,
        body: b"method not allowed\n".to_vec(),
    }
}

/// One probe or status request, start to finish. Synchronous: loads, a pure
/// evaluation and one serialization, bounded at 256 shards and 40 conditions.
pub(crate) fn respond(
    source: &dyn StatusSource,
    endpoint: Endpoint,
    verbose: bool,
    now: SystemTime,
    checks: &[LivezCheck],
    thresholds: &Thresholds,
) -> Response {
    match endpoint {
        Endpoint::Livez => render_livez(&source.probe(endpoint, now, checks, thresholds), verbose),
        Endpoint::Readyz => {
            render_readyz(&source.probe(endpoint, now, checks, thresholds), verbose)
        }
        Endpoint::Startupz => render_startupz(&source.probe(endpoint, now, checks, thresholds)),
        Endpoint::Status => render_status(&source.public_doc(now, checks, thresholds)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(endpoint: Endpoint, head_only: bool, verbose: bool) -> Route {
        Route::Probe {
            endpoint,
            head_only,
            verbose,
        }
    }

    #[test]
    fn routing_table() {
        use Endpoint::*;
        let cases: &[(&[u8], Route)] = &[
            // The four reserved paths, GET and HEAD.
            (
                b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n",
                probe(Livez, false, false),
            ),
            (b"GET /readyz HTTP/1.0\r\n\r\n", probe(Readyz, false, false)),
            (
                b"GET /startupz HTTP/1.1\r\n\r\n",
                probe(Startupz, false, false),
            ),
            (b"GET /status HTTP/1.1\r\n\r\n", probe(Status, false, false)),
            (b"HEAD /readyz HTTP/1.1\r\n\r\n", probe(Readyz, true, false)),
            // Query stripped; `verbose` recognised as a bare key or key=value.
            (
                b"GET /livez?verbose HTTP/1.1\r\n\r\n",
                probe(Livez, false, true),
            ),
            (
                b"GET /readyz?a=1&verbose=1 HTTP/1.1\r\n\r\n",
                probe(Readyz, false, true),
            ),
            (
                b"GET /readyz?verbosely HTTP/1.1\r\n\r\n",
                probe(Readyz, false, false),
            ),
            (
                b"GET /status?x=y HTTP/1.1\r\n\r\n",
                probe(Status, false, false),
            ),
            // Other methods on a reserved path.
            (b"POST /readyz HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
            (b"get /readyz HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
            (b"DELETE /status HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
            // Exact, case-sensitive, no trailing-slash folding: these are metrics.
            (b"GET /READYZ HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET /readyz/ HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET //readyz HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET http://h/readyz HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET /metrics HTTP/1.0\r\n\r\n", Route::Metrics),
            (b"GET / HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET /foo HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"POST /metrics HTTP/1.1\r\n\r\n", Route::Metrics),
            // Unparseable request lines get metrics, as today.
            (b"", Route::Metrics),
            (b"GARBAGE not http", Route::Metrics),
            (b"GET /readyz\r\n\r\n", Route::Metrics),
            (b"GET  /readyz HTTP/1.1\r\n\r\n", Route::Metrics),
            (b"GET /readyz HTTP/1.1 extra\r\n\r\n", Route::Metrics),
            (b"GET /readyz FTP/1.1\r\n\r\n", Route::Metrics),
            (b"\xff\xfe /readyz HTTP/1.1\r\n\r\n", Route::Metrics),
        ];
        for (head, want) in cases {
            assert_eq!(
                route(head),
                *want,
                "head {:?}",
                String::from_utf8_lossy(head)
            );
        }
    }

    use weir_status::{ConditionCode, ProbeCheck, ProbeVerdict};

    fn answer(
        checks: &[(ConditionCode, bool)],
        summaries: &[(ConditionCode, &str)],
    ) -> ProbeAnswer {
        let failing = checks
            .iter()
            .filter(|(_, ok)| !ok)
            .map(|(c, _)| *c)
            .collect::<Vec<_>>();
        ProbeAnswer {
            verdict: ProbeVerdict {
                ok: failing.is_empty(),
                failing,
                checks: checks
                    .iter()
                    .map(|(code, ok)| ProbeCheck {
                        code: *code,
                        ok: *ok,
                        summary: None,
                    })
                    .collect(),
            },
            summaries: summaries.iter().map(|(c, s)| (*c, s.to_string())).collect(),
            startup: StartupFacts {
                phase: "recovering",
                phase_age_secs: 12,
                replay_remaining: Some(40),
            },
        }
    }

    fn text(r: &Response) -> &str {
        std::str::from_utf8(&r.body).unwrap()
    }

    #[test]
    fn livez_bodies() {
        let ok = answer(&[], &[]);
        assert_eq!(
            (
                render_livez(&ok, false).status,
                text(&render_livez(&ok, false))
            ),
            (200, "ok\n")
        );
        let bad = answer(
            &[
                (ConditionCode::NoLiveShard, false),
                (ConditionCode::ListenerDown, false),
                (ConditionCode::WorkerExited, true),
            ],
            &[
                (
                    ConditionCode::NoLiveShard,
                    "every shard's flusher or owning worker is offline",
                ),
                (
                    ConditionCode::ListenerDown,
                    "unix listener is not accepting; restart re-binds it",
                ),
            ],
        );
        let r = render_livez(&bad, false);
        assert_eq!(
            (r.status, text(&r)),
            (503, "not live: no_live_shard,listener_down\n")
        );
        let r = render_livez(&bad, true);
        assert_eq!(
            text(&r),
            "[-]no_live_shard failed: every shard's flusher or owning worker is offline\n\
             [-]listener_down failed: unix listener is not accepting; restart re-binds it\n\
             [+]worker_exited ok\n\
             not live: no_live_shard,listener_down\n"
        );
    }

    #[test]
    fn readyz_bodies() {
        let bad = answer(
            &[
                (ConditionCode::Starting, true),
                (ConditionCode::ShuttingDown, false),
                (ConditionCode::WabCapRejecting, false),
            ],
            &[
                (
                    ConditionCode::ShuttingDown,
                    "graceful shutdown; new connections refused",
                ),
                (
                    ConditionCode::WabCapRejecting,
                    "WAB holds 9 of the 8 cap\nINJECTED",
                ),
            ],
        );
        let r = render_readyz(&bad, false);
        assert_eq!(
            (r.status, text(&r)),
            (503, "not ready: shutting_down, wab_cap_rejecting\n")
        );
        let v = render_readyz(&bad, true);
        // A control character in a summary cannot forge a probe line.
        assert!(
            text(&v).contains("[-]wab_cap_rejecting failed: WAB holds 9 of the 8 cap INJECTED\n"),
            "{}",
            text(&v)
        );
        assert!(text(&v).starts_with("[+]starting ok\n"));
        assert_eq!(text(&render_readyz(&answer(&[], &[]), false)), "ok\n");
    }

    #[test]
    fn startupz_bodies() {
        let starting = answer(&[(ConditionCode::Starting, false)], &[]);
        let r = render_startupz(&starting);
        assert_eq!(
            (r.status, text(&r)),
            (503, "starting: phase=recovering since=12s replay=40\n")
        );
        let mut unknown = starting.clone();
        unknown.startup.replay_remaining = None;
        assert_eq!(
            text(&render_startupz(&unknown)),
            "starting: phase=recovering since=12s replay=unknown\n"
        );
        assert_eq!(text(&render_startupz(&answer(&[], &[]))), "ok\n");
    }

    #[test]
    fn encode_heads_get_head_and_405() {
        let r = render_livez(&answer(&[], &[]), false);
        assert_eq!(
            r.encode(false),
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 3\r\n\
              Cache-Control: no-store\r\nConnection: close\r\n\r\nok\n"
                .to_vec()
        );
        // HEAD: the GET head, Content-Length included, no body.
        assert!(r.encode(true).ends_with(
            b"Content-Length: 3\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"
        ));
        let m = render_method_not_allowed().encode(false);
        let m = String::from_utf8(m).unwrap();
        assert!(m.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"), "{m}");
        assert!(m.contains("\r\nAllow: GET, HEAD\r\n"), "{m}");
        assert!(m.contains("\r\nCache-Control: no-store\r\n"), "{m}");
    }

    /// A fixed answer and document, so `respond`'s dispatch is tested
    /// without a wired board (the board glue has its own tests, Task 5.9).
    struct Fixed {
        answer: ProbeAnswer,
        doc: PublicStatusDoc,
    }

    impl StatusSource for Fixed {
        fn probe(
            &self,
            _endpoint: Endpoint,
            _now: SystemTime,
            checks: &[LivezCheck],
            thresholds: &Thresholds,
        ) -> ProbeAnswer {
            assert_eq!(checks, &[LivezCheck::NoLiveShard], "checks pass through");
            assert_eq!(*thresholds, Thresholds::DEFAULT, "thresholds pass through");
            self.answer.clone()
        }
        fn public_doc(
            &self,
            _now: SystemTime,
            checks: &[LivezCheck],
            _thresholds: &Thresholds,
        ) -> PublicStatusDoc {
            assert_eq!(checks, &[LivezCheck::NoLiveShard], "checks pass through");
            self.doc.clone()
        }
    }

    #[test]
    fn respond_sends_each_endpoint_to_its_renderer() {
        let board = crate::status::StatusBoard::for_test();
        let checks = [LivezCheck::NoLiveShard];
        let doc = weir_status::public(&crate::status::render::render_admin_doc(
            &board,
            &Thresholds::DEFAULT,
            &checks,
            SystemTime::now(),
        ));
        let fixed = Fixed {
            answer: answer(
                &[(ConditionCode::ShuttingDown, false)],
                &[(ConditionCode::ShuttingDown, "graceful shutdown")],
            ),
            doc: doc.clone(),
        };
        let call = |endpoint, verbose| {
            respond(
                &fixed,
                endpoint,
                verbose,
                SystemTime::now(),
                &checks,
                &Thresholds::DEFAULT,
            )
        };
        assert_eq!(
            call(Endpoint::Livez, true),
            render_livez(&fixed.answer, true)
        );
        assert_eq!(
            call(Endpoint::Readyz, false),
            render_readyz(&fixed.answer, false)
        );
        assert_eq!(
            call(Endpoint::Startupz, true),
            render_startupz(&fixed.answer)
        );
        let status = call(Endpoint::Status, false);
        assert_eq!(
            (status.status, status.content_type),
            (200, "application/json; charset=utf-8")
        );
        let back: PublicStatusDoc = serde_json::from_slice(&status.body).unwrap();
        assert_eq!(back, doc);
        let head = String::from_utf8(status.encode(true)).unwrap();
        assert!(
            head.starts_with(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
            ),
            "{head}"
        );
    }
}
