//! Routing and rendering for the metrics/health listener (spec §5.8, §10).
//!
//! Pure: `route` maps a request head to a [`Route`], and the `render_*`
//! functions map a probe answer to a `Response`. No I/O, no clock, no
//! lock. `server.rs` does the socket work, and a `StatusSource` does the
//! board loads.
#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "wired into metrics::server in Task 5.8 and status::http in Task 5.9"
    )
)]

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
}
