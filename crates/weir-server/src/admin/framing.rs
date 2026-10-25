//! JSON-lines framing for the admin socket (spec §11 "Framing"): one UTF-8
//! JSON object per `\n` each way, answered in order. The 64 KiB line limit
//! is enforced WHILE reading, so an oversized line is refused without being
//! buffered. Deadlines: 10 s idle before a request's first byte, 5 s from
//! that byte to the newline, 5 s per write. At most 64 requests per
//! connection.

use std::{future::Future, io, time::Duration};

use serde_json::{Map, Value};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::timeout,
};
use weir_status::admin::{
    AdminError, ErrorCode, InstanceId, MAX_LINE_BYTES, MAX_WAIT_MS, Response, Verb,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) max_line_bytes: usize,
    pub(crate) request_deadline: Duration,
    pub(crate) idle_timeout: Duration,
    pub(crate) write_timeout: Duration,
    pub(crate) max_requests: usize,
}

impl Limits {
    pub(crate) const PRODUCTION: Limits = Limits {
        max_line_bytes: MAX_LINE_BYTES,
        request_deadline: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(5),
        max_requests: 64,
    };
}

/// How long a refused oversized connection's input is drained after the
/// error is written, so the close is a clean EOF rather than a reset.
const DRAIN_ON_CLOSE: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LineRead {
    Line,
    Eof,
    TooLarge,
}

/// Appends bytes up to (not including) the next `\n` to `line`, consuming
/// the `\n`. Returns `TooLarge` as soon as the line would exceed `max`,
/// having buffered at most `max` bytes.
pub(crate) async fn read_line_bounded<R: AsyncBufRead + Unpin>(
    r: &mut R,
    line: &mut Vec<u8>,
    max: usize,
) -> io::Result<LineRead> {
    loop {
        let buf = r.fill_buf().await?;
        if buf.is_empty() {
            return Ok(LineRead::Eof);
        }
        match buf.iter().position(|&b| b == b'\n') {
            Some(i) => {
                if line.len() + i > max {
                    return Ok(LineRead::TooLarge);
                }
                line.extend_from_slice(&buf[..i]);
                r.consume(i + 1);
                return Ok(LineRead::Line);
            }
            None => {
                let n = buf.len();
                if line.len() + n > max {
                    return Ok(LineRead::TooLarge);
                }
                line.extend_from_slice(buf);
                r.consume(n);
            }
        }
    }
}

/// What a connection hands each complete request line to.
pub(crate) trait Dispatch {
    /// Answers one line. Never fails: every problem is an error `Response`.
    fn dispatch(&self, line: &[u8], peer_uid: u32) -> impl Future<Output = Response> + Send;
    /// Identity for framing-level errors (deadline, oversized line).
    fn instance(&self) -> Option<InstanceId>;
    /// Called once per framing-level error, for metrics.
    fn on_framing_error(&self, _code: ErrorCode) {}
}

pub(crate) async fn serve_connection<D: Dispatch + Sync>(
    stream: UnixStream,
    peer_uid: u32,
    d: &D,
    limits: &Limits,
) {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::with_capacity(8 * 1024, rd);
    let mut line = Vec::with_capacity(1024);
    for _ in 0..limits.max_requests {
        // Idle: wait for the next request's first byte.
        match timeout(limits.idle_timeout, rd.fill_buf()).await {
            Ok(Ok(b)) if !b.is_empty() => {}
            _ => return, // idle timeout, EOF or a read error
        }
        line.clear();
        let read = timeout(
            limits.request_deadline,
            read_line_bounded(&mut rd, &mut line, limits.max_line_bytes),
        )
        .await;
        let resp = match read {
            Ok(Ok(LineRead::Line)) => d.dispatch(&line, peer_uid).await,
            Ok(Ok(LineRead::Eof)) | Ok(Err(_)) => return,
            Ok(Ok(LineRead::TooLarge)) => {
                let r = framing_error(
                    d,
                    ErrorCode::RequestTooLarge,
                    format!(
                        "a request line may be at most {} bytes",
                        limits.max_line_bytes
                    ),
                );
                let _ = write_response(&mut wr, &r, limits.write_timeout).await;
                let _ = wr.shutdown().await;
                // Discard (never buffer) what is still arriving, briefly.
                let mut sink = [0u8; 8 * 1024];
                let _ = timeout(DRAIN_ON_CLOSE, async {
                    while matches!(rd.read(&mut sink).await, Ok(n) if n > 0) {}
                })
                .await;
                return;
            }
            Err(_elapsed) => {
                let r = framing_error(
                    d,
                    ErrorCode::BadRequest,
                    format!(
                        "request not completed within {} ms of its first byte",
                        limits.request_deadline.as_millis()
                    ),
                );
                let _ = write_response(&mut wr, &r, limits.write_timeout).await;
                return;
            }
        };
        if write_response(&mut wr, &resp, limits.write_timeout)
            .await
            .is_err()
        {
            return;
        }
    }
}

fn framing_error<D: Dispatch>(d: &D, code: ErrorCode, message: String) -> Response {
    d.on_framing_error(code);
    Response::failure(Value::Null, d.instance(), AdminError::new(code, message))
}

pub(crate) async fn write_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    r: &Response,
    t: Duration,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(r).map_err(io::Error::other)?;
    bytes.push(b'\n');
    match timeout(t, w.write_all(&bytes)).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "admin write timed out",
        )),
    }
}

pub(crate) struct ParsedRequest {
    pub(crate) id: Value,
    pub(crate) verb: Verb,
    pub(crate) args: Map<String, Value>,
}

fn bad_request(m: impl Into<String>) -> AdminError {
    AdminError::new(ErrorCode::BadRequest, m)
}

/// Validates the v1 envelope. Unknown fields are ignored (spec §11). The
/// `id` is echoed on errors whenever it could be read.
pub(crate) fn parse_request(line: &[u8]) -> Result<ParsedRequest, (Value, AdminError)> {
    let v: Value = serde_json::from_slice(line)
        .map_err(|e| (Value::Null, bad_request(format!("not valid JSON: {e}"))))?;
    let Value::Object(mut obj) = v else {
        return Err((Value::Null, bad_request("a request is one JSON object")));
    };
    let id = obj.remove("id").unwrap_or(Value::Null);
    match obj.get("v").and_then(Value::as_u64) {
        Some(1) => {}
        Some(other) => {
            return Err((
                id,
                AdminError::unsupported_version(format!(
                    "protocol version {other} is not supported"
                )),
            ));
        }
        None => return Err((id, bad_request("missing or non-integer \"v\""))),
    }
    let Some(verb_s) = obj.get("verb").and_then(Value::as_str) else {
        return Err((id, bad_request("missing \"verb\"")));
    };
    let Some(verb) = Verb::parse(verb_s) else {
        return Err((
            id,
            AdminError::new(
                ErrorCode::UnknownVerb,
                format!("unknown verb; this daemon knows {:?}", Verb::names()),
            ),
        ));
    };
    let args = match obj.remove("args") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m,
        Some(_) => return Err((id, bad_request("\"args\" must be an object"))),
    };
    Ok(ParsedRequest { id, verb, args })
}

/// `args.wait_ms`: an integer `0..=30000`, defaulting per verb.
pub(crate) fn wait_arg(args: &Map<String, Value>, default_ms: u64) -> Result<Duration, AdminError> {
    match args.get("wait_ms") {
        None | Some(Value::Null) => Ok(Duration::from_millis(default_ms)),
        Some(v) => match v.as_u64() {
            Some(n) if n <= MAX_WAIT_MS => Ok(Duration::from_millis(n)),
            _ => Err(bad_request(format!(
                "wait_ms must be an integer 0..={MAX_WAIT_MS}"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::unix::OwnedReadHalf;

    fn inst() -> InstanceId {
        InstanceId {
            pid: 1,
            started_at_ms: 2,
            wab_dir: "/w".into(),
            socket_path: "/s".into(),
            wab_dir_dev: 3,
            wab_dir_ino: 4,
        }
    }

    /// A stub dispatcher, so framing is tested without any daemon state.
    struct Echo;
    impl Dispatch for Echo {
        async fn dispatch(&self, line: &[u8], peer_uid: u32) -> Response {
            match parse_request(line) {
                Ok(r) => Response::success(
                    r.id,
                    inst(),
                    json!({"verb": r.verb.as_str(), "peer_uid": peer_uid}),
                ),
                Err((id, e)) => Response::failure(id, Some(inst()), e),
            }
        }
        fn instance(&self) -> Option<InstanceId> {
            Some(inst())
        }
    }

    fn fast() -> Limits {
        Limits {
            request_deadline: Duration::from_millis(200),
            idle_timeout: Duration::from_millis(300),
            ..Limits::PRODUCTION
        }
    }

    fn serve(limits: Limits) -> (BufReader<OwnedReadHalf>, tokio::net::unix::OwnedWriteHalf) {
        let (client, server) = UnixStream::pair().unwrap();
        tokio::spawn(async move { serve_connection(server, 1000, &Echo, &limits).await });
        let (rd, wr) = client.into_split();
        (BufReader::new(rd), wr)
    }

    /// `Some(json)` for a line, `None` once the server closed (EOF or reset).
    async fn next(rd: &mut BufReader<OwnedReadHalf>) -> Option<Value> {
        let mut s = String::new();
        let n = tokio::time::timeout(Duration::from_secs(2), rd.read_line(&mut s))
            .await
            .expect("the server answers or closes within 2 s");
        match n {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(serde_json::from_str(&s).expect("one JSON object per line")),
        }
    }

    #[tokio::test]
    async fn requests_are_answered_in_order_one_line_each() {
        let (mut rd, mut wr) = serve(Limits::PRODUCTION);
        wr.write_all(b"{\"v\":1,\"id\":1,\"verb\":\"status\"}\n{\"v\":1,\"id\":2,\"verb\":\"reload\",\"extra\":true}\n")
            .await
            .unwrap();
        let a = next(&mut rd).await.unwrap();
        let b = next(&mut rd).await.unwrap();
        assert_eq!(
            (a["id"].clone(), a["result"]["verb"].clone()),
            (json!(1), json!("status"))
        );
        assert_eq!(
            (b["id"].clone(), b["result"]["verb"].clone()),
            (json!(2), json!("reload"))
        );
        assert_eq!(a["result"]["peer_uid"], 1000);
        assert_eq!(a["instance"]["pid"], 1, "identity on every response");
    }

    #[tokio::test]
    async fn an_oversized_line_is_refused_then_closed() {
        let (mut rd, mut wr) = serve(Limits::PRODUCTION);
        // The server stops reading at 64 KiB, so the writer may be cut off.
        tokio::spawn(async move {
            let _ = wr.write_all(&vec![b'a'; 70 * 1024]).await;
        });
        let r = next(&mut rd).await.expect("one error line");
        assert_eq!(r["error"]["code"], "request_too_large");
        assert!(next(&mut rd).await.is_none(), "then the connection closes");
    }

    /// The bound holds while reading: nothing past the limit is buffered.
    #[tokio::test]
    async fn read_line_bounded_never_buffers_past_the_limit() {
        let data = vec![b'a'; 70 * 1024];
        let mut r = &data[..];
        let mut line = Vec::new();
        assert_eq!(
            read_line_bounded(&mut r, &mut line, MAX_LINE_BYTES)
                .await
                .unwrap(),
            LineRead::TooLarge
        );
        assert!(line.len() <= MAX_LINE_BYTES, "buffered {}", line.len());

        let mut exact = vec![b'b'; MAX_LINE_BYTES];
        exact.push(b'\n');
        let mut r = &exact[..];
        line.clear();
        assert_eq!(
            read_line_bounded(&mut r, &mut line, MAX_LINE_BYTES)
                .await
                .unwrap(),
            LineRead::Line
        );
        assert_eq!(line.len(), MAX_LINE_BYTES);

        // A line split over many small reads.
        let mut r = tokio::io::BufReader::with_capacity(4, &b"{\"v\":1}\nrest"[..]);
        line.clear();
        assert_eq!(
            read_line_bounded(&mut r, &mut line, 64).await.unwrap(),
            LineRead::Line
        );
        assert_eq!(line, b"{\"v\":1}");
    }

    #[tokio::test]
    async fn a_request_that_never_ends_hits_the_deadline() {
        let (mut rd, mut wr) = serve(fast());
        wr.write_all(b"{\"v\":1").await.unwrap(); // no newline
        let r = next(&mut rd).await.expect("an error line");
        assert_eq!(r["error"]["code"], "bad_request");
        assert!(
            r["error"]["message"].as_str().unwrap().contains("within"),
            "{r}"
        );
        assert!(next(&mut rd).await.is_none());
    }

    #[tokio::test]
    async fn an_idle_connection_is_closed() {
        let (mut rd, _wr) = serve(fast());
        let t0 = std::time::Instant::now();
        assert!(next(&mut rd).await.is_none());
        assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
    }

    #[tokio::test]
    async fn at_most_max_requests_per_connection() {
        let (mut rd, mut wr) = serve(Limits {
            max_requests: 2,
            ..Limits::PRODUCTION
        });
        for i in 0..3 {
            let _ = wr
                .write_all(format!("{{\"v\":1,\"id\":{i},\"verb\":\"status\"}}\n").as_bytes())
                .await;
        }
        assert_eq!(next(&mut rd).await.unwrap()["id"], 0);
        assert_eq!(next(&mut rd).await.unwrap()["id"], 1);
        assert!(next(&mut rd).await.is_none(), "the third is never answered");
    }

    #[test]
    fn parse_request_table() {
        let e = |line: &str| parse_request(line.as_bytes()).err().expect(line);
        let (id, err) = e("not json");
        assert_eq!((id, err.code), (Value::Null, ErrorCode::BadRequest));
        let (id, err) = e(r#"[1,2]"#);
        assert_eq!((id, err.code), (Value::Null, ErrorCode::BadRequest));
        let (id, err) = e(r#"{"v":2,"id":"a","verb":"status"}"#);
        assert_eq!((id, err.code), (json!("a"), ErrorCode::UnsupportedVersion));
        assert_eq!(err.supported, Some(vec![1]));
        let (_, err) = e(r#"{"id":1,"verb":"status"}"#);
        assert_eq!(err.code, ErrorCode::BadRequest, "v is required");
        let (_, err) = e(r#"{"v":1,"verb":"drain.explode"}"#);
        assert_eq!(err.code, ErrorCode::UnknownVerb);
        assert!(
            !err.message.contains("explode"),
            "the request text is not echoed"
        );
        let (_, err) = e(r#"{"v":1,"verb":"status","args":[1]}"#);
        assert_eq!(err.code, ErrorCode::BadRequest);
        let ok =
            parse_request(br#"{"v":1,"verb":"drain.pause","args":{"wait_ms":5},"x":1}"#).unwrap();
        assert_eq!(ok.verb, Verb::DrainPause);
        assert_eq!(ok.id, Value::Null);
        assert_eq!(ok.args.get("wait_ms"), Some(&json!(5)));
    }

    #[test]
    fn wait_arg_bounds() {
        let m = |v: Value| v.as_object().unwrap().clone();
        assert_eq!(
            wait_arg(&m(json!({})), 5000).unwrap(),
            Duration::from_millis(5000)
        );
        assert_eq!(
            wait_arg(&m(json!({"wait_ms": null})), 10).unwrap(),
            Duration::from_millis(10)
        );
        assert_eq!(
            wait_arg(&m(json!({"wait_ms": 0})), 5000).unwrap(),
            Duration::ZERO
        );
        assert_eq!(
            wait_arg(&m(json!({"wait_ms": 30000})), 0).unwrap(),
            Duration::from_secs(30)
        );
        for bad in [
            json!({"wait_ms": 30001}),
            json!({"wait_ms": -1}),
            json!({"wait_ms": "5"}),
        ] {
            assert_eq!(
                wait_arg(&m(bad), 0).unwrap_err().code,
                ErrorCode::BadRequest
            );
        }
    }
}
