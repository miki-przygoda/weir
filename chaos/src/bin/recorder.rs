//! The recording sink — the oracle's delivery log.
//!
//! weir posts committed batches here as NDJSON; this appends each record's
//! identity to a log and **fsyncs before returning 200**.
//!
//! The fsync is not incidental — it is the `Sink` contract. weir treats a 200
//! as "durably committed downstream" and becomes free to reclaim the segment.
//! A recorder that buffered in memory and lost records on its own crash would
//! manufacture false durability violations. The oracle must be at least as
//! durable as the thing it judges.
//!
//! Hand-rolled HTTP over `TcpListener`: the surface is one POST route, and a
//! framework dependency would buy nothing while adding a supply chain to a
//! component whose whole job is to be trustworthy.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(test)]
use std::net::Shutdown;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::Duration;

/// Append-only log of delivered record identities, fsynced before each ack.
struct DeliveryLog {
    file: File,
}

impl DeliveryLog {
    fn create(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    /// Appends every decodable record in an NDJSON body. Returns the count
    /// written. Undecodable lines are skipped rather than failing the batch:
    /// weir would retry the whole segment, and a permanently-undecodable
    /// record would wedge the drain forever.
    fn append_ndjson(&mut self, body: &[u8]) -> std::io::Result<usize> {
        let mut out = String::new();
        let mut count = 0usize;
        for line in body.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(text) = std::str::from_utf8(line) else {
                continue;
            };
            let Some(rec) = weir_chaos::decode_record(text) else {
                continue;
            };
            out.push_str(&format!("{} {}\n", rec.run_id, rec.seq));
            count += 1;
        }
        if !out.is_empty() {
            self.file.write_all(out.as_bytes())?;
            // Durable BEFORE the 200. See the module docs.
            self.file.sync_data()?;
        }
        Ok(count)
    }
}

/// Read/write timeout on an accepted connection.
///
/// Without this the recorder wedges **permanently**: `read_exact` on a raw
/// `TcpStream` blocks forever, and the accept loop below is serial, so one
/// half-open peer starves every subsequent delivery until the process is
/// killed. This harness injects faults that produce exactly that shape — a peer
/// killed mid-POST leaves the connection open with no FIN — and a delivery lost
/// that way reads back as a phantom durability violation. Ten seconds is six
/// orders of magnitude of headroom for a localhost POST, and bounds the
/// worst-case stall.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest body the recorder will allocate for.
///
/// `Content-Length` is peer-supplied. Allocating it blindly lets a corrupt
/// header abort the process — Rust aborts on allocation failure — which would
/// kill the oracle itself. Sized far above the largest plausible NDJSON batch.
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// What the recorder did with one request.
///
/// Returned rather than swallowed so tests can assert on the decision instead
/// of scraping response bytes.
#[derive(Debug, PartialEq, Eq)]
enum Response {
    /// Health probe answered; nothing recorded.
    Health,
    /// Batch durably recorded; carries the record count.
    Recorded(usize),
    /// Refused without recording; carries the status code (`0` = peer vanished
    /// before sending a complete request head).
    Rejected(u16),
}

/// Extracts `Content-Length` from an HTTP request head. Case-insensitive.
fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
}

fn reject(stream: &mut TcpStream, code: u16, reason: &str) -> std::io::Result<Response> {
    write!(
        stream,
        "HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\n\r\n"
    )?;
    stream.flush()?;
    Ok(Response::Rejected(code))
}

fn handle(stream: &mut TcpStream, log: &mut DeliveryLog) -> std::io::Result<Response> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            // Peer vanished mid-headers. Nothing to record, nothing to ack.
            return Ok(Response::Rejected(0));
        }
        let blank = line == "\r\n" || line == "\n";
        head.push_str(&line);
        if blank {
            break;
        }
    }

    // A HEAD probe is how the HTTP sink's health check works; answer it 200.
    if head.starts_with("HEAD") {
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
        stream.flush()?;
        return Ok(Response::Health);
    }
    if !head.starts_with("POST") {
        return reject(stream, 405, "Method Not Allowed");
    }

    // A POST with no Content-Length must NOT be acked. Treating it as a
    // zero-length body would return 200 for a delivery never read off the
    // socket — a phantom ack, the one thing the oracle must never produce.
    let Some(len) = content_length(&head) else {
        return reject(stream, 411, "Length Required");
    };
    if len > MAX_BODY_BYTES {
        return reject(stream, 413, "Payload Too Large");
    }

    let mut body = vec![0u8; len];
    reader.read_exact(&mut body)?;
    // Durable BEFORE the 200 — see the module docs. If this errors we return
    // without acking, weir classifies it transient, and retries the segment.
    let recorded = log.append_ndjson(&body)?;
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
    stream.flush()?;
    Ok(Response::Recorded(recorded))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut bind = "127.0.0.1:9900".to_string();
    let mut log_path = String::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--bind" => bind = args[i + 1].clone(),
            "--log" => log_path = args[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    if log_path.is_empty() {
        eprintln!("usage: recorder --bind <addr> --log <path>");
        std::process::exit(2);
    }

    let mut log = DeliveryLog::create(Path::new(&log_path)).expect("open delivery log");
    let listener = TcpListener::bind(&bind).expect("bind recorder");
    eprintln!("recorder listening on {bind}, log at {log_path}");

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                // Bound every blocking read and write. Without this one stalled
                // peer wedges the whole recorder — see IO_TIMEOUT.
                if let Err(e) = s.set_read_timeout(Some(IO_TIMEOUT)) {
                    eprintln!("recorder: could not set read timeout: {e}");
                }
                if let Err(e) = s.set_write_timeout(Some(IO_TIMEOUT)) {
                    eprintln!("recorder: could not set write timeout: {e}");
                }
                match handle(&mut s, &mut log) {
                    Ok(Response::Rejected(code)) if code != 0 => {
                        eprintln!("recorder: refused a request with {code}");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("recorder: request failed: {e}"),
                }
            }
            Err(e) => eprintln!("recorder: accept failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_one_line_per_ndjson_record() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!(
            "{}\n{}\n",
            weir_chaos::encode_record(3, 1, 64),
            weir_chaos::encode_record(3, 2, 64)
        );
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 2);

        let mut contents = String::new();
        std::fs::File::open(&log_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "3 1\n3 2\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skips_undecodable_lines_without_failing_the_batch() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-junk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!("junk\n{}\n\n", weir_chaos::encode_record(4, 9, 64));
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 1, "only the decodable record counts");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_content_length_from_a_request_head() {
        let head = "POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Length: 42\r\n\r\n";
        assert_eq!(content_length(head), Some(42));
        assert_eq!(content_length("POST / HTTP/1.1\r\n\r\n"), None);
    }

    // ── Socket-level tests ────────────────────────────────────────────────
    // The unit tests above exercise the log and the header parser in
    // isolation. These drive `handle` over a real loopback socket, because
    // every defect that actually threatens the oracle lives at that boundary:
    // a peer that stalls, a peer that lies about its length, a peer that
    // never sends a length at all.

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("chaos-rec-{tag}-{}-{n}", std::process::id()))
    }

    /// Drives one request through `handle` over loopback.
    ///
    /// `linger` keeps the client socket open after writing, so a
    /// short-body request genuinely stalls the server rather than hitting a
    /// clean EOF. `timeout` is applied to the server side.
    fn drive(
        request: Vec<u8>,
        linger: Duration,
        timeout: Duration,
    ) -> (std::io::Result<Response>, String) {
        let dir = unique_dir("drive");
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = std::thread::spawn(move || {
            let mut s = TcpStream::connect(addr).unwrap();
            let _ = s.write_all(&request);
            let _ = s.flush();
            if linger.is_zero() {
                // Half-close: FIN on the write side only. The server sees a
                // real EOF — so an incomplete request head is rejected
                // promptly instead of waiting out its timeout — while this
                // side can still read the response. A full `drop` here would
                // instead risk an RST that beats the server's response write,
                // which is what made these tests flaky.
                let _ = s.shutdown(Shutdown::Write);
            } else {
                // Simulate a HALF-OPEN peer: hold the connection open with no
                // FIN at all, which is what a process killed mid-POST leaves
                // behind. The server must time out rather than block forever.
                // Do NOT half-close here — that would hand the server a clean
                // EOF and the timeout would never be exercised.
                std::thread::sleep(linger);
            }
            let mut discard = Vec::new();
            let _ = s.read_to_end(&mut discard);
        });

        let (mut server_side, _) = listener.accept().unwrap();
        server_side.set_read_timeout(Some(timeout)).unwrap();
        server_side.set_write_timeout(Some(timeout)).unwrap();
        let result = handle(&mut server_side, &mut log);
        // Close the server side so the client's `read_to_end` sees EOF and the
        // join below cannot deadlock.
        drop(server_side);

        client.join().unwrap();
        let contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        std::fs::remove_dir_all(&dir).ok();
        (result, contents)
    }

    fn post(body: &str) -> Vec<u8> {
        format!(
            "POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    #[test]
    fn post_records_every_decodable_record_then_acks() {
        let body = format!(
            "{}\n{}\n",
            weir_chaos::encode_record(3, 1, 64),
            weir_chaos::encode_record(3, 2, 64)
        );
        let (res, log) = drive(post(&body), Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Recorded(2));
        assert_eq!(log, "3 1\n3 2\n");
    }

    #[test]
    fn head_probe_is_answered_without_touching_the_log() {
        let req = b"HEAD /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Health);
        assert_eq!(log, "", "a health probe must not write a delivery");
    }

    #[test]
    fn post_without_content_length_is_refused_not_acked() {
        // Treating a missing length as a zero-length body would return 200 for
        // a delivery never read off the socket — a phantom ack, which is the
        // one thing the oracle must never produce.
        let req = b"POST /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(411));
        assert_eq!(log, "");
    }

    #[test]
    fn oversized_content_length_is_refused_before_allocating() {
        // Content-Length is peer-supplied. Allocating it blindly lets a corrupt
        // header abort the process (Rust aborts on allocation failure), killing
        // the oracle outright.
        let req = format!(
            "POST /ingest HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        )
        .into_bytes();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(413));
        assert_eq!(log, "");
    }

    #[test]
    fn a_non_post_method_is_refused() {
        let req = b"GET /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(405));
        assert_eq!(log, "");
    }

    #[test]
    fn a_truncated_body_times_out_rather_than_wedging_forever() {
        // Declares 1000 bytes, sends 10, holds the socket open. Without a read
        // timeout this blocks FOREVER — and because the accept loop is serial,
        // one such peer starves every subsequent delivery from every future
        // connection until the process is killed. This harness injects faults
        // that produce exactly this shape (a peer killed mid-POST leaves the
        // connection open with no FIN), and a lost delivery reads back as a
        // phantom durability violation.
        let req = b"POST /ingest HTTP/1.1\r\nContent-Length: 1000\r\n\r\n0123456789".to_vec();
        let start = std::time::Instant::now();
        let (res, log) = drive(req, Duration::from_millis(600), Duration::from_millis(150));
        assert!(res.is_err(), "a stalled peer must error, not hang");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the stall must be bounded, took {:?}",
            start.elapsed()
        );
        assert_eq!(log, "", "nothing was durably delivered");
    }

    #[test]
    fn a_connection_closed_mid_headers_is_refused_not_acked() {
        let req = b"POST /ingest HTTP/1.1\r\nHost: x\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(0));
        assert_eq!(log, "");
    }
}
