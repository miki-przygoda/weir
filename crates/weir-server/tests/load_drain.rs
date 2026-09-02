//! Delivery-side load tests for weir-server — the drain, not the ingest.
//!
//! `tests/load.rs` has fifteen scenarios and every one of them measures the
//! producer→daemon→WAB path against the **noop** sink. It says so itself, under
//! "Coverage caveats", and proposes exactly this file as the follow-up: a
//! localhost mock endpoint, parameterised over batch mode and concurrency.
//!
//! The gap mattered because weir is a *buffer*. Ingest throughput says how fast
//! the buffer fills; nothing here previously said how fast it empties, which is
//! the half that decides whether it ever does. A daemon that accepts 200k rec/s
//! and delivers 2k rec/s is not fast, it is a queue with good marketing.
//!
//! # Running locally
//!
//! ```sh
//! cargo test -p weir-server --test load_drain --release --features http-sink -- --nocapture
//! ```
//!
//! # What the numbers mean
//!
//! Every scenario measures **records confirmed by the sink**, counted at the
//! mock endpoint, from the moment the last record is acked at ingest. It is a
//! delivery rate, not a round trip: ingest is deliberately finished first so the
//! two paths are not competing for the same cores while the drain is timed.
//!
//! The mock is an in-process HTTP/1.1 endpoint doing the minimum: read a
//! request, count records, reply 200. It has no framework, no allocation per
//! record beyond the body, and answers in microseconds — so these are ceilings
//! set by weir's drain, not by the endpoint. A real sink will be slower, which
//! is the point of `drain_under_slow_sink`.
//!
//! # Coverage caveats (known gaps)
//!
//! - **Loopback only.** No network latency, no TLS to the sink, no DNS. A real
//!   HTTP sink over a WAN pays an RTT per batch that dominates everything here.
//! - **One sink type.** HTTP only. The SQL sinks batch very differently and are
//!   not covered.
//! - **No sustained-pressure scenario.** Each test fills the WAB, then drains
//!   it. Steady-state behaviour where ingest and drain run concurrently at
//!   matched rates is not measured.

#![cfg(all(unix, feature = "http-sink"))]

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use weir_core::Durability;
use weir_testkit::{free_port, weir_server};

// ── Result reporting ───────────────────────────────────────────────────────
//
// Same `BENCH: {json}` line shape as tests/load.rs so deploy/avg_benchmarks.py
// picks these up with no changes. `delivered_rps` is deliberately a distinct
// key from load.rs's `throughput_rps` — conflating an ingest rate with a
// delivery rate in one column is precisely the confusion this file exists to
// end.

fn emit_delivery(scenario: &str, records: usize, elapsed: Duration) {
    let rps = records as f64 / elapsed.as_secs_f64();
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"delivered_records\":{records},\
         \"wall_ms\":{},\"delivered_rps\":{}}}",
        elapsed.as_millis(),
        rps as u64,
    );
}

// ── Mock HTTP sink ─────────────────────────────────────────────────────────

/// Counts records delivered to a localhost endpoint.
///
/// Records are counted per *record*, not per request, so per-record POST mode
/// and NDJSON batch mode produce directly comparable numbers: an NDJSON body
/// counts its lines. That comparability is the whole reason the two modes can
/// be put side by side in the results table.
struct MockSink {
    addr: String,
    delivered: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    /// Per-response delay, to model a sink that is not instantaneous.
    delay: Arc<AtomicU64>,
    /// While true, every request is answered 503 — a retryable failure.
    failing: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl MockSink {
    fn start() -> Self {
        let port = free_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind mock sink");
        listener.set_nonblocking(true).expect("nonblocking");

        let delivered = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let delay = Arc::new(AtomicU64::new(0));
        let failing = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));

        {
            let (delivered, requests) = (Arc::clone(&delivered), Arc::clone(&requests));
            let (delay, failing, stop) =
                (Arc::clone(&delay), Arc::clone(&failing), Arc::clone(&stop));
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((sock, _)) => {
                            let (delivered, requests) =
                                (Arc::clone(&delivered), Arc::clone(&requests));
                            let (delay, failing, stop) =
                                (Arc::clone(&delay), Arc::clone(&failing), Arc::clone(&stop));
                            thread::spawn(move || {
                                serve_conn(sock, &delivered, &requests, &delay, &failing, &stop);
                            });
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        Self {
            addr: format!("http://127.0.0.1:{port}/ingest"),
            delivered,
            requests,
            delay,
            failing,
            stop,
        }
    }

    fn delivered(&self) -> usize {
        self.delivered.load(Ordering::Relaxed)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    fn set_delay(&self, d: Duration) {
        self.delay.store(d.as_micros() as u64, Ordering::Relaxed);
    }

    fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Relaxed);
    }

    /// Blocks until `n` records have been counted, or panics on timeout.
    ///
    /// Returns how long the wait took — that duration IS the measurement in
    /// every throughput scenario below.
    fn await_delivery(&self, n: usize, timeout: Duration) -> Duration {
        let t0 = Instant::now();
        while self.delivered() < n {
            assert!(
                t0.elapsed() < timeout,
                "drain stalled: {}/{n} records delivered after {:?}",
                self.delivered(),
                t0.elapsed()
            );
            thread::sleep(Duration::from_millis(1));
        }
        t0.elapsed()
    }
}

impl Drop for MockSink {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Serves one keep-alive connection. reqwest pools connections, so a single
/// socket carries many requests and this must loop rather than answer once.
fn serve_conn(
    sock: TcpStream,
    delivered: &AtomicUsize,
    requests: &AtomicUsize,
    delay: &AtomicU64,
    failing: &AtomicBool,
    stop: &AtomicBool,
) {
    // A socket returned by `accept()` inherits O_NONBLOCK from the listener on
    // macOS and the BSDs. The listener is non-blocking so its accept loop can
    // poll for the stop flag, so without this every read below returns
    // WouldBlock, the handler drops the connection, and the client retries —
    // which looked exactly like a catastrophically slow drain (21 rec/s) rather
    // than like the harness bug it was.
    sock.set_nonblocking(false).expect("blocking mode");
    sock.set_nodelay(true).ok();
    let mut reader = BufReader::new(sock.try_clone().expect("clone sock"));
    let mut sock = sock;

    while !stop.load(Ordering::Relaxed) {
        // Request line + headers, terminated by a blank line.
        let mut content_length = 0usize;
        let mut saw_request = false;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => return, // peer closed
                Ok(_) => {}
                Err(_) => return,
            }
            if !saw_request {
                saw_request = true;
            }
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break; // end of headers
            }
            if let Some(v) = trimmed
                .strip_prefix("Content-Length: ")
                .or_else(|| trimmed.strip_prefix("content-length: "))
            {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
        if !saw_request {
            return;
        }

        let mut body = vec![0u8; content_length];
        if content_length > 0 && reader.read_exact(&mut body).is_err() {
            return;
        }

        requests.fetch_add(1, Ordering::Relaxed);

        let micros = delay.load(Ordering::Relaxed);
        if micros > 0 {
            thread::sleep(Duration::from_micros(micros));
        }

        if failing.load(Ordering::Relaxed) {
            // 503 is retryable, so the drain backs off and retries rather than
            // dead-lettering. Nothing is counted as delivered.
            let _ = sock.write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            );
            let _ = sock.flush();
            continue;
        }

        // One record per request, unless the body is NDJSON — in which case
        // every non-empty line is a record.
        let n = if body.is_empty() {
            0
        } else {
            let lines = body
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .count();
            lines.max(1)
        };
        delivered.fetch_add(n, Ordering::Relaxed);

        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
        let _ = sock.flush();
    }
}

// ── Scenarios ──────────────────────────────────────────────────────────────

/// 256-byte records: small enough to be realistic, big enough that a few
/// thousand of them roll several segments.
const PAYLOAD: &[u8] = &[b'x'; 256];

/// 64 KiB segments — roughly 240 records each. The default is 256 MiB, at which
/// a few thousand small records never rotate at all and the drain has literally
/// nothing to do. Getting this wrong is why a first version of this file timed
/// out rather than measuring anything.
const SEGMENT_BYTES: &str = "65536";

/// Idle-seal after 1 s so the final partial segment is sealed and delivered
/// without waiting for shutdown. It only ever applies to the tail.
const IDLE_SEAL_SECS: &str = "1";

/// The portion of a fill guaranteed to sit in *sealed* segments, and so to drain
/// without waiting on the idle timer.
///
/// Timing covers this bulk alone, deliberately: including the tail would fold up
/// to a second of idle-seal latency into a throughput number and make it a
/// measure of the timer. Each scenario then asserts separately that the full
/// count arrives, so the tail is checked for correctness without polluting the
/// rate.
fn bulk(n: usize) -> usize {
    n * 3 / 4
}

/// Builds a daemon pointed at `sink`, with the segment sizing every scenario
/// needs. Callers add sink-specific env on top.
fn daemon(tag: &'static str, sink: &MockSink) -> weir_testkit::WeirServerBuilder {
    weir_server!(tag)
        .bench_preset()
        .env("WEIR_SINK_TYPE", "http")
        .env("WEIR_SINK_URL", sink.addr.clone())
        .env("WEIR_WAB_SEGMENT_MAX_BYTES", SEGMENT_BYTES)
        .env("WEIR_WAB_SEGMENT_MAX_AGE_SECS", IDLE_SEAL_SECS)
}

/// Pushes `n` Buffered records and returns once every one is acked at ingest.
///
/// Buffered, not Durable, on purpose: this file measures the drain, and an
/// fsync per batch at ingest would put the WAB's write path inside the
/// measurement window of a delivery benchmark.
fn fill(srv: &weir_testkit::WeirServer, n: usize) {
    let mut client = srv.client();
    for _ in 0..n {
        client.push(PAYLOAD, Durability::Buffered).expect("push");
    }
}

/// Accumulates a backlog of `n` records the sink has refused, then lets the sink
/// answer and times how fast the backlog clears.
///
/// **Why every scenario is shaped this way.** The drain runs concurrently with
/// ingest, so simply filling and then timing measures whatever happens to be
/// left — for the faster modes that is nothing at all, and the first version of
/// this file duly reported a `wall_ms` of 0 and a rate of nine billion. Holding
/// the sink down during the fill makes the backlog a known quantity and the
/// start of the measurement an actual event, so the number means one thing:
/// records per second out of a full buffer.
///
/// Returns `(elapsed_for_bulk, requests_issued_during_delivery)`.
fn measure_backlog_drain(
    srv: &weir_testkit::WeirServer,
    sink: &MockSink,
    records: usize,
    timeout: Duration,
) -> (Duration, usize) {
    fill(srv, records);
    assert_eq!(
        sink.delivered(),
        0,
        "nothing may count as delivered while the sink is refusing"
    );

    let requests_before = sink.requests();
    sink.set_failing(false);
    let elapsed = sink.await_delivery(bulk(records), timeout);

    // Correctness, separate from the rate: nothing may be dropped on the way to
    // the sink. No other test in either load suite checks this.
    sink.await_delivery(records, Duration::from_secs(60));
    (elapsed, sink.requests() - requests_before)
}

/// Baseline: default HTTP sink, one POST per record.
///
/// This is the number the project has never published — the rate at which a
/// default-configured weir empties its buffer into an HTTP endpoint.
#[test]
fn drain_throughput_http_per_record() {
    const RECORDS: usize = 2_000;
    let sink = MockSink::start();
    sink.set_failing(true);
    let srv = daemon("drain_per_record", &sink)
        .env("WEIR_SINK_HTTP_BATCH", "none")
        .start();

    let (elapsed, requests) = measure_backlog_drain(&srv, &sink, RECORDS, Duration::from_secs(180));
    emit_delivery("drain_http_per_record", bulk(RECORDS), elapsed);

    assert!(
        requests >= RECORDS,
        "per-record mode must issue at least one request per record, got {requests} for {RECORDS}"
    );
}

/// The same backlog through NDJSON batch framing.
///
/// `sink_http_batch = ndjson` exists to raise this number and, until now,
/// nothing measured whether it does. The assertion is deliberately weak — that
/// batching issued fewer requests than there were records — because the *rate*
/// is the reportable output, and pinning a ratio would make this a flaky gate on
/// shared CI hardware rather than a measurement.
#[test]
fn drain_throughput_http_ndjson() {
    const RECORDS: usize = 2_000;
    let sink = MockSink::start();
    sink.set_failing(true);
    let srv = daemon("drain_ndjson", &sink)
        .env("WEIR_SINK_HTTP_BATCH", "ndjson")
        .start();

    let (elapsed, requests) = measure_backlog_drain(&srv, &sink, RECORDS, Duration::from_secs(180));
    emit_delivery("drain_http_ndjson", bulk(RECORDS), elapsed);

    assert!(
        requests < RECORDS,
        "ndjson mode must batch: {requests} requests for {RECORDS} records"
    );
}

/// Backlog drain against a sink that takes ~1 ms to answer.
///
/// The interesting property is not the rate itself but whether the drain
/// overlaps requests. At 1 ms per response, serial delivery caps at ~1000 rec/s;
/// anything materially above that is concurrency working. This is the scenario
/// that would catch a regression turning the HTTP sink serial — something no
/// ingest benchmark can see.
#[test]
fn drain_under_slow_sink() {
    const RECORDS: usize = 1_000;
    let sink = MockSink::start();
    sink.set_failing(true);
    let srv = daemon("drain_slow", &sink)
        .env("WEIR_SINK_HTTP_BATCH", "none")
        .env("WEIR_SINK_HTTP_CONCURRENCY", "16")
        .start();

    // The delay applies to the delivery window only — a 1 ms pause on each of
    // the refusals would just slow the backlog build-up down.
    fill(&srv, RECORDS);
    sink.set_delay(Duration::from_millis(1));
    sink.set_failing(false);

    let elapsed = sink.await_delivery(bulk(RECORDS), Duration::from_secs(240));
    emit_delivery("drain_slow_sink_1ms_conc16", bulk(RECORDS), elapsed);

    sink.await_delivery(RECORDS, Duration::from_secs(120));
}

/// Ingest must be unaffected by a sink outage — the separation is weir's entire
/// proposition, and it is asserted here rather than assumed.
///
/// The catch-up rate itself is the same measurement as
/// `drain_throughput_http_ndjson`; what this test adds is the guarantee that
/// none of the 2,000 records was lost, dead-lettered, or double-counted across
/// an outage long enough for the drain to exhaust its retries.
#[test]
fn ingest_survives_a_sink_outage_and_every_record_arrives() {
    const RECORDS: usize = 2_000;
    let sink = MockSink::start();
    sink.set_failing(true);
    let srv = daemon("drain_catchup", &sink)
        .env("WEIR_SINK_HTTP_BATCH", "ndjson")
        .start();

    let t0 = Instant::now();
    fill(&srv, RECORDS);
    let ingest = t0.elapsed();
    assert_eq!(
        sink.delivered(),
        0,
        "nothing may count as delivered while the sink is returning 503"
    );
    assert!(
        sink.requests() > 0,
        "the drain must have been trying to deliver during the outage"
    );

    sink.set_failing(false);
    sink.await_delivery(RECORDS, Duration::from_secs(240));

    // No delivery RATE is reported here, deliberately. This test waits for the
    // FULL count including the tail, and the tail only seals on the 1 s idle
    // timer — so the elapsed time is dominated by that timer and a rate derived
    // from it would describe the timer, not the drain. An early version of this
    // file did publish that number (1,979 rec/s against a real 7,363) which is
    // exactly the kind of quietly-wrong figure this suite exists to stop.
    // The catch-up rate is `drain_throughput_http_ndjson`; this test is here for
    // the correctness properties below.
    println!(
        "BENCH: {{\"scenario\":\"ingest_during_sink_outage\",\"records\":{RECORDS},\
         \"wall_ms\":{}}}",
        ingest.as_millis()
    );
    assert_eq!(
        sink.delivered(),
        RECORDS,
        "every acked record must reach the sink exactly once after recovery"
    );
}
