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

// `delivered_records` is the count observed DURING `wall_ms`, never the size of
// the backlog. The two are different numbers and reporting the second over the
// first is what overstated every rate this file has ever published:
// `excluded_before_t0` is that gap, emitted so a published row carries its own
// basis instead of needing a sweep to reconstruct it. Only
// `measure_backlog_drain` may call this — it is the one place that observes both
// endpoints of the window.
fn emit_delivery(
    scenario: &str,
    records: usize,
    elapsed: Duration,
    excluded_before_t0: usize,
    wakeup: Duration,
) {
    let rps = records as f64 / elapsed.as_secs_f64();
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"delivered_records\":{records},\
         \"wall_ms\":{},\"delivered_rps\":{},\"excluded_before_t0\":{excluded_before_t0},\
         \"wakeup_ms\":{},\"wab\":\"{}\"}}",
        elapsed.as_millis(),
        rps as u64,
        wakeup.as_millis(),
        wab_backing(),
    );
}

/// What the WAB is sitting on, recorded in every result line.
///
/// This is not decoration. The confirm path calls `sync_all`, which on macOS is
/// `F_FULLFSYNC` — measured at 3,965 µs against 238 µs for the `F_BARRIERFSYNC`
/// used for record data. That single call dominated this suite's window, so a
/// number is meaningless without knowing what it was written to: the same
/// scenario went from 6,548 to 20,233 rec/s purely by moving the WAB to a RAM
/// disk, and its run-to-run spread went from ±100% to ±9%.
///
/// Set `WEIR_BENCH_WAB_DIR` to a tmpfs/RAM-disk path to measure the drain rather
/// than the filesystem. Leave it unset to measure what a real deployment sees.
fn wab_backing() -> &'static str {
    if std::env::var_os("WEIR_BENCH_WAB_DIR").is_some() {
        "external"
    } else {
        "default"
    }
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

    /// Blocks until at least one record has been delivered.
    ///
    /// This is what separates the drain's RATE from the drain's LATENCY TO
    /// START. After a sink outage the drain is inside an exponential backoff
    /// (`100ms * attempts`), so the interval between "the sink became healthy"
    /// and "the drain noticed" is a sleep, not work. Timing from the flip folded
    /// a variable few-hundred-ms sleep into a window of roughly the same size —
    /// which is most of why five consecutive runs of this suite spread 2.6-4.1x.
    fn await_first_delivery(&self, timeout: Duration) -> Duration {
        let t0 = Instant::now();
        while self.delivered() == 0 {
            assert!(
                t0.elapsed() < timeout,
                "no record was delivered within {timeout:?} of the sink recovering"
            );
            thread::sleep(Duration::from_millis(1));
        }
        t0.elapsed()
    }

    /// Blocks until `n` records have been counted, or panics on timeout.
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
    // 50%, not 75%. A 256-byte record costs ~270 bytes framed, so a 64 KiB
    // segment holds ~242 of them and only whole rotated segments are sealed.
    // At 1,000 records that is 3 sealed segments = ~726 records, but 75% asked
    // for 750 — so the wait ran past the sealed data and blocked on the 1 s
    // idle-seal timer. `drain_under_slow_sink` was returning 1,004/1,010/1,010 ms
    // as a result: three digits of timer, not of drain.
    //
    // 50% leaves a whole segment of headroom at every size this file uses.
    n / 2
}

/// Builds a daemon pointed at `sink`, with the segment sizing every scenario
/// needs. Callers add sink-specific env on top.
fn daemon(tag: &'static str, sink: &MockSink) -> weir_testkit::WeirServerBuilder {
    let b = weir_server!(tag)
        .bench_preset()
        .env("WEIR_SINK_TYPE", "http")
        .env("WEIR_SINK_URL", sink.addr.clone())
        .env("WEIR_WAB_SEGMENT_MAX_BYTES", SEGMENT_BYTES)
        .env("WEIR_WAB_SEGMENT_MAX_AGE_SECS", IDLE_SEAL_SECS)
        // Shorten the retry backoff. The scenarios below hold the sink down to
        // build a known backlog, so the drain is always in backoff when the sink
        // returns; the default 100ms-per-attempt schedule then decides when
        // measurement starts. The window itself now begins at the first
        // delivered record, so this only reduces dead time — it does not move
        // the rate.
        .env("WEIR_SINK_RETRY_BASE_DELAY_MS", "5");
    match std::env::var("WEIR_BENCH_WAB_DIR") {
        Ok(d) if !d.is_empty() => {
            // `wab_dir` requires the caller to create the directory — the whole
            // point of the override is to choose the filesystem, so the builder
            // will not conjure a path for us. Omitting this is why the knob
            // never worked: every scenario panicked before pushing a record.
            //
            // Per-tag, so concurrently-running scenarios do not share a WAB, and
            // fresh, so a previous run's segments are not drained by this one.
            let dir = std::path::PathBuf::from(d).join(tag);
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)
                .unwrap_or_else(|e| panic!("WEIR_BENCH_WAB_DIR: create {}: {e}", dir.display()));
            b.wab_dir(dir)
        }
        _ => b,
    }
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
/// This helper **emits the BENCH line itself**, and that is deliberate. The
/// numerator used to be chosen by the caller (`bulk(RECORDS)`) while the window
/// was computed here, so the two could disagree and nothing could notice —
/// which is exactly what happened. Both now come from the same observation, and
/// `emit_delivery` documents that it may only be called from here.
///
/// `before_flip` runs after the backlog is built and before the sink is let
/// through, for scenarios that need to arm a delay that must not slow the
/// refusals during the fill.
///
/// Returns `requests_issued_during_delivery`.
fn measure_backlog_drain(
    scenario: &str,
    srv: &weir_testkit::WeirServer,
    sink: &MockSink,
    records: usize,
    timeout: Duration,
    before_flip: impl FnOnce(),
) -> usize {
    fill(srv, records);
    assert_eq!(
        sink.delivered(),
        0,
        "nothing may count as delivered while the sink is refusing"
    );

    let requests_before = sink.requests();
    before_flip();
    sink.set_failing(false);

    // Start the clock at the FIRST delivered record, not at the flip. Between
    // the two the drain is asleep in its retry backoff, and that sleep is not
    // delivery work — including it measured the backoff timer as though it were
    // throughput.
    let wakeup = sink.await_first_delivery(timeout);
    let t0 = Instant::now();
    // Read the counter in the same breath as the clock. `await_first_delivery`
    // polls on a 1 ms sleep and one NDJSON POST carries up to
    // `sink_max_batch_size` records, so by the time it returns, hundreds can
    // already be out the door. Those records were delivered BEFORE this window
    // and must not be counted inside it.
    let delivered_at_t0 = sink.delivered();
    assert!(
        delivered_at_t0 < bulk(records),
        "the whole measured backlog ({}) was delivered before the clock started \
         ({delivered_at_t0} records); there is no window left to measure — raise \
         RECORDS for this scenario",
        bulk(records)
    );

    sink.await_delivery(bulk(records), timeout);
    // Counter before clock, at both ends of the window. A record landing
    // between the two reads is then excluded from the numerator while its time
    // still counts in the denominator, so the residual error understates the
    // rate. Reading the clock first would bias it upward, which is the same
    // mistake this helper exists to stop making.
    let delivered_at_end = sink.delivered();
    let elapsed = t0.elapsed();
    let measured = delivered_at_end - delivered_at_t0;

    println!(
        "    (drain woke {} ms after the sink recovered; excluded from the rate. \
         {delivered_at_t0} records were already delivered at t0 and are excluded \
         from the numerator too)",
        wakeup.as_millis()
    );
    emit_delivery(scenario, measured, elapsed, delivered_at_t0, wakeup);

    // Correctness, separate from the rate: nothing may be dropped on the way to
    // the sink. No other test in either load suite checks this.
    sink.await_delivery(records, Duration::from_secs(60));
    sink.requests() - requests_before
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

    let requests = measure_backlog_drain(
        "drain_http_per_record",
        &srv,
        &sink,
        RECORDS,
        Duration::from_secs(180),
        || {},
    );

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
    // 20,000 rather than 2,000, because this scenario was measuring its own
    // quantisation. NDJSON delivers in POSTs of `sink_max_batch_size` (100 by
    // default) and `await_delivery` polls on a 1 ms sleep, so both ends of the
    // window move in ~100-record steps. At 2,000 the timed window was 4-5 ms on
    // a CI runner and 8-11 ms on an operator box -- four to eleven poll ticks --
    // and three identical iterations spread 1.75x. The other two scenarios,
    // whose windows are 31-45 ms, spread 1.007x and 1.002x over the same runs.
    //
    // Ten times the records makes the window ~10x wider without changing what
    // is measured: `bulk()` still times only the sealed portion, and the
    // per-record and slow-sink scenarios are deliberately left at their sizes,
    // which are already wide enough.
    const RECORDS: usize = 20_000;
    let sink = MockSink::start();
    sink.set_failing(true);
    let srv = daemon("drain_ndjson", &sink)
        .env("WEIR_SINK_HTTP_BATCH", "ndjson")
        .start();

    let requests = measure_backlog_drain(
        "drain_http_ndjson",
        &srv,
        &sink,
        RECORDS,
        Duration::from_secs(180),
        || {},
    );

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

    // The delay is armed after the backlog is built and before the sink is let
    // through: a 1 ms pause on each of the refusals would only slow the
    // build-up down.
    //
    // Adopting `measure_backlog_drain` is also what puts this scenario on the
    // same basis as its two table neighbours. It used to start its clock at the
    // flip, so its window included the drain's retry backoff — time in which no
    // record is delivered at all — while theirs excluded it. Three rows were
    // published side by side on two opposite biases: these understated, those
    // overstated.
    let _ = measure_backlog_drain(
        "drain_slow_sink_1ms_conc16",
        &srv,
        &sink,
        RECORDS,
        Duration::from_secs(240),
        || sink.set_delay(Duration::from_millis(1)),
    );
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

// ── The sizing rule A6 shipped without ─────────────────────────────────────

/// How fast the WAB grows when ingest outruns the sink, and whether that rate
/// is predictable from the two rates and the stored record size.
///
/// **Why this exists.** §8 of the A6 design is unambiguous:
///
/// > There is **one** drain thread for the whole daemon; delivery does not
/// > scale with `shard_count` while ingest does. A6 multiplies the rate at
/// > which the WAB outruns the drain by ~100x, against a `wab_max_bytes` that
/// > **defaults to 0 (disabled)**. A6 must not ship without a recommended
/// > value and a sizing rule.
///
/// A6 shipped in 4.0.0. There is no recommended value and no sizing rule. This
/// measures the input to one: it runs a producer flat out against a sink held
/// to a known rate, samples `weir_wab_bytes_on_disk` as the backlog builds, and
/// reports observed growth beside the growth predicted by
/// `(ingest_rps - drain_rps) x stored_bytes_per_record`. If the two agree, an
/// operator can size `wab_max_bytes` from a burst duration and two rates they
/// already know, without running this.
///
/// Deliberately *not* asserted. The point is the number, and a threshold here
/// would be a guess about a machine.
#[test]
#[ignore = "operator-run; ~5 minutes"]
fn wab_growth_when_ingest_outruns_the_sink() {
    // Per-request delays for the sink, in ms. In NDJSON mode each request
    // carries up to `sink_max_batch_size` records, so this sets a rate rather
    // than a per-record cost: 100 records per request at 10 ms is ~10k rec/s.
    const SINK_DELAYS_MS: &[u64] = &[2, 10, 50];
    const SAMPLE_MS: u64 = 500;
    const RUN_SECS: u64 = 20;

    for &delay_ms in SINK_DELAYS_MS {
        let sink = MockSink::start();
        sink.set_delay(Duration::from_millis(delay_ms));

        let srv = daemon("r_size", &sink)
            .extra_config("sink_http_batch      = \"ndjson\"")
            .extra_config("sink_max_batch_size  = 100")
            .start();
        let mut client = srv.client();

        let read = |name: &str| -> f64 {
            let body = srv.scrape_metrics();
            for line in body.lines() {
                if line.starts_with(name)
                    && let Some(v) = line.split_whitespace().next_back()
                    && let Ok(n) = v.parse()
                {
                    return n;
                }
            }
            f64::NAN
        };

        let batch: Vec<&[u8]> = vec![PAYLOAD; 256];
        let started = Instant::now();
        let mut pushed: u64 = 0;
        let mut samples: Vec<(f64, f64, f64)> = Vec::new(); // (t, wab_bytes, delivered)
        let mut next_sample = Duration::from_millis(SAMPLE_MS);

        while started.elapsed() < Duration::from_secs(RUN_SECS) {
            let out = client
                .push_batch(&batch, Durability::Buffered)
                .expect("push_batch");
            assert!(out.all_accepted(), "ingest rejected a record mid-burst");
            pushed += batch.len() as u64;

            if started.elapsed() >= next_sample {
                let t = started.elapsed().as_secs_f64();
                let wab = read("weir_wab_bytes_on_disk");
                let delivered = sink.delivered() as f64;
                // One line per sample. A first version reported only a growth
                // rate between two chosen samples and produced 0 B/s while
                // ingest outran the drain 3:1 -- which is not a rate, it is a
                // plateau the two-point estimate could not see. The curve is
                // the result; the summary below is a convenience.
                println!(
                    "\nBENCH: {{\"scenario\":\"wab_growth_sample\",\
                     \"sink_delay_ms\":{delay_ms},\"t_s\":{t:.2},\
                     \"wab_bytes\":{wab:.0},\"delivered\":{delivered:.0},\
                     \"pushed\":{pushed}}}"
                );
                samples.push((t, wab, delivered));
                next_sample += Duration::from_millis(SAMPLE_MS);
            }
        }
        let elapsed = started.elapsed();

        // Growth over the middle of the run: the first samples include the
        // drain starting up and the last include whatever the final segment is
        // doing, and neither is the steady state being sized for.
        let n = samples.len();
        let (a, b) = (samples[n / 4], samples[n - 1 - n / 8]);
        let observed_growth = (b.1 - a.1) / (b.0 - a.0);
        let drain_rps = (b.2 - a.2) / (b.0 - a.0);
        let ingest_rps = pushed as f64 / elapsed.as_secs_f64();

        // Bytes on disk per record, from the daemon rather than from sizeof.
        // `weir_wab_record_stored_bytes` is a COUNTER, so it exposes `_total`
        // and no `_sum`/`_count`; reading the histogram names returns the
        // not-found sentinel and every derived figure becomes NaN.
        let stored_total = read("weir_wab_record_stored_bytes_total");
        let per_record = stored_total / pushed as f64;
        let predicted_growth = (ingest_rps - drain_rps) * per_record;

        println!(
            "\nBENCH: {{\"scenario\":\"wab_growth\",\"sink_delay_ms\":{delay_ms},\
             \"ingest_rps\":{ingest_rps:.0},\"drain_rps\":{drain_rps:.0},\
             \"stored_bytes_per_record\":{per_record:.1},\
             \"observed_growth_bytes_s\":{observed_growth:.0},\
             \"predicted_growth_bytes_s\":{predicted_growth:.0},\
             \"model_error\":{:.3},\"wab_bytes_min\":{:.0},\
             \"wab_bytes_max\":{:.0},\"wab_bytes_final\":{:.0},\
             \"samples\":{},\"pushed\":{pushed}}}",
            if predicted_growth.abs() > 1.0 {
                observed_growth / predicted_growth
            } else {
                f64::NAN
            },
            samples.iter().map(|s| s.1).fold(f64::INFINITY, f64::min),
            samples
                .iter()
                .map(|s| s.1)
                .fold(f64::NEG_INFINITY, f64::max),
            b.1,
            n,
        );
        srv.shutdown();
    }
}

/// Time to recover across a hard restart, against the size of the backlog held
/// at the moment of the crash.
///
/// An operator restarting a daemon that is holding a backlog has no published
/// number for how long it is unavailable, and the answer scales with the buffer
/// rather than being constant.
///
/// **This lives here, not in `tests/research.rs`, and that is the whole point.**
/// The first version used `bench_preset`'s noop sink, which drains as fast as
/// ingest fills — so the WAB was empty at kill time (`wab_bytes: 0` on every
/// row) and it timed an empty daemon's startup: 160, 20 and 20 ms for
/// nominally 64 MiB, 256 MiB and 512 MiB of backlog. A recovery benchmark whose
/// answer does not vary with the thing it claims to vary with is measuring
/// something else. Holding the sink failing is what makes a backlog exist.
#[test]
#[ignore = "operator-run; writes multi-hundred-MB WABs"]
fn recovery_time_vs_backlog_size() {
    const PAYLOAD_BYTES: usize = 1_024;
    // 64 MiB, 256 MiB, 512 MiB of payload, ascending so a timeout on the
    // largest still leaves the smaller results emitted.
    const RECORD_COUNTS: &[usize] = &[64 * 1024, 256 * 1024, 512 * 1024];

    for &records in RECORD_COUNTS {
        let sink = MockSink::start();
        // Down before the first record, and never let through: every segment
        // stays sealed-awaiting-drain, which is the state a crash must recover.
        sink.set_failing(true);

        let mut srv = daemon("r_recov", &sink).start();
        let payload = vec![b'x'; PAYLOAD_BYTES];
        let mut client = srv.client();

        let fill_start = Instant::now();
        let batch: Vec<&[u8]> = vec![payload.as_slice(); 256];
        for _ in 0..(records / 256) {
            let out = client
                .push_batch(&batch, Durability::Buffered)
                .expect("push_batch");
            assert!(out.all_accepted(), "ingest rejected a record while filling");
        }
        let fill = fill_start.elapsed();

        let body = srv.scrape_metrics();
        let wab_bytes = body
            .lines()
            .find(|l| l.starts_with("weir_wab_bytes_on_disk"))
            .and_then(|l| l.split_whitespace().next_back())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(f64::NAN);
        assert!(
            wab_bytes > 0.0,
            "the WAB is empty at kill time, so this measures daemon startup and \
             not recovery: the sink is draining when it was told to fail"
        );

        // SIGKILL, not a clean shutdown: a clean stop seals and flushes, which
        // is the case an operator is not worried about.
        srv.kill_ungracefully();
        let restart = Instant::now();
        srv.restart_in_place();
        let ready = restart.elapsed();

        println!(
            "\nBENCH: {{\"scenario\":\"recovery\",\"records\":{records},\
             \"payload_bytes\":{PAYLOAD_BYTES},\"wab_bytes\":{wab_bytes:.0},\
             \"fill_ms\":{},\"ready_ms\":{}}}",
            fill.as_millis(),
            ready.as_millis(),
        );
        srv.shutdown();
    }
}

/// Is the WAB's plateau under sustained overload bounded by records or by bytes?
///
/// `wab_growth_when_ingest_outruns_the_sink` found that the buffer does not grow
/// without limit when ingest outruns the drain: it climbs to ~17 MB and stops,
/// after which the producer advances at exactly the drain rate. That is
/// backpressure reaching the producer, and it is the opposite of what A6 §8
/// worried about — but the *mechanism* was not established. The backlog at the
/// plateau came to ~64,000 records in all three configurations, against a
/// `QUEUE_CAPACITY` of 65,536, which is a correlation and not a cause.
///
/// This separates the two candidates without touching weir. Hold the sink to one
/// rate and sweep the payload:
///
/// - **record-bound** (the bounded queue): plateau *record count* stays near
///   constant and plateau *bytes* scale with the payload.
/// - **byte-bound** (a disk or segment limit): plateau *bytes* stay near
///   constant and the record count falls as the payload grows.
///
/// The two predictions diverge by 64x across this sweep, so one run decides it.
#[test]
#[ignore = "operator-run; ~4 minutes"]
fn wab_plateau_is_record_bound_or_byte_bound() {
    const PAYLOAD_SIZES: &[usize] = &[64, 256, 1_024, 4_096];
    const RUN_SECS: u64 = 25;
    const SINK_DELAY_MS: u64 = 50;

    for &size in PAYLOAD_SIZES {
        let sink = MockSink::start();
        sink.set_delay(Duration::from_millis(SINK_DELAY_MS));

        let srv = daemon("r_plateau", &sink)
            .extra_config("sink_http_batch      = \"ndjson\"")
            .extra_config("sink_max_batch_size  = 100")
            .start();
        let mut client = srv.client();

        let wab_bytes = || -> f64 {
            srv.scrape_metrics()
                .lines()
                .find(|l| l.starts_with("weir_wab_bytes_on_disk"))
                .and_then(|l| l.split_whitespace().next_back())
                .and_then(|v| v.parse().ok())
                .unwrap_or(f64::NAN)
        };

        let payload = vec![b'x'; size];
        let batch: Vec<&[u8]> = vec![payload.as_slice(); 256];
        let started = Instant::now();
        let mut pushed: u64 = 0;
        let mut peak = 0.0f64;
        let mut next_sample = Duration::from_millis(500);

        while started.elapsed() < Duration::from_secs(RUN_SECS) {
            let out = client
                .push_batch(&batch, Durability::Buffered)
                .expect("push_batch");
            assert!(out.all_accepted(), "ingest rejected a record mid-burst");
            pushed += batch.len() as u64;
            // Sample on the clock, not on a record count. Keying off
            // `pushed % 25_600` meant the 4 KiB case -- which only reaches
            // ~10k records in the window -- never sampled at all and reported a
            // plateau of 0, the one payload where the answer mattered most.
            if started.elapsed() >= next_sample {
                peak = peak.max(wab_bytes());
                next_sample += Duration::from_millis(500);
            }
        }

        let delivered = sink.delivered() as u64;
        let backlog = pushed.saturating_sub(delivered);
        println!(
            "\nBENCH: {{\"scenario\":\"wab_plateau\",\"payload_bytes\":{size},\
             \"plateau_bytes\":{peak:.0},\"backlog_records\":{backlog},\
             \"plateau_bytes_per_record\":{:.1},\"pushed\":{pushed},\
             \"delivered\":{delivered}}}",
            if backlog > 0 {
                peak / backlog as f64
            } else {
                f64::NAN
            },
        );
        srv.shutdown();
    }
}
