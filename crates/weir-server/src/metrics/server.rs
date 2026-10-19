//! HTTP server for the metrics/health listener (spec §5.8, §10).
//!
//! Every connection goes through two phases. **Head:** one of
//! `HEAD_READ_SLOTS` cheap slots, held while the request head is read
//! (≤ 4096 bytes, ≤ 1 s). **Route:** `/livez`, `/readyz`, `/startupz` and
//! `/status` take a permit from a pool of `PROBE_SLOTS`; every other path
//! takes one of `metrics_max_connections` and gets today's metrics response
//! byte for byte. Writes are bounded by `SCRAPE_IO_TIMEOUT`. No route reads a
//! body or changes state.

use std::sync::Arc;

use std::time::Duration;

use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tracing::debug;
use weir_status::{LivezCheck, Thresholds};

use crate::metrics::router::{self, Route, StatusSource};
use crate::metrics::{HttpDropReason, HttpDroppedLabel, Metrics};

/// Backoff after an accept() error, to avoid a hot spin on a persistent
/// condition (e.g. EMFILE) while keeping metrics exposition alive.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Per-connection write deadline for a response. A response is small and goes
/// to a local Prometheus or kubelet; anything slower than this is a stuck peer.
/// Bounding the write stops such a peer from parking a handler (and its
/// scrape or probe permit) forever, which would exhaust the small pool and
/// blind monitoring until restart. The read side has its own, tighter bound:
/// `HEAD_READ_TIMEOUT`.
const SCRAPE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Cheap slots for the head phase (spec §5.8 step 1). Every connection takes
/// one before anything else, and holds it for at most `HEAD_READ_TIMEOUT`.
pub(crate) const HEAD_READ_SLOTS: usize = 64;
/// A peer that has not sent a complete request head by then is closed, so
/// it can no longer hold a scrape or probe permit for `SCRAPE_IO_TIMEOUT`.
pub(crate) const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(1);
/// The request-head bound (spec §10): a head that has not ended by then is
/// not a scrape or a probe.
pub(crate) const HEAD_MAX_BYTES: usize = 4096;
/// The four health routes' own pool: a scrape storm cannot starve kubelet,
/// and a probe flood cannot starve Prometheus.
pub(crate) const PROBE_SLOTS: usize = 4;
/// After an oversized head: how long to read and discard what the peer still
/// sends after our FIN, so the close is not a RST.
const OVERSIZE_DRAIN: Duration = Duration::from_millis(100);

/// How a request-head read ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HeadRead {
    /// `\r\n\r\n` arrived. Holds every byte read.
    Complete(Vec<u8>),
    /// `max` bytes and no terminator.
    TooLarge(Vec<u8>),
    /// The peer half-closed before a terminator. Possibly empty.
    Eof(Vec<u8>),
    /// The deadline passed first.
    TimedOut,
    /// A read error.
    Failed,
}

/// Reads until `\r\n\r\n`, `max` bytes, EOF or `deadline`, whichever comes
/// first. Reading the whole head also drains the request headers, so a close
/// after responding sends FIN, not RST (spec §5.8).
pub(crate) async fn read_head<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
    deadline: Duration,
) -> HeadRead {
    let read = async move {
        let mut buf: Vec<u8> = Vec::with_capacity(512);
        let mut chunk = [0u8; 1024];
        loop {
            let room = max.saturating_sub(buf.len()).min(chunk.len());
            let n = match r.read(&mut chunk[..room]).await {
                Ok(n) => n,
                Err(_) => return HeadRead::Failed,
            };
            if n == 0 {
                return HeadRead::Eof(buf);
            }
            // Re-scan 3 bytes of overlap: the terminator may straddle reads.
            let from = buf.len().saturating_sub(3);
            buf.extend_from_slice(&chunk[..n]);
            if buf[from..].windows(4).any(|w| w == b"\r\n\r\n") {
                return HeadRead::Complete(buf);
            }
            if buf.len() >= max {
                return HeadRead::TooLarge(buf);
            }
        }
    };
    match tokio::time::timeout(deadline, read).await {
        Ok(outcome) => outcome,
        Err(_) => HeadRead::TimedOut,
    }
}

/// The listener's three connection pools (spec §5.8). Every connection takes
/// a `head` slot first; after the head it takes a `metrics` permit (a scrape)
/// or a `probe` permit (the four health routes). All three are `try_acquire`
/// only: a full pool closes the connection, it never queues it.
#[derive(Clone)]
pub(crate) struct HttpPools {
    pub head: Arc<Semaphore>,
    pub metrics: Arc<Semaphore>,
    pub probe: Arc<Semaphore>,
}

impl HttpPools {
    pub(crate) fn new(metrics_max_connections: usize) -> Self {
        Self {
            head: Arc::new(Semaphore::new(HEAD_READ_SLOTS)),
            metrics: Arc::new(Semaphore::new(metrics_max_connections)),
            probe: Arc::new(Semaphore::new(PROBE_SLOTS)),
        }
    }
}

/// Everything a connection handler reads. Cloned per connection: every
/// field is an `Arc` or `Copy`.
#[derive(Clone)]
pub(crate) struct HttpState {
    pub registry: Arc<Registry>,
    /// `None` only in tests: drops are then not counted.
    pub metrics: Option<Arc<Metrics>>,
    /// `None`: every path gets metrics (the pre-router contract).
    pub status: Option<Arc<dyn StatusSource>>,
    pub thresholds: Thresholds,
    pub livez_checks: Arc<[LivezCheck]>,
    pub pools: HttpPools,
}

impl HttpState {
    pub(crate) fn new(
        registry: Arc<Registry>,
        metrics: Option<Arc<Metrics>>,
        status: Option<Arc<dyn StatusSource>>,
        livez_checks: Vec<LivezCheck>,
        metrics_max_connections: usize,
    ) -> Self {
        Self {
            registry,
            metrics,
            status,
            thresholds: Thresholds::default(),
            livez_checks: livez_checks.into(),
            pools: HttpPools::new(metrics_max_connections),
        }
    }

    /// No status source and no drop counting: every path gets the metrics
    /// response, as before the router (spec §5.8).
    #[cfg(test)]
    pub(crate) fn metrics_only(registry: Arc<Registry>, metrics_max_connections: usize) -> Self {
        Self::new(registry, None, None, Vec::new(), metrics_max_connections)
    }

    fn count_drop(&self, reason: HttpDropReason) {
        if let Some(m) = &self.metrics {
            m.http_requests_dropped
                .get_or_create(&HttpDroppedLabel { reason })
                .inc();
        }
    }
}

/// Spawns the server as a tokio task. It never ends on its own (G14): it
/// owns `listener`, and an accept error is logged and retried after a short
/// backoff rather than ending it. It stops when its runtime shuts down or the
/// handle is aborted. The handle can be ignored if the server should run for
/// the lifetime of the process.
///
/// # Tokio runtime
///
/// Must be called from within a tokio runtime context. The spawned task runs
/// on the same runtime.
pub(crate) fn spawn(listener: TcpListener, state: HttpState) -> JoinHandle<()> {
    tokio::spawn(serve(listener, state))
}

async fn serve(listener: TcpListener, state: HttpState) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                // A transient accept error (ECONNABORTED, or resource exhaustion
                // like EMFILE) must NOT take metrics exposition down permanently.
                // Log, back off briefly to avoid a hot spin on a persistent
                // error, and continue; Prometheus retries on its next scrape
                // regardless (G14).
                debug!(error = %e, "metrics: accept error; backing off and continuing");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        // try_acquire never blocks: a full head pool closes the connection
        // at once (the stream drops here, so the kernel closes it).
        let Ok(head_permit) = Arc::clone(&state.pools.head).try_acquire_owned() else {
            state.count_drop(HttpDropReason::head_pool_full);
            continue;
        };
        let state = state.clone();
        tokio::spawn(handle(stream, state, head_permit));
    }
}

async fn handle(mut stream: TcpStream, state: HttpState, head_permit: OwnedSemaphorePermit) {
    let head = read_head(&mut stream, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await;
    // The head slot is held for the head phase only, never into the route.
    drop(head_permit);
    let (bytes, oversized) = match head {
        HeadRead::Complete(b) => (b, false),
        // A peer that sent something and half-closed still gets an answer,
        // as before the router (`endpoint_survives_problematic_connections`).
        HeadRead::Eof(b) if !b.is_empty() => (b, false),
        HeadRead::TooLarge(b) => (b, true),
        HeadRead::Eof(_) | HeadRead::Failed => return,
        HeadRead::TimedOut => {
            state.count_drop(HttpDropReason::head_timeout);
            return;
        }
    };
    let route = match &state.status {
        None => Route::Metrics,
        Some(_) => router::route(&bytes),
    };
    match route {
        Route::Metrics => {
            let Ok(_permit) = Arc::clone(&state.pools.metrics).try_acquire_owned() else {
                state.count_drop(HttpDropReason::metrics_pool_full);
                return;
            };
            write_metrics(&mut stream, &state.registry).await;
        }
        Route::Probe { .. } | Route::MethodNotAllowed => {
            let Ok(_permit) = Arc::clone(&state.pools.probe).try_acquire_owned() else {
                state.count_drop(HttpDropReason::probe_pool_full);
                return;
            };
            let Some(source) = state.status.as_deref() else {
                return;
            };
            let (response, head_only) = match route {
                Route::Probe {
                    endpoint,
                    head_only,
                    verbose,
                } => (
                    router::respond(
                        source,
                        endpoint,
                        verbose,
                        std::time::SystemTime::now(),
                        &state.livez_checks,
                        &state.thresholds,
                    ),
                    head_only,
                ),
                _ => (router::render_method_not_allowed(), false),
            };
            let bytes = response.encode(head_only);
            let _ = tokio::time::timeout(SCRAPE_IO_TIMEOUT, stream.write_all(&bytes)).await;
        }
    }
    if oversized {
        finish_oversized(&mut stream).await;
    }
}

/// Today's `handle()` response, unchanged: same head, same body, no new
/// headers (spec §10), writes bounded by `SCRAPE_IO_TIMEOUT` (F20), so a
/// peer that stops reading mid-response cannot pin the handler or its permit.
async fn write_metrics(stream: &mut TcpStream, registry: &Registry) {
    let mut body = String::new();
    if encode(&mut body, registry).is_err() {
        let _ = tokio::time::timeout(
            SCRAPE_IO_TIMEOUT,
            stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\n\r\n"),
        )
        .await;
        return;
    }
    let body_bytes = body.as_bytes();
    let response_head = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         \r\n",
        body_bytes.len()
    );
    let write = async {
        stream.write_all(response_head.as_bytes()).await?;
        stream.write_all(body_bytes).await
    };
    let _ = tokio::time::timeout(SCRAPE_IO_TIMEOUT, write).await;
}

/// The head hit 4096 bytes, so the peer may still be sending: FIN our half,
/// then read and discard for up to 100 ms, so the close is not a RST that
/// destroys the response in the peer's receive buffer (spec §5.8).
async fn finish_oversized(stream: &mut TcpStream) {
    let _ = stream.shutdown().await;
    let mut sink = [0u8; 1024];
    let _ = tokio::time::timeout(OVERSIZE_DRAIN, async {
        while let Ok(n) = stream.read(&mut sink).await {
            if n == 0 {
                break;
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn scrape(addr: std::net::SocketAddr) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn endpoint_returns_200_ok() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::new(registry), 8));

        let response = scrape(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "unexpected response: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_content_type_is_openmetrics() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::new(registry), 8));

        let response = scrape(addr).await;
        assert!(
            response.contains("Content-Type: application/openmetrics-text"),
            "unexpected Content-Type in: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_body_contains_metric_names() {
        let (metrics, registry) = Metrics::new();
        metrics.recovery_records_replayed.inc_by(7);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::new(registry), 8));

        let response = scrape(addr).await;
        assert!(
            response.contains("weir_recovery_records_replayed_total 7"),
            "counter value not in response: {response}"
        );
        assert!(
            response.contains("weir_drain_state"),
            "drain_state metric missing: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_caps_concurrent_connections() {
        // The cap is unchanged: with every scrape permit taken, a complete scrape
        // is closed without a response. What changed (spec §5.8) is WHEN a permit
        // is taken: after the request head, so the test takes it itself instead
        // of parking an idle peer on it (see idle_peer_holds_only_a_head_slot).
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = HttpState::metrics_only(Arc::new(registry), 1);
        let _held = Arc::clone(&state.pools.metrics)
            .try_acquire_owned()
            .unwrap();
        let _handle = spawn(listener, state);

        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let read = tokio::time::timeout(Duration::from_millis(500), conn.read(&mut buf))
            .await
            .expect("the server closes at once when the scrape pool is full")
            .expect("a clean close, not a reset");
        assert_eq!(read, 0, "got {read} bytes: {:?}", &buf[..read]);
    }

    #[tokio::test]
    async fn endpoint_handles_multiple_requests() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::new(registry), 8));

        for _ in 0..3 {
            let response = scrape(addr).await;
            assert!(response.starts_with("HTTP/1.1 200 OK"));
        }
    }

    /// S34: the accept loop keeps serving after connections that fail at the
    /// handler level — each problematic connection is isolated to its own task and
    /// never takes the loop down. The OS-level `accept()` error branch
    /// (ECONNABORTED / EMFILE, the G14 `continue`) cannot be injected against a
    /// real `TcpListener` without an fd-exhaustion hack or an accept-source
    /// abstraction in production; this test pins the observable survival property
    /// that fix exists to protect.
    #[tokio::test]
    async fn endpoint_survives_problematic_connections() {
        use tokio::io::AsyncWriteExt;
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::new(registry), 8));

        // A connection that opens then immediately closes without a request.
        drop(TcpStream::connect(addr).await.unwrap());
        // A connection that sends a few junk bytes then closes mid-stream.
        {
            let mut bad = TcpStream::connect(addr).await.unwrap();
            let _ = bad.write_all(b"GARBAGE not http").await;
        }
        // Let the server accept and dispatch both before we scrape.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The loop must still be alive and serving.
        let response = scrape(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "metrics server stopped serving after problematic connections: {response}"
        );
    }

    use std::time::Instant;
    use tokio::io::duplex;

    #[tokio::test]
    async fn head_split_across_writes_and_across_the_terminator_is_complete() {
        let (mut client, mut server) = duplex(8192);
        let reader =
            tokio::spawn(
                async move { read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await },
            );
        client.write_all(b"GET /readyz HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"Host: x\r\n\r").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"\n").await.unwrap();
        assert_eq!(
            reader.await.unwrap(),
            HeadRead::Complete(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n".to_vec())
        );
    }

    #[tokio::test]
    async fn head_without_terminator_stops_at_the_byte_bound() {
        let (mut client, mut server) = duplex(16384);
        client.write_all(&[b'a'; 5000]).await.unwrap();
        match read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await {
            HeadRead::TooLarge(b) => assert_eq!(b.len(), HEAD_MAX_BYTES),
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn silent_peer_times_out_at_the_deadline() {
        let (mut client, mut server) = duplex(1024);
        client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap(); // no blank line, ever
        let t = Instant::now();
        assert_eq!(
            read_head(&mut server, HEAD_MAX_BYTES, Duration::from_millis(200)).await,
            HeadRead::TimedOut
        );
        let e = t.elapsed();
        assert!(
            e >= Duration::from_millis(190) && e < Duration::from_millis(800),
            "{e:?}"
        );
        drop(client);
    }

    #[tokio::test]
    async fn half_closed_peer_returns_what_it_sent() {
        let (mut client, mut server) = duplex(1024);
        client.write_all(b"GARBAGE not http").await.unwrap();
        drop(client);
        assert_eq!(
            read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await,
            HeadRead::Eof(b"GARBAGE not http".to_vec())
        );
    }

    /// The head-phase constants are spec constants, not knobs (spec §13;
    /// plan Global Constraints): pinned here so a change is deliberate.
    #[test]
    fn head_phase_constants_are_the_spec_s() {
        assert_eq!(HEAD_READ_SLOTS, 64);
        assert_eq!(HEAD_READ_TIMEOUT, Duration::from_secs(1));
        assert_eq!(HEAD_MAX_BYTES, 4096);
        assert_eq!(PROBE_SLOTS, 4);
        // The post-oversize discard stays well inside the head deadline.
        assert!(OVERSIZE_DRAIN < HEAD_READ_TIMEOUT);
    }

    // ── Task 5.8: the head phase and the split pools ─────────────────────────

    use crate::metrics::router::{Endpoint, ProbeAnswer, StartupFacts, StatusSource};
    use crate::metrics::{HttpDropReason, HttpDroppedLabel};
    use std::sync::atomic::{AtomicBool, Ordering};
    use weir_status::{
        ConditionCode, LivezCheck, ProbeCheck, ProbeVerdict, PublicStatusDoc, Thresholds,
    };

    const GOLDEN_ADMIN: &str =
        include_str!("../../../weir-status/tests/golden/status_v1_admin.json");

    struct FakeSource {
        ready: AtomicBool,
    }

    impl StatusSource for FakeSource {
        fn probe(
            &self,
            endpoint: Endpoint,
            _now: std::time::SystemTime,
            _c: &[LivezCheck],
            _t: &Thresholds,
        ) -> ProbeAnswer {
            let ok = endpoint != Endpoint::Readyz || self.ready.load(Ordering::Relaxed);
            ProbeAnswer {
                verdict: ProbeVerdict {
                    ok,
                    failing: if ok {
                        vec![]
                    } else {
                        vec![ConditionCode::Starting]
                    },
                    checks: vec![ProbeCheck {
                        code: ConditionCode::Starting,
                        ok,
                        summary: None,
                    }],
                },
                summaries: vec![(
                    ConditionCode::Starting,
                    "startup phase recovering for 3s".into(),
                )],
                startup: StartupFacts {
                    phase: "serving",
                    phase_age_secs: 1,
                    replay_remaining: Some(0),
                },
            }
        }

        fn public_doc(
            &self,
            _now: std::time::SystemTime,
            _c: &[LivezCheck],
            _t: &Thresholds,
        ) -> PublicStatusDoc {
            weir_status::public(&serde_json::from_str(GOLDEN_ADMIN).unwrap())
        }
    }

    /// A router with a status source and counted drops.
    fn routed(max: usize) -> (HttpState, Arc<Metrics>) {
        let (m, reg) = Metrics::new();
        let m = Arc::new(m);
        let source: Arc<dyn StatusSource> = Arc::new(FakeSource {
            ready: AtomicBool::new(true),
        });
        (
            HttpState::new(
                Arc::new(reg),
                Some(Arc::clone(&m)),
                Some(source),
                Vec::new(),
                max,
            ),
            m,
        )
    }

    fn dropped(m: &Metrics, reason: HttpDropReason) -> u64 {
        m.http_requests_dropped
            .get_or_create(&HttpDroppedLabel { reason })
            .get()
    }

    async fn start(state: HttpState) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, state);
        addr
    }

    async fn exchange(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(request).await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out))
            .await
            .expect("response within 5 s")
            .expect("a full response, not a reset");
        out
    }

    async fn idle(addr: std::net::SocketAddr, n: usize) -> Vec<TcpStream> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(TcpStream::connect(addr).await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(100)).await; // let the server accept them all
        v
    }

    /// Today's `handle()` response, for comparison: head and body format.
    fn legacy_metrics_response(registry: &Registry) -> Vec<u8> {
        let mut body = String::new();
        prometheus_client::encoding::text::encode(&mut body, registry).unwrap();
        let mut out = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
             Content-Length: {}\r\n\
             \r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body.as_bytes());
        out
    }

    #[tokio::test]
    async fn metrics_paths_are_byte_identical_to_the_pre_router_response() {
        let (state, _m) = routed(8);
        let registry = Arc::clone(&state.registry);
        let addr = start(state).await;
        for path in [
            "/metrics",
            "/",
            "/foo",
            "/READYZ",
            "/readyz/",
            "/metrics?x=1",
        ] {
            let got = exchange(addr, format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes()).await;
            assert_eq!(
                String::from_utf8_lossy(&got),
                String::from_utf8_lossy(&legacy_metrics_response(&registry)),
                "path {path}"
            );
        }
    }

    #[tokio::test]
    async fn routes_answer_get_head_and_405() {
        let (state, _m) = routed(8);
        let addr = start(state).await;
        let livez =
            String::from_utf8(exchange(addr, b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n").await)
                .unwrap();
        assert!(livez.starts_with("HTTP/1.1 200 OK\r\n"), "{livez}");
        assert!(
            livez.contains("\r\nCache-Control: no-store\r\nConnection: close\r\n"),
            "{livez}"
        );
        assert!(livez.ends_with("\r\n\r\nok\n"), "{livez}");
        let head =
            String::from_utf8(exchange(addr, b"HEAD /readyz HTTP/1.1\r\n\r\n").await).unwrap();
        assert!(
            head.contains("Content-Length: 3\r\n") && head.ends_with("\r\n\r\n"),
            "{head}"
        );
        let post = String::from_utf8(
            exchange(addr, b"POST /status HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await,
        )
        .unwrap();
        assert!(
            post.starts_with("HTTP/1.1 405 Method Not Allowed\r\n")
                && post.contains("Allow: GET, HEAD\r\n"),
            "{post}"
        );
        let status =
            String::from_utf8(exchange(addr, b"GET /status HTTP/1.1\r\n\r\n").await).unwrap();
        assert!(
            status.contains("Content-Type: application/json; charset=utf-8\r\n"),
            "{status}"
        );
        let body = &status[status.find("\r\n\r\n").unwrap() + 4..];
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            (v["schema"].as_str(), v["view"].as_str()),
            (Some("weir.status/v1"), Some("public"))
        );
    }

    #[tokio::test]
    async fn request_head_split_across_writes_gets_a_full_response() {
        let (state, _m) = routed(8);
        let addr = start(state).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /metrics HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        s.write_all(b"Host: x\r\nAccept: */*\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out)
            .await
            .expect("FIN after the full head, not RST");
        let text = String::from_utf8_lossy(&out);
        let split = text.find("\r\n\r\n").unwrap();
        let len: usize = text[..split]
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(out.len() - split - 4, len, "the whole body arrived");
    }

    #[tokio::test]
    async fn silent_peer_is_closed_after_the_head_deadline() {
        let (state, m) = routed(8);
        let addr = start(state).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        let t = std::time::Instant::now();
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0);
        let e = t.elapsed();
        assert!(
            e >= Duration::from_millis(900) && e < Duration::from_millis(2000),
            "{e:?}"
        );
        assert_eq!(dropped(&m, HttpDropReason::head_timeout), 1);
    }

    #[tokio::test]
    async fn idle_peer_holds_only_a_head_slot() {
        // The intended change behind the rewritten endpoint_caps_concurrent_connections:
        // with metrics_max_connections = 1, an idle peer no longer blocks the scrape.
        let (state, _m) = routed(1);
        let addr = start(state).await;
        let _idle = idle(addr, 1).await;
        let got = exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n").await;
        assert!(got.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn eight_idle_connections_do_not_prevent_a_readyz_200() {
        let (state, _m) = routed(8);
        let addr = start(state).await;
        let _idle = idle(addr, 8).await;
        let t = std::time::Instant::now();
        let got = String::from_utf8(exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n").await).unwrap();
        assert!(
            got.starts_with("HTTP/1.1 200 OK\r\n") && got.ends_with("ok\n"),
            "{got}"
        );
        assert!(t.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn sixty_four_idle_connections_are_shed_within_a_second() {
        let (state, m) = routed(8);
        let addr = start(state).await;
        let mut held = idle(addr, HEAD_READ_SLOTS).await;
        // The head pool is full: a 65th connection is closed at once, uncounted
        // against any scrape or probe permit.
        let mut extra = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_millis(300), extra.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0);
        assert!(dropped(&m, HttpDropReason::head_pool_full) >= 1);
        // Every idle peer is closed by the head deadline.
        let t = std::time::Instant::now();
        for s in &mut held {
            let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(n, 0);
        }
        assert!(
            t.elapsed() < Duration::from_millis(1500),
            "{:?}",
            t.elapsed()
        );
        let got = exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n").await;
        assert!(got.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn a_full_scrape_pool_does_not_block_probes() {
        let (state, m) = routed(1);
        let _held = Arc::clone(&state.pools.metrics)
            .try_acquire_owned()
            .unwrap();
        let addr = start(state).await;
        assert!(
            exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n")
                .await
                .starts_with(b"HTTP/1.1 200 OK\r\n")
        );
        assert!(
            exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n")
                .await
                .is_empty()
        );
        assert_eq!(dropped(&m, HttpDropReason::metrics_pool_full), 1);
    }

    #[tokio::test]
    async fn a_full_probe_pool_does_not_block_scrapes() {
        let (state, m) = routed(8);
        let _held: Vec<_> = (0..PROBE_SLOTS)
            .map(|_| Arc::clone(&state.pools.probe).try_acquire_owned().unwrap())
            .collect();
        let addr = start(state).await;
        assert!(
            exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n")
                .await
                .starts_with(b"HTTP/1.1 200 OK\r\n")
        );
        assert!(
            exchange(addr, b"GET /livez HTTP/1.1\r\n\r\n")
                .await
                .is_empty()
        );
        assert_eq!(dropped(&m, HttpDropReason::probe_pool_full), 1);
    }

    #[tokio::test]
    async fn oversized_head_gets_a_response_then_a_clean_close() {
        let (state, _m) = routed(8);
        let addr = start(state).await;
        let mut req = b"GET /livez HTTP/1.1\r\nX-Pad: ".to_vec();
        req.extend(std::iter::repeat_n(b'a', 6000));
        let got = exchange(addr, &req).await;
        assert!(
            got.starts_with(b"HTTP/1.1 200 OK\r\n") && got.ends_with(b"ok\n"),
            "{}",
            String::from_utf8_lossy(&got)
        );
    }

    #[tokio::test]
    async fn the_head_slot_is_released_before_the_route_phase() {
        // Spec §5.8: the head slot covers the head phase only. An oversized
        // head puts the handler into its 100 ms post-response drain; the peer
        // keeps its half open, so the handler is still running when the
        // response's FIN arrives. Its head slot must already be free.
        let (state, _m) = routed(8);
        let head = Arc::clone(&state.pools.head);
        let addr = start(state).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        let mut req = b"GET /livez HTTP/1.1\r\nX-Pad: ".to_vec();
        req.extend(std::iter::repeat_n(b'a', 6000));
        s.write_all(&req).await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out))
            .await
            .expect("response within 5 s")
            .expect("a full response, not a reset");
        assert!(out.starts_with(b"HTTP/1.1 200 OK\r\n"));
        // `s` is still open, so the handler is inside its drain window.
        assert_eq!(
            head.available_permits(),
            HEAD_READ_SLOTS,
            "the head slot was held past the head phase"
        );
        drop(s);
    }

    #[tokio::test]
    async fn metrics_only_keeps_the_any_path_contract() {
        let (_m, registry) = Metrics::new();
        let registry = Arc::new(registry);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, HttpState::metrics_only(Arc::clone(&registry), 8));
        for path in ["/readyz", "/status", "/livez?verbose"] {
            let got = exchange(addr, format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes()).await;
            assert_eq!(got, legacy_metrics_response(&registry), "{path}");
        }
    }
}
