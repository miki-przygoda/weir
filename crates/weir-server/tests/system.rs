//! System integration tests — exercises the real `weir-server` binary.
//!
//! Each test uses [`WeirServer`] (from `weir-testkit`) to spawn the binary,
//! wait for the socket to be ready, and clean everything up on drop. Tests are
//! independent: each gets its own temp directory, socket path, WAB dir, and
//! metrics port so they can run in parallel without interference.
//!
//! # Running
//!
//! Each test spawns a real OS process with several threads. Running all tests
//! with maximum parallelism can exhaust OS resources on dev machines. Use a
//! bounded thread count to keep things stable:
//!
//! ```sh
//! cargo test -p weir-server --test system -- --test-threads=4
//! ```
//!
//! These tests are Unix-only because weir-server only binds Unix sockets.

#![cfg(unix)]

use std::{
    fs,
    io,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use weir_client::{ClientError, WeirClient};
use weir_core::Durability;
use weir_testkit::{free_port, process_lock, weir_server};

// Helper: sum all file bytes under a directory tree.
fn wab_dir_bytes(dir: &Path) -> u64 {
    let Ok(rd) = fs::read_dir(dir) else { return 0 };
    let mut total = 0u64;
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            total += wab_dir_bytes(&p);
        } else {
            total += fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

// Helper: count .wab and .wab.sealed files in a directory tree.
fn count_wab_files(dir: &Path, ext: &str) -> usize {
    let Ok(rd) = fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0;
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            count += count_wab_files(&p, ext);
        } else if p.to_str().map(|s| s.ends_with(ext)).unwrap_or(false) {
            count += 1;
        }
    }
    count
}

// Helper: collect every byte from all files under a directory tree.
fn read_wab_bytes(dir: &Path) -> Vec<u8> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.extend_from_slice(&read_wab_bytes(&p));
        } else if let Ok(bytes) = fs::read(&p) {
            out.extend_from_slice(&bytes);
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// ── Basic push / ack ──────────────────────────────────────────────────────────

#[test]
fn smoke_single_push_ack() {
    let srv = weir_server!("smoke").start();
    let mut client = srv.client();
    client.push(b"hello weir", Durability::Sync).unwrap();
}

#[test]
fn all_durability_tiers_behave_per_contract() {
    // Strengthened from `all_durability_tiers_acked`: the original test
    // only checked that each tier returned Ok, which would still pass
    // if Sync silently skipped fsync or Buffered silently fsynced. This
    // version reads the `weir_wab_fsync_duration_seconds_count`
    // histogram counter, which is incremented exactly once per fsync
    // syscall — a deterministic differential between the tiers.
    const N_PER_TIER: u32 = 10;

    let srv = weir_server!("durability").start();
    let mut client = srv.client();

    let read_fsync_count = || -> u64 {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_wab_fsync_duration_seconds_count",
        )
    };

    // Buffered first, while the WAB flushers are quiet. Buffered acks before
    // any fsync, so the counter must not move on its account.
    let fsync_before = read_fsync_count();
    for i in 0..N_PER_TIER {
        client
            .push(format!("buf-{i}").as_bytes(), Durability::Buffered)
            .expect("Buffered push failed");
    }
    // Give the metrics aggregator a beat in case any background work runs.
    thread::sleep(Duration::from_millis(150));
    let fsync_after_buffered = read_fsync_count();
    assert!(
        fsync_after_buffered - fsync_before <= 1,
        "Buffered tier triggered {} fsyncs over {N_PER_TIER} pushes — \
         expected 0 (a single drift is tolerated for unrelated background work)",
        fsync_after_buffered - fsync_before
    );

    // Sync: every push forces fdatasync. Counter must climb by ≥ N.
    for i in 0..N_PER_TIER {
        client
            .push(format!("sync-{i}").as_bytes(), Durability::Sync)
            .expect("Sync push failed");
    }
    let fsync_after_sync = read_fsync_count();
    assert!(
        fsync_after_sync - fsync_after_buffered >= u64::from(N_PER_TIER),
        "Sync tier produced only {} fsyncs for {N_PER_TIER} pushes — \
         expected ≥ {N_PER_TIER} (one per record)",
        fsync_after_sync - fsync_after_buffered
    );

    // Batched: group fdatasync per batch. Under *serial* push (each call
    // waits for ack before the next) the batch only ever contains a single
    // record, so the deadline timer fires once per record and Batched looks
    // identical to Sync. The meaningful regression to catch here is
    // "Batched silently skips fsync" — we'd see zero new fsyncs in that
    // case. The records-per-fsync compression Batched provides under
    // concurrent load is exercised by the `compression_ratio_*` load
    // scenario, not by this test.
    for i in 0..N_PER_TIER {
        client
            .push(format!("bat-{i}").as_bytes(), Durability::Batched)
            .expect("Batched push failed");
    }
    thread::sleep(Duration::from_millis(150));
    let fsync_after_batched = read_fsync_count();
    assert!(
        fsync_after_batched - fsync_after_sync >= 1,
        "Batched tier produced 0 fsyncs over {N_PER_TIER} pushes — records were not durably flushed"
    );
}

// ── Health check ──────────────────────────────────────────────────────────────

#[test]
fn health_check_returns_ok() {
    let srv = weir_server!("health").start();
    let mut client = srv.client();
    client.health_check().unwrap();
}

// ── Concurrent producers ──────────────────────────────────────────────────────

#[test]
fn concurrent_producers_all_acked() {
    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 100;

    let srv = weir_server!("concurrent").start();
    let socket_path = srv.socket_path.clone();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect failed: {e}"));
                for i in 0..RECORDS_PER_THREAD {
                    let payload = format!("thread-{t:02}-record-{i:04}");
                    client
                        .push(payload.as_bytes(), Durability::Batched)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    let mut failures = 0usize;
    for h in handles {
        if h.join().is_err() {
            failures += 1;
        }
    }
    assert_eq!(failures, 0, "{failures}/{THREADS} producer threads failed");

    // Strengthen: a thread that silently drops half its pushes wouldn't
    // panic but would land here with records_ack < THREADS*RECORDS_PER_THREAD.
    // The records_ack counter is incremented *before* the Ack frame is sent
    // (see send_ack call site in src/socket/connection.rs), so it's
    // synchronously consistent with what the client observed.
    let body = srv.scrape_metrics();
    let acked = parse_metric(&body, "weir_records_ack_total{tier=\"batched\"}");
    let expected = (THREADS * RECORDS_PER_THREAD) as u64;
    assert_eq!(
        acked,
        expected,
        "expected {expected} batched acks, got {acked} — \
         {} records appear to have been dropped silently",
        expected - acked
    );
}

#[test]
fn many_connections_open_simultaneously() {
    // 200 is high enough to genuinely exercise the semaphore-based connection
    // cap (default max_connections = 256, so 200 lives inside the cap with
    // headroom). 20 — the original count — was so far below the cap that a
    // broken semaphore could only have leaked to a count > 256 to fail the
    // test, which is not what this test should be guarding against.
    const CONN_COUNT: usize = 200;

    let srv = weir_server!("many_conns").start();

    // Open all connections first, then push from each, to exercise the
    // semaphore-based connection cap under load.
    //
    // On macOS kern.ipc.somaxconn=128 caps the listen backlog. A tight connect
    // loop can fill it faster than the server's accept loop drains it, causing
    // ECONNREFUSED. Retry with a 1 ms back-off until the server catches up or
    // the 5-second deadline expires.
    let clients: Vec<WeirClient> = (0..CONN_COUNT)
        .map(|i| {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match WeirClient::connect(&srv.socket_path) {
                    Ok(c) => break c,
                    Err(ClientError::Io(ref e))
                        if e.kind() == io::ErrorKind::ConnectionRefused
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => panic!("connect {i}: {e}"),
                }
            }
        })
        .collect();

    for (i, mut client) in clients.into_iter().enumerate() {
        client
            .push(format!("conn-{i}").as_bytes(), Durability::Buffered)
            .unwrap_or_else(|e| panic!("push on conn {i}: {e}"));
    }
}

// ── WAB on-disk verification ──────────────────────────────────────────────────

#[test]
fn records_written_to_wab_on_disk() {
    let srv = weir_server!("wab_disk").start();
    let mut client = srv.client();

    for i in 0..20u32 {
        client
            .push(format!("wab-record-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    // Give the flusher thread a moment to write.
    thread::sleep(Duration::from_millis(100));

    let active = count_wab_files(&srv.wab_dir, ".wab");
    let sealed = count_wab_files(&srv.wab_dir, ".wab.sealed");
    assert!(
        active + sealed > 0,
        "no WAB files found in {} after 20 pushes",
        srv.wab_dir.display()
    );
}

// ── Metrics accuracy ──────────────────────────────────────────────────────────

#[test]
fn metrics_endpoint_responds_with_openmetrics_content() {
    let srv = weir_server!("metrics_up").start();
    let body = srv.scrape_metrics();
    assert!(!body.is_empty(), "metrics endpoint returned empty body");
    // OpenMetrics text format always ends with EOF marker.
    assert!(
        body.contains("weir_") || body.contains("# EOF"),
        "metrics body does not look like OpenMetrics: {body:.200}"
    );
}

#[test]
fn metrics_all_families_registered() {
    let srv = weir_server!("metrics_families").start();
    let body = srv.scrape_metrics();

    // Every metric family must appear as a `# HELP weir_<name>` line.
    // Data lines only appear for pre-initialised gauges and histograms;
    // counter families that haven't been incremented yet show only HELP/TYPE.
    //
    // The list below is the SINGLE source of truth for what /metrics is
    // expected to expose. The test additionally asserts the count of
    // distinct `# HELP weir_` lines matches the list length, so adding a
    // new metric to `metrics/mod.rs` without updating this list fails
    // here loudly — preventing the "test passes because we forgot to
    // assert on the new metric" failure mode.
    let expected: &[&str] = &[
        // Wire/socket layer
        "weir_records_accepted",
        "weir_records_ack",
        "weir_records_nack",
        "weir_accept_latency_seconds",
        "weir_connection_idle_timeout",
        "weir_connection_rejected_peer_uid",
        "weir_connections_aborted_at_shutdown",
        "weir_ack_timeout",
        // WAB
        "weir_wab_segments",
        "weir_wab_bytes_on_disk",
        "weir_wab_fsync_duration_seconds",
        "weir_wab_flusher_panics",
        "weir_wab_fsync_failures",
        // Sink / drain
        "weir_sink_commit_duration_seconds",
        "weir_sink_commit_records",
        "weir_sink_health",
        "weir_queue_depth",
        // Recovery
        "weir_recovery_records_replayed",
        "weir_recovery_segments_quarantined",
        "weir_wab_unexpected_mode",
        // Dead letter
        "weir_dead_letter_bytes_on_disk",
        "weir_dead_letter_full",
        "weir_drain_state",
        "weir_dead_letter_blocked_duration_seconds",
    ];

    for family in expected {
        assert!(
            body.contains(&format!("# HELP {family}")),
            "metric family not registered in /metrics: {family}"
        );
    }

    // Count the actual `# HELP weir_` lines and assert it matches the list
    // length. This is the "drift detector": a new metric added without
    // updating the expected list fails here.
    let actual_help_count = body
        .lines()
        .filter(|l| l.starts_with("# HELP weir_"))
        .count();
    assert_eq!(
        actual_help_count,
        expected.len(),
        "metric family count mismatch: /metrics has {actual_help_count} \
         `# HELP weir_` lines but the expected list has {}. \
         Did a new metric land in metrics/mod.rs without being added to \
         this test? Diff the two sets to find which one.",
        expected.len()
    );
}

#[test]
fn drain_state_shows_draining_and_not_blocked() {
    let srv = weir_server!("drain_state").start();
    let body = srv.scrape_metrics();

    // weir_drain_state is pre-initialised so all label values appear on the
    // first scrape with exactly one set to 1.
    assert!(
        body.contains("weir_drain_state{state=\"draining\"} 1"),
        "drain should be in Draining state on startup; body:\n{body:.500}"
    );
    assert!(
        body.contains("weir_drain_state{state=\"retrying_transient\"} 0"),
        "retrying_transient should be 0 on startup"
    );
    assert!(
        body.contains("weir_drain_state{state=\"blocked_dead_letter_full\"} 0"),
        "blocked_dead_letter_full should be 0 on startup"
    );
}

#[test]
fn sink_health_shows_healthy_via_noop_sink() {
    let srv = weir_server!("sink_health").start();
    let body = srv.scrape_metrics();

    // NoopSink always reports Healthy; weir_sink_health is pre-initialised.
    assert!(
        body.contains("weir_sink_health{state=\"healthy\"} 1"),
        "NoopSink should report Healthy; body:\n{body:.500}"
    );
    assert!(
        body.contains("weir_sink_health{state=\"degraded\"} 0"),
        "degraded should be 0 with NoopSink"
    );
    assert!(
        body.contains("weir_sink_health{state=\"down\"} 0"),
        "down should be 0 with NoopSink"
    );
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

#[test]
fn server_shuts_down_cleanly_on_sigterm() {
    // 5 s is generous — clean shutdown on an idle daemon should finish in
    // tens of milliseconds. Without this bound, a shutdown that hung for
    // any duration short of cargo's per-test timeout would pass.
    const SHUTDOWN_BUDGET: Duration = Duration::from_secs(5);

    let mut srv = weir_server!("shutdown").start();
    let mut client = srv.client();
    client.push(b"before-shutdown", Durability::Sync).unwrap();
    drop(client);

    let elapsed = srv.sigterm();
    assert!(
        elapsed < SHUTDOWN_BUDGET,
        "SIGTERM took {elapsed:?} — exceeded {SHUTDOWN_BUDGET:?} budget; \
         shutdown is slow or hanging"
    );
}

#[test]
fn server_exits_and_socket_disappears_after_sigterm() {
    let srv = weir_server!("socket_gone").start();
    let socket_path = srv.socket_path.clone();

    assert!(socket_path.exists(), "socket should exist before shutdown");
    srv.shutdown();

    // The daemon removes its socket on clean exit.
    assert!(
        !socket_path.exists(),
        "socket should be removed after clean shutdown"
    );
}

// ── Reconnect / restart ───────────────────────────────────────────────────────

#[test]
fn new_connection_accepted_after_previous_client_drops() {
    // 100 rounds. A semaphore-permit leak that only triggers after N drops
    // is invisible to a single-shot test; cycling well past
    // max_connections proves the permit returns reliably every time.
    const ROUNDS: usize = 100;

    let srv = weir_server!("reconnect").start();
    for round in 0..ROUNDS {
        let mut c = srv.client();
        c.push(
            format!("reconnect-round-{round}").as_bytes(),
            Durability::Buffered,
        )
        .unwrap_or_else(|e| panic!("round {round}: push failed: {e}"));
        // c drops here → connection closed → permit must be returned
        // before the next iteration can acquire one.
    }

    // Final health check confirms the server is still responsive after the
    // permit-acquire/release cycle.
    srv.client()
        .health_check()
        .expect("server unresponsive after reconnect loop");
}

// ── Payload edge cases ────────────────────────────────────────────────────────

#[test]
fn empty_payload_is_accepted() {
    let srv = weir_server!("empty_payload").start();
    let mut client = srv.client();
    client.push(b"", Durability::Sync).unwrap();
}

#[test]
fn arbitrary_binary_payload_accepted() {
    // Renamed from binary_payload_round_trips: there is no Pop API, so no
    // round-trip occurs. The test verifies the server doesn't strip null bytes
    // or high bytes in any text-mode handling.
    let srv = weir_server!("binary_payload").start();
    let mut client = srv.client();
    let payload: Vec<u8> = (0u8..=255).collect();
    client.push(&payload, Durability::Sync).unwrap();
}

#[test]
fn payload_size_boundary_enforced() {
    // Strengthened from `large_payload_accepted`: the original test pushed
    // 1 MiB and called it done. A bug at the 16 MiB boundary would never
    // be exposed by a 1 MiB push. This version tests the actual limit.
    use weir_core::MAX_PAYLOAD_HARD_CAP;

    let srv = weir_server!("payload_boundary").start();

    // Exactly at the cap: must succeed. Uses Batched so we don't wait for
    // a 16 MiB fsync — the property under test is the size acceptance
    // check, not durability.
    {
        let mut client = srv.client();
        let payload = vec![0xAAu8; MAX_PAYLOAD_HARD_CAP];
        client
            .push(&payload, Durability::Batched)
            .expect("MAX_PAYLOAD_HARD_CAP-sized push should be accepted");
    }

    // One byte over the cap: the server must Nack with PayloadTooLarge BEFORE
    // reading the body. WeirClient::push writes the entire frame (header +
    // body) before reading the response; with a 16 MiB + 1 body, the server
    // sends the Nack and closes the socket while the client is mid-write,
    // so the high-level client surfaces BrokenPipe rather than the Nack.
    //
    // To verify the Nack reason directly we use a raw socket: write just the
    // header claiming an over-cap payload (no body bytes follow), then read
    // the server's response.
    {
        use std::{
            io::{Read, Write},
            os::unix::net::UnixStream as RawStream,
        };
        use weir_core::{HEADER_LEN, Header, MessageType, NackReason};

        let mut stream = RawStream::connect(&srv.socket_path).expect("raw connect");
        let header = Header::new(
            MessageType::Push,
            Durability::Batched,
            0,
            (MAX_PAYLOAD_HARD_CAP + 1) as u32,
        );
        stream
            .write_all(&header.encode())
            .expect("write header for over-cap push");

        // Read response header.
        let mut resp_header_buf = [0u8; HEADER_LEN];
        stream
            .read_exact(&mut resp_header_buf)
            .expect("read response header");
        let resp_header = Header::decode(&resp_header_buf).expect("decode response header");
        assert_eq!(
            resp_header.message_type,
            MessageType::Nack,
            "expected Nack response for over-cap header"
        );

        // The wire format puts the NackReason as the first byte of the
        // payload (see `send_nack` in src/socket/connection.rs). Read the
        // payload and verify the reason byte.
        let mut payload = vec![0u8; resp_header.payload_len as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).expect("read nack payload");
        }
        let mut crc = [0u8; 4];
        stream.read_exact(&mut crc).expect("read response crc");

        assert!(
            !payload.is_empty(),
            "nack response had zero-length payload — no room for reason byte"
        );
        let reason = NackReason::try_from(payload[0])
            .unwrap_or_else(|e| panic!("invalid NackReason byte {}: {e}", payload[0]));
        assert_eq!(
            reason,
            NackReason::PayloadTooLarge,
            "expected NackReason::PayloadTooLarge for {}-byte payload, got {reason:?}",
            MAX_PAYLOAD_HARD_CAP + 1
        );
        let _ = (HEADER_LEN, crc); // suppress unused warnings on the wire-format reads
    }

    // Server must still be alive after the rejection.
    srv.client()
        .health_check()
        .expect("server unresponsive after oversize-payload rejection");
}

// ── Stress ────────────────────────────────────────────────────────────────────

#[test]
fn sustained_load_1000_records_single_client() {
    const N: u64 = 1000;

    let srv = weir_server!("sustained").start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("load-{i:06}").as_bytes(), Durability::Buffered)
            .unwrap_or_else(|e| panic!("record {i} failed: {e}"));
    }
    drop(client);

    // Strengthened beyond the original "push().unwrap() in a loop": verify
    // that the accepted and ack counters reflect every record. A silent-drop
    // regression (worker pool drops on contention, batch buffer overruns,
    // etc.) would leave the counters under N while every push().unwrap()
    // happily passes.
    let body = srv.scrape_metrics();
    let accepted = parse_metric(&body, "weir_records_accepted_total{tier=\"buffered\"}");
    let acked = parse_metric(&body, "weir_records_ack_total{tier=\"buffered\"}");
    assert_eq!(
        accepted, N,
        "expected accepted == {N}, got {accepted} — records dropped before reaching the worker pool"
    );
    assert_eq!(
        acked, N,
        "expected acked == {N}, got {acked} — records dropped between accept and ack"
    );
}

#[test]
fn mixed_durability_under_concurrent_load() {
    const THREADS: usize = 6;
    const RECORDS: usize = 50;

    let srv = weir_server!("mixed_load").start();
    let socket_path = srv.socket_path.clone();

    let tiers = [Durability::Sync, Durability::Batched, Durability::Buffered];
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            let tier = tiers[t % tiers.len()];
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect: {e}"));
                for i in 0..RECORDS {
                    client
                        .push(format!("t{t}-r{i}").as_bytes(), tier)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    // Strengthened: assert each tier's ack counter matches the records
    // pushed for that tier. With THREADS=6 and tiers cycling 3-wide,
    // threads 0,3 push Sync (2 × RECORDS), threads 1,4 push Batched
    // (2 × RECORDS), threads 2,5 push Buffered (2 × RECORDS).
    //
    // The thing this catches that the original test couldn't: a buggy
    // tier-dispatch table that silently re-routes (e.g. all Buffered
    // counted as Sync) or drops one tier under contention. The original
    // panic-on-Err pattern would still pass under such a bug because
    // the client only sees Ack/Nack, not which tier the server thinks
    // it was.
    let per_tier_expected = (THREADS / tiers.len() * RECORDS) as u64;
    let body = srv.scrape_metrics();
    for (label, _tier) in [
        ("sync", Durability::Sync),
        ("batched", Durability::Batched),
        ("buffered", Durability::Buffered),
    ] {
        let acked = parse_metric(
            &body,
            &format!("weir_records_ack_total{{tier=\"{label}\"}}"),
        );
        assert_eq!(
            acked, per_tier_expected,
            "expected {per_tier_expected} acks for tier {label}, got {acked} — \
             tier dispatch is dropping or mis-routing records"
        );
    }
}

// ── Crash recovery ────────────────────────────────────────────────────────────

#[test]
fn server_restarts_after_sigkill() {
    // 10 s is generous: cold start on a fresh daemon takes well under 1 s.
    // The bound catches a regression where startup blocks on something
    // (recovery, bind_cleanup, segment scan) for an unreasonable time.
    const RESTART_BUDGET: Duration = Duration::from_secs(10);

    let mut srv = weir_server!("crash_restart").start();
    srv.client()
        .push(b"before-crash", Durability::Sync)
        .unwrap();

    srv.kill_ungracefully();

    // Socket file persists on disk (SIGKILL left it behind).
    assert!(
        srv.socket_path.exists(),
        "socket should remain after SIGKILL"
    );

    // bind_cleanup removes the stale socket; server starts clean.
    let restart_start = Instant::now();
    srv.restart_in_place();
    let restart_elapsed = restart_start.elapsed();
    assert!(
        restart_elapsed < RESTART_BUDGET,
        "restart took {restart_elapsed:?} — exceeded {RESTART_BUDGET:?} budget"
    );

    srv.client()
        .push(b"after-restart", Durability::Sync)
        .unwrap();
}

#[test]
fn wab_data_preserved_across_crash_restart() {
    const N: u32 = 20;

    // Track records that actually acked. The post-restart replay counter
    // must be ≥ this value; "preserved" cannot mean "bytes on disk" alone,
    // because a recovery pass that quarantines every segment would leave
    // the bytes intact while losing every record.
    let mut srv = weir_server!("wab_crash").start();
    let mut client = srv.client();
    let mut acked: u32 = 0;
    for i in 0..N {
        if client
            .push(format!("crash-rec-{i}").as_bytes(), Durability::Sync)
            .is_ok()
        {
            acked += 1;
        }
    }
    drop(client);
    thread::sleep(Duration::from_millis(150));

    let bytes_before = wab_dir_bytes(&srv.wab_dir);
    assert!(bytes_before > 0, "WAB should have data before crash");

    srv.kill_ungracefully();

    let bytes_after_kill = wab_dir_bytes(&srv.wab_dir);
    assert_eq!(
        bytes_before, bytes_after_kill,
        "WAB data must not change during a crash"
    );

    srv.restart_in_place();

    let bytes_after_restart = wab_dir_bytes(&srv.wab_dir);
    assert!(
        bytes_after_restart > 0,
        "WAB data must persist across crash + restart"
    );

    // Give the drain a moment to replay before scraping.
    thread::sleep(Duration::from_millis(200));

    // The strengthened assertion: recovery must replay every acked record.
    // Without this check, a recovery pass that quarantined every segment
    // (no replay) would pass this test — bytes would still be on disk
    // (in the quarantine dir), but the records would be lost.
    let body = srv.scrape_metrics();
    let replayed = parse_metric(&body, "weir_recovery_records_replayed_total");
    assert!(
        replayed >= u64::from(acked),
        "expected weir_recovery_records_replayed_total >= {acked} (acked pre-crash), \
         got {replayed} — recovery did not replay the preserved bytes"
    );
}

// ── Fault injection ───────────────────────────────────────────────────────────

#[test]
fn readonly_wab_dir_prevents_startup() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;

    let _lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let tmp_dir = std::env::temp_dir().join(format!("weir_fault_ro_{}", std::process::id()));
    let wab_dir = tmp_dir.join("wab");
    let socket_dir = tmp_dir.join("run");
    let socket_path = socket_dir.join("weir.sock");
    let config_path = tmp_dir.join("weir.toml");
    let metrics_port = free_port();

    fs::create_dir_all(&wab_dir).unwrap();
    fs::create_dir_all(&socket_dir).unwrap();

    // Remove all permissions so the server cannot create shard subdirs.
    fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o000)).unwrap();

    // When the test harness runs as root, chmod 0o000 doesn't prevent access —
    // root bypasses DAC. Drop privileges in the child to uid `nobody` (65534)
    // so the permission bit actually bites. socket_dir is widened so the
    // dropped child can bind the socket; the test target is wab_dir access,
    // not socket creation.
    let drop_to_nobody = unsafe { libc::geteuid() } == 0;
    if drop_to_nobody {
        fs::set_permissions(&socket_dir, fs::Permissions::from_mode(0o777)).unwrap();
        fs::set_permissions(&tmp_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let config = format!(
        "[server]\n\
         socket_path  = \"{}\"\n\
         wab_dir      = \"{}\"\n\
         metrics_port = {}\n\
         shard_count  = 1\n\
         worker_count = 2\n\
         batch_size   = 100\n\
         batch_deadline_ms = 20\n\
         log_level    = \"error\"\n",
        socket_path.display(),
        wab_dir.display(),
        metrics_port,
    );
    fs::write(&config_path, config).unwrap();

    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut cmd = Command::new(binary);
    cmd.args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    if drop_to_nobody {
        unsafe {
            cmd.pre_exec(|| {
                // 65534 is the conventional `nobody` uid in Linux containers
                // (busybox, Debian, Ubuntu, Alpine all default to this). If the
                // setuid call fails (uid doesn't exist on this system), let
                // the child exec proceed — the test will then fall back to
                // the original behavior, which is the only thing we can do
                // without a guaranteed-present unprivileged uid.
                let _ = libc::setgid(65534);
                let _ = libc::setuid(65534);
                Ok(())
            });
        }
    }

    let mut child = cmd.spawn().expect("failed to spawn weir-server");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_status = Some(status);
                break;
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(_) => break,
        }
    }
    if exit_status.is_none() {
        let _ = child.kill();
    }
    // Idempotent if try_wait already reaped; this satisfies clippy's
    // zombie_processes lint by ensuring wait() runs on every code path.
    let _ = child.wait();

    // Restore permissions so cleanup can remove the directory.
    fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).ok();
    fs::remove_dir_all(&tmp_dir).ok();

    let failed = exit_status.map(|s| !s.success()).unwrap_or(false);
    assert!(
        failed,
        "weir-server should fail to start when wab_dir is unreadable/unwritable \
         (running {}as root)",
        if drop_to_nobody {
            "originally "
        } else {
            "not "
        }
    );
}

// ── Multi-shard correctness ───────────────────────────────────────────────────

#[test]
fn shard_directories_created_on_disk() {
    let srv = weir_server!("shard_dirs").shard_count(3).start();
    let mut client = srv.client();
    client
        .push(b"trigger-shard-creation", Durability::Sync)
        .unwrap();
    thread::sleep(Duration::from_millis(100));

    // WAB creates shard_00, shard_01, shard_02 directories.
    let mut found = 0usize;
    for entry in fs::read_dir(&srv.wab_dir).unwrap().flatten() {
        if entry.file_type().unwrap().is_dir() {
            let name = entry.file_name();
            if name
                .to_str()
                .map(|n| n.starts_with("shard_"))
                .unwrap_or(false)
            {
                found += 1;
            }
        }
    }
    assert!(
        found >= 3,
        "expected at least 3 shard dirs in {}, found {found}",
        srv.wab_dir.display()
    );
}

#[test]
fn concurrent_producers_all_acked_with_multiple_shards() {
    const THREADS: usize = 4;
    const RECORDS_PER_THREAD: usize = 50;
    const SHARD_COUNT: usize = 4;

    let srv = weir_server!("multi_shard_conc")
        .shard_count(SHARD_COUNT)
        .start();
    let socket_path = srv.socket_path.clone();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect: {e}"));
                for i in 0..RECORDS_PER_THREAD {
                    client
                        .push(format!("t{t}-r{i}").as_bytes(), Durability::Sync)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    // Without the assertion below the test would be indistinguishable from
    // `concurrent_producers_all_acked` — a buggy router that always picked
    // shard 0 would still pass. Walk the per-shard directories and confirm
    // work actually fanned out. With round-robin assignment by connection
    // counter and 4 concurrent connections, all 4 shards should see data.
    let mut shards_with_data = 0usize;
    for shard_id in 0..SHARD_COUNT {
        let shard_dir = srv.wab_dir.join(format!("shard_{shard_id:02}"));
        if wab_dir_bytes(&shard_dir) > 0 {
            shards_with_data += 1;
        }
    }
    assert!(
        shards_with_data >= 2,
        "expected records to land in ≥2 shard directories, only {shards_with_data} of \
         {SHARD_COUNT} have data — connection-to-shard routing is collapsed"
    );
}

// ── Graceful shutdown under load ──────────────────────────────────────────────

/// Verifies that SIGTERM under concurrent Sync load produces no silent drops.
///
/// Every push that returned `Ok` must be on disk (Sync durability guarantee).
/// Every push that did not complete must surface as `ClientError::Io` so the
/// producer knows it needs to retry — not a silent half-write or a panic.
#[test]
fn graceful_shutdown_under_load() {
    const THREADS: usize = 8;
    const PUSH_BEFORE_SIGTERM: Duration = Duration::from_secs(2);
    // shutdown_timeout_secs=3 in config + buffer for process exit overhead.
    const MAX_SHUTDOWN_SECS: u64 = 8;

    let mut srv = weir_server!("shutdown_load").start();

    let ok_count = Arc::new(AtomicU64::new(0));
    let io_err_count = Arc::new(AtomicU64::new(0));
    let unexpected_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let ok = Arc::clone(&ok_count);
            let io_err = Arc::clone(&io_err_count);
            let unexpected = Arc::clone(&unexpected_count);
            let path = srv.socket_path.clone();
            thread::spawn(move || {
                let Ok(mut client) = WeirClient::connect(&path) else {
                    return; // server already gone
                };
                loop {
                    match client.push(b"shutdown-load", Durability::Sync) {
                        Ok(()) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ClientError::Io(_)) => {
                            // Connection closed — expected during shutdown.
                            io_err.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => {
                            // Nack or protocol error — not expected.
                            unexpected.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    // Let the threads push for a bit, then signal shutdown.
    thread::sleep(PUSH_BEFORE_SIGTERM);
    let shutdown_elapsed = srv.sigterm();

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    let oks = ok_count.load(Ordering::Relaxed);
    let io_errs = io_err_count.load(Ordering::Relaxed);
    let unexpected = unexpected_count.load(Ordering::Relaxed);
    let wab_bytes = wab_dir_bytes(&srv.wab_dir);

    // Server must exit within a reasonable bound after SIGTERM.
    assert!(
        shutdown_elapsed < Duration::from_secs(MAX_SHUTDOWN_SECS),
        "server took {shutdown_elapsed:?} to shut down — expected < {MAX_SHUTDOWN_SECS}s"
    );

    // No unexpected errors: threads may see Ok or Io(EOF), never Nack/Protocol.
    assert_eq!(
        unexpected, 0,
        "{unexpected} unexpected errors (Nack or Protocol) during shutdown — \
         producers should only see Ok or Io"
    );

    // Every Ok means a Sync-flushed record. The WAB must have bytes.
    assert!(
        oks > 0,
        "expected successful pushes before SIGTERM; got 0 — \
         either the server started too slowly or {PUSH_BEFORE_SIGTERM:?} was too short"
    );
    assert!(
        wab_bytes > 0,
        "WAB has 0 bytes on disk after {oks} successful Sync pushes — \
         possible silent data loss"
    );

    println!(
        "graceful_shutdown_under_load: {oks} ok, {io_errs} io_err, \
         {unexpected} unexpected | wab={wab_bytes}B | shutdown={shutdown_elapsed:?}"
    );
}

// ── Stalled client isolation ──────────────────────────────────────────────────

/// A client that connects, sends one Push frame, and then never reads the Ack
/// must not block or slow down other connections.
///
/// The stalled connection holds a permit in the connection semaphore and keeps
/// the server's per-connection async task suspended (waiting to read the next
/// frame). Other connections must get their own permits and proceed normally.
#[test]
fn stalled_client_does_not_block_other_connections() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Envelope, Header, MessageType};

    const CONCURRENT_RECORDS: usize = 50;
    const CONCURRENT_DEADLINE: Duration = Duration::from_secs(5);

    let srv = weir_server!("stall_isolation").start();

    // Pre-encode one Push frame for the stalled client to send.
    let payload = b"stall";
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let frame = Envelope::new(header, payload.to_vec()).encode();

    let stop_stall = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Stalled client: connects, sends one frame, holds the connection open
    // without ever reading the Ack. The server task for this connection is
    // suspended waiting for the next frame — which never comes.
    let stall_handle = {
        let path = srv.socket_path.clone();
        let stop = Arc::clone(&stop_stall);
        thread::spawn(move || {
            let mut stream = RawStream::connect(&path).expect("stall: connect");
            stream.write_all(&frame).expect("stall: write frame");
            // Deliberately do not read the Ack. Hold the connection open.
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(50));
            }
        })
    };

    // Let the stall thread connect and send its frame before we proceed.
    thread::sleep(Duration::from_millis(100));

    // Concurrent client: 50 Sync pushes while the stalled connection is held.
    let mut client = srv.client();
    let t0 = Instant::now();
    for i in 0..CONCURRENT_RECORDS {
        client
            .push(format!("concurrent-{i}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("concurrent push {i} failed: {e}"));
    }
    let elapsed = t0.elapsed();

    stop_stall.store(true, Ordering::Relaxed);
    stall_handle.join().expect("stall thread panicked");

    assert!(
        elapsed < CONCURRENT_DEADLINE,
        "{CONCURRENT_RECORDS} pushes took {elapsed:?} with stalled connection held — \
         expected < {CONCURRENT_DEADLINE:?} (stalled client may be blocking the worker)"
    );

    srv.client()
        .health_check()
        .expect("server unresponsive after stalled client test");
}

// ── Partial frame injection ───────────────────────────────────────────────────

/// Sending a valid header then only half the declared payload bytes before
/// closing the connection must not corrupt the server's per-connection state
/// machine. The next fresh connection must work normally.
#[test]
fn partial_frame_does_not_corrupt_next_connection() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Envelope, HEADER_LEN, Header, MessageType};

    let srv = weir_server!("partial_frame").start();

    // Build a valid Push frame with a 64-byte payload.
    let payload = vec![0xabu8; 64];
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let frame = Envelope::new(header, payload).encode();

    {
        let mut stream = RawStream::connect(&srv.socket_path).expect("connect for partial frame");
        // Write only header + first 16 bytes of the 64-byte payload.
        stream
            .write_all(&frame[..HEADER_LEN + 16])
            .expect("write partial frame");
        // Drop the stream — connection dies mid-frame.
    }

    // Give the server time to observe the EOF and clean up the connection.
    thread::sleep(Duration::from_millis(50));

    // A fresh connection must work normally — the partial frame must not have
    // left the server's read state in a corrupt position.
    srv.client()
        .push(b"after-partial-frame", Durability::Sync)
        .expect("push failed after partial frame injection");

    srv.client()
        .health_check()
        .expect("server unresponsive after partial frame test");
}

// ── Write-error handling (EFBIG / ENOSPC) ─────────────────────────────────────

/// A WAB write failure caused by `RLIMIT_FSIZE = 0` (kernel returns `EFBIG`)
/// must produce `Nack(InternalError)` on the client, not a server crash or a
/// silent data drop. EFBIG is the cheapest write-failure mode to simulate
/// without root: see `enospc_returns_nack_not_crash` for the
/// production-shaped ENOSPC variant.
#[test]
fn efbig_returns_nack_not_crash() {
    use weir_core::NackReason;

    // RLIMIT_FSIZE = 0 makes every WAB write fail with EFBIG. SIGXFSZ is
    // ignored so the signal doesn't kill the process; writes return an
    // error that the server surfaces as Nack(InternalError). stdout/stderr
    // are silenced because the log file itself would also fail to write.
    let srv = unsafe {
        weir_server!("efbig").silence_logs().pre_exec(|| {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            let rl = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            libc::setrlimit(libc::RLIMIT_FSIZE, &rl);
            Ok(())
        })
    }
    .start();
    let mut client = srv.client();

    // With RLIMIT_FSIZE=0 the first WAB segment header write fails immediately.
    let result = client.push(b"should-nack", Durability::Sync);
    assert!(
        matches!(result, Err(ClientError::Nack(NackReason::InternalError))),
        "expected Nack(InternalError) from EFBIG-throttled server, got {result:?}"
    );

    // Server must still be alive and accepting connections after the nack.
    srv.client()
        .health_check()
        .expect("server unresponsive after EFBIG nack");
}

/// ENOSPC variant — production-shaped write failure (filesystem out of space)
/// rather than EFBIG (file-size rlimit hit). Requires a small pre-mounted
/// filesystem at the path in `WEIR_TEST_ENOSPC_DIR`; ignored by default
/// because creating one needs root.
///
/// Setup (run once, as root, before invoking this test):
///
/// ```sh
/// sudo mkdir -p /mnt/weir-enospc
/// sudo mount -t tmpfs -o size=64K tmpfs /mnt/weir-enospc
/// sudo chmod 0700 /mnt/weir-enospc
/// sudo chown $USER /mnt/weir-enospc
/// WEIR_TEST_ENOSPC_DIR=/mnt/weir-enospc \
///   cargo test -p weir-server --test system -- --ignored enospc_returns_nack_not_crash
/// sudo umount /mnt/weir-enospc && sudo rmdir /mnt/weir-enospc
/// ```
///
/// The 64 KiB tmpfs is small enough that the first WAB segment header
/// (16 KiB pre-allocated) plus a single Sync record fills it; subsequent
/// pushes must Nack rather than panic.
#[test]
#[ignore = "requires WEIR_TEST_ENOSPC_DIR pointing at a small pre-mounted tmpfs (see test docstring)"]
fn enospc_returns_nack_not_crash() {
    use weir_core::NackReason;

    let enospc_dir = std::env::var("WEIR_TEST_ENOSPC_DIR")
        .expect("WEIR_TEST_ENOSPC_DIR not set — see test docstring for the tmpfs setup procedure");
    let enospc_dir = PathBuf::from(enospc_dir);
    let wab_dir = enospc_dir.join("wab");

    // WAB dir on the small filesystem; tolerate "already exists" from a prior run.
    if let Err(e) = fs::create_dir(&wab_dir)
        && e.kind() != std::io::ErrorKind::AlreadyExists
    {
        panic!("create wab_dir on {}: {e}", enospc_dir.display());
    }

    // Spawn with the WAB on the tmpfs, but everything else (socket, log,
    // config) on the regular tmp dir. SIGXFSZ ignored defensively in the
    // pre_exec hook — not strictly needed for ENOSPC but harmless.
    let handle = unsafe {
        weir_server!("enospc").wab_dir(&wab_dir).pre_exec(|| {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            Ok(())
        })
    }
    .start();

    // Push records until one fails with Nack(InternalError). The 64 KiB tmpfs
    // should fill within a small handful of records.
    let mut client = handle.client();
    let mut saw_nack = false;
    for i in 0..200u32 {
        let payload = vec![0xAAu8; 1024]; // 1 KiB; tmpfs holds ~64.
        match client.push(&payload, Durability::Sync) {
            Ok(()) => continue,
            Err(ClientError::Nack(NackReason::InternalError)) => {
                saw_nack = true;
                break;
            }
            Err(other) => panic!("unexpected error on record {i}: {other:?}"),
        }
    }
    assert!(
        saw_nack,
        "filesystem at {} did not return ENOSPC within 200 × 1 KiB records — \
         is it larger than 64 KiB? Check tmpfs size.",
        enospc_dir.display()
    );

    // Server must still be alive after the nack.
    handle
        .client()
        .health_check()
        .expect("server unresponsive after ENOSPC nack");
}

// ── WAB data integrity after crash ────────────────────────────────────────────

/// Every Sync push that returned `Ok` must be present on disk byte-for-byte
/// after the server is killed with SIGKILL.
///
/// This tests the "Sync durability" contract at the byte level: if the client
/// got an `Ok`, the payload must be in the WAB file (the fsync happened before
/// the ack was sent).
#[test]
fn wab_data_integrity_after_crash() {
    const N: usize = 50;

    let mut srv = weir_server!("wab_integrity").shard_count(1).start();
    let mut client = srv.client();

    let mut acked: Vec<Vec<u8>> = Vec::new();
    for i in 0..N {
        let payload = format!("integrity-{i:05}").into_bytes();
        match client.push(&payload, Durability::Sync) {
            Ok(()) => acked.push(payload),
            Err(_) => break, // server died mid-push
        }
    }

    assert!(!acked.is_empty(), "no pushes acked before crash");

    // Crash without cleanup — WAB files stay on disk exactly as they were.
    srv.kill_ungracefully();

    let wab_bytes = read_wab_bytes(&srv.wab_dir);
    assert!(
        !wab_bytes.is_empty(),
        "WAB directory empty after {} acked Sync pushes",
        acked.len()
    );

    // Every acked payload must appear verbatim in the WAB bytes.
    for payload in &acked {
        let found = wab_bytes
            .windows(payload.len())
            .any(|w| w == payload.as_slice());
        assert!(
            found,
            "acked payload {:?} not found in WAB bytes — possible data loss",
            String::from_utf8_lossy(payload)
        );
    }
}

// ── Socket takeover data safety ───────────────────────────────────────────────

/// `bind_cleanup` removes the socket file (even if another process is
/// listening) so that crash-recovery always succeeds. When a second server
/// takes the socket path the first server's WAB files must be left entirely
/// untouched — the socket file and the WAB are independent resources.
#[test]
fn socket_takeover_does_not_corrupt_wab_data() {
    const N: usize = 20;

    let srv_a = weir_server!("socket_takeover").start();
    let mut client = srv_a.client();

    for i in 0..N {
        client
            .push(format!("srv-a-{i}").as_bytes(), Durability::Sync)
            .expect("push to server A failed");
    }

    let wab_before = wab_dir_bytes(&srv_a.wab_dir);
    assert!(wab_before > 0, "server A must have written WAB bytes");

    // Spawn server B at the same socket path (it will call bind_cleanup and
    // take over the socket). Use Command directly — we hold the process lock
    // via srv_a and need two processes alive at once intentionally.
    let second_tmp =
        std::env::temp_dir().join(format!("weir_sys_takeover_b_{}", std::process::id()));
    let second_wab = second_tmp.join("wab");
    let second_config = second_tmp.join("weir.toml");

    fs::create_dir_all(&second_wab).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&second_wab, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(
        &second_config,
        format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = 1\n\
             worker_count          = 2\n\
             batch_size            = 100\n\
             batch_deadline_ms     = 20\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"warn\"\n",
            srv_a.socket_path.display(),
            second_wab.display(),
            free_port(),
        ),
    )
    .unwrap();

    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut child_b = Command::new(binary)
        .args(["--config", second_config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server B");

    // Give server B time to start and take the socket.
    thread::sleep(Duration::from_millis(500));

    // Server A's WAB files must be completely untouched.
    let wab_after = wab_dir_bytes(&srv_a.wab_dir);
    assert_eq!(
        wab_before, wab_after,
        "server B startup modified server A's WAB bytes ({wab_before} → {wab_after})"
    );

    // Verify payload bytes are still present.
    let wab_bytes = read_wab_bytes(&srv_a.wab_dir);
    for i in 0..N {
        let payload = format!("srv-a-{i}").into_bytes();
        assert!(
            wab_bytes
                .windows(payload.len())
                .any(|w| w == payload.as_slice()),
            "payload srv-a-{i} missing from server A's WAB after socket takeover"
        );
    }

    let _ = child_b.kill();
    let _ = child_b.wait();
    let _ = fs::remove_dir_all(&second_tmp);
}

// ── File descriptor limit exhaustion ─────────────────────────────────────────

/// When the server hits its `RLIMIT_NOFILE` ceiling it must not crash —
/// new connections are refused or queued by the kernel, and connections
/// within the fd budget continue to work normally.
#[test]
fn fd_limit_exhaustion_does_not_crash_server() {
    use std::os::unix::net::UnixStream as RawStream;

    // 128 fds: comfortably above server startup overhead (~20 fds) but low
    // enough that opening 200 connections exhausts the limit.
    const NOFILE_LIMIT: u64 = 128;
    const FLOOD_CONNS: usize = 200;

    // RLIMIT_NOFILE caps the daemon's fd budget. The pre_exec hook installs
    // it just before exec so it survives into the daemon process.
    let srv = unsafe {
        weir_server!("fd_limit").pre_exec(move || {
            let rl = libc::rlimit {
                rlim_cur: NOFILE_LIMIT,
                rlim_max: NOFILE_LIMIT,
            };
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
            Ok(())
        })
    }
    .start();

    // Flood the server with raw connections and keep them open. The audit
    // recommended asserting ≥1 connect refused as proof the fd limit was
    // really biting, but on Linux that's not environment-portable: with
    // net.core.somaxconn ≥ FLOOD_CONNS (default 4096 on most distros and
    // containers) the kernel absorbs every connect into the listen backlog
    // and the client sees no failures even when the server's accept() is
    // returning EMFILE. We track the count as diagnostic info but the
    // property under test — "server doesn't crash or hang" — is verified
    // by the post-flood health_check and push.
    let mut open: Vec<RawStream> = Vec::new();
    let mut connect_errors = 0usize;
    for _ in 0..FLOOD_CONNS {
        match RawStream::connect(&srv.socket_path) {
            Ok(s) => open.push(s),
            Err(_) => connect_errors += 1,
        }
    }
    eprintln!(
        "fd_limit_exhaustion: opened {} sockets, {connect_errors} connect()s refused \
         (zero refusals is fine — the kernel backlog absorbs them when \
         somaxconn ≥ FLOOD_CONNS)",
        open.len()
    );

    // Hold connections briefly to let the server try (and fail) to accept them.
    thread::sleep(Duration::from_millis(200));

    drop(open);

    // After releasing the flood, the server must still be alive.
    thread::sleep(Duration::from_millis(200));
    srv.client()
        .health_check()
        .expect("server crashed or hung under fd pressure");

    // A normal Sync push must succeed after the fd pressure is relieved.
    // This is the strongest portable verification that the daemon survives
    // fd-budget exhaustion: end-to-end producer → ack works again.
    srv.client()
        .push(b"after-fd-flood", Durability::Sync)
        .expect("push failed after fd-limit flood");
}

// ── Metrics accuracy ──────────────────────────────────────────────────────────

#[test]
fn records_accepted_counter_increments_after_sync_pushes() {
    const N: u32 = 10;

    let srv = weir_server!("metrics_accepted").start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("acc-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_accepted_total{{tier=\"sync\"}} {N}");
    assert!(
        body.contains(&expected),
        "expected '{expected}' in metrics; body:\n{body:.800}"
    );
}

#[test]
fn records_ack_counter_increments_after_sync_pushes() {
    const N: u32 = 7;

    let srv = weir_server!("metrics_ack").start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("ack-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_ack_total{{tier=\"sync\"}} {N}");
    assert!(
        body.contains(&expected),
        "expected '{expected}' in metrics; body:\n{body:.800}"
    );
}

// ── Per-shard record ordering ─────────────────────────────────────────────────

/// With a single-shard server, records submitted sequentially from a single
/// producer must appear in submission order in the raw WAB bytes.
///
/// This is the fundamental append-log ordering contract. Any change to the
/// batching or queue path that accidentally reorders records will be caught
/// here.
#[test]
fn per_shard_records_appear_in_submission_order() {
    const N: usize = 30;

    // Single shard: all records go to the same WAB file, so order is preserved.
    let srv = weir_server!("ordering").shard_count(1).start();
    let mut client = srv.client();

    for i in 0..N {
        client
            .push(format!("order-{i:05}").as_bytes(), Durability::Sync)
            .expect("push failed");
    }

    // Flush any remaining data and give the server time to seal.
    srv.client()
        .health_check()
        .expect("server unresponsive after pushes");

    let wab_bytes = read_wab_bytes(&srv.wab_dir);
    assert!(!wab_bytes.is_empty(), "WAB must have data after pushes");

    // Find the byte offset of each payload in the WAB.
    let mut prev_offset: Option<usize> = None;
    for i in 0..N {
        let payload = format!("order-{i:05}").into_bytes();
        let offset = wab_bytes
            .windows(payload.len())
            .position(|w| w == payload.as_slice())
            .unwrap_or_else(|| panic!("payload order-{i:05} not found in WAB bytes"));

        if let Some(prev) = prev_offset {
            assert!(
                offset > prev,
                "record order-{i:05} at offset {offset} appears before the previous record \
                 at offset {prev} — submission order not preserved"
            );
        }
        prev_offset = Some(offset);
    }
}

/// Concurrent producers writing to the same shard must each see their own
/// records appear in submission order in the WAB, even with worker_count > 1.
///
/// Single-shard, worker_count = 2 (the default). N concurrent producers each
/// push M records identifying themselves by `(producer_id, sequence)`. After
/// all producers finish, every producer's records must appear in ascending
/// sequence order in the WAB bytes.
///
/// Pre-F3 (single MPMC queue, multiple workers racing): per-producer order is
/// still preserved by the request/response protocol (a producer can't push the
/// next record until the previous is acked, and the ack post-dates the WAB
/// write). This test pins that behaviour and ensures the partition queue
/// (which now routes shard_id → worker so each shard is owned by exactly one
/// worker) preserves it too — the partition channel is FIFO, the worker's
/// intra-shard buffer is FIFO, the shard's flusher channel is FIFO.
#[test]
fn concurrent_producers_to_same_shard_preserve_per_producer_order() {
    const N_PRODUCERS: usize = 4;
    const N_RECORDS: usize = 50;

    let srv = weir_server!("concurrent_ordering").shard_count(1).start();
    let socket_path = srv.socket_path.clone();

    let handles: Vec<_> = (0..N_PRODUCERS)
        .map(|producer_id| {
            let sock = socket_path.clone();
            std::thread::spawn(move || {
                let mut client = weir_client::WeirClient::connect(&sock).expect("client connect");
                for seq in 0..N_RECORDS {
                    let payload = format!("p{producer_id:02}-s{seq:05}").into_bytes();
                    client
                        .push(&payload, Durability::Sync)
                        .unwrap_or_else(|e| panic!("push p{producer_id} s{seq}: {e:?}"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    // Flush + give the server a beat to seal whatever's still active.
    srv.client()
        .health_check()
        .expect("server unresponsive after concurrent pushes");

    let wab_bytes = read_wab_bytes(&srv.wab_dir);
    assert!(!wab_bytes.is_empty(), "WAB must have data after pushes");

    for producer_id in 0..N_PRODUCERS {
        let mut prev_offset: Option<usize> = None;
        for seq in 0..N_RECORDS {
            let payload = format!("p{producer_id:02}-s{seq:05}").into_bytes();
            let offset = wab_bytes
                .windows(payload.len())
                .position(|w| w == payload.as_slice())
                .unwrap_or_else(|| {
                    panic!("payload p{producer_id:02}-s{seq:05} not found in WAB bytes")
                });
            if let Some(prev) = prev_offset {
                assert!(
                    offset > prev,
                    "producer {producer_id} record {seq} at offset {offset} appears \
                     before its own predecessor at offset {prev} — per-producer \
                     submission order not preserved"
                );
            }
            prev_offset = Some(offset);
        }
    }
}

// ── Batch deadline timer accuracy ─────────────────────────────────────────────

/// With `batch_deadline_ms = 20`, each individual Sync push must complete
/// within a generous multiple of the deadline. A push that exceeds 5×deadline
/// indicates the batch timer is being starved (e.g. the accept loop is
/// spinning) and latency would be non-deterministic in production.
#[test]
fn batch_deadline_timer_keeps_latency_bounded() {
    // 100 samples so the median and p99 are statistically meaningful.
    // The original 20 was too few to estimate p99 — labelling the max of
    // 20 samples "p99" was sloppy.
    const SAMPLES: usize = 100;
    const DEADLINE_MS: u64 = 20; // matches start_impl config
    // Median should be very close to the deadline on an idle daemon — a 2×
    // ceiling here catches starvation regressions cleanly.
    const MEDIAN_CEILING: Duration = Duration::from_millis(DEADLINE_MS * 2); // 40 ms
    // Tail bound is loose so noisy CI runners (scheduling jitter, GC-equivalent
    // pauses) don't flake the test. The starvation regressions this catches
    // are order-of-magnitude, not 10% drift.
    const TAIL_CEILING: Duration = Duration::from_millis(DEADLINE_MS * 5); // 100 ms

    let srv = weir_server!("deadline_accuracy").start();
    let mut client = srv.client();
    let mut latencies: Vec<Duration> = Vec::with_capacity(SAMPLES);

    for i in 0..SAMPLES {
        let t0 = Instant::now();
        client
            .push(format!("timer-{i}").as_bytes(), Durability::Sync)
            .expect("push failed");
        latencies.push(t0.elapsed());
    }

    // Every sample must finish within 5 × deadline (the tail ceiling).
    for (i, &lat) in latencies.iter().enumerate() {
        assert!(
            lat <= TAIL_CEILING,
            "sample {i} took {lat:?} — exceeded 5 × batch_deadline_ms ({TAIL_CEILING:?})"
        );
    }

    // The middle of the distribution has to be tight — that's where the
    // batch timer is doing its job. A starvation regression that only
    // bites the tail (e.g. a rare lock contention) would slip past a
    // tail-only assertion; a starvation regression that biases the
    // middle is a real production problem and we want to flag it.
    let mut sorted = latencies.clone();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    assert!(
        median <= MEDIAN_CEILING,
        "median latency {median:?} exceeded 2 × batch_deadline_ms ({MEDIAN_CEILING:?}) — \
         the batch timer is being starved on the common path"
    );

    // p99 (sample 99 of 100, i.e. the 99th-percentile point) gets the
    // looser tail bound.
    let p99 = sorted[sorted.len() - sorted.len() / 100 - 1];
    assert!(
        p99 <= TAIL_CEILING,
        "p99 latency {p99:?} exceeded 5 × batch_deadline_ms ({TAIL_CEILING:?})"
    );
}

// ── Metrics across crash-restart ──────────────────────────────────────────────

/// Within a single server process the per-tier counters must be internally
/// consistent: `records_accepted` never exceeds the number of pushes made, and
/// `records_ack` never exceeds `records_accepted`. Three rounds across
/// restarts exercise the assertion on a fresh atomic each time.
///
/// Note: this is a *per-session* invariant. The cross-restart resets and the
/// recovery counter are tested separately
/// (`metrics_reset_to_zero_after_restart`, `recovery_replays_records_after_crash`).
#[test]
fn metrics_internally_consistent_per_session() {
    const PUSHES_PER_ROUND: u32 = 10;
    const ROUNDS: u32 = 3;

    let mut srv = weir_server!("metrics_per_session").start();

    for round in 0..ROUNDS {
        let mut client = srv.client();
        for i in 0..PUSHES_PER_ROUND {
            client
                .push(
                    format!("round-{round}-rec-{i}").as_bytes(),
                    Durability::Sync,
                )
                .unwrap_or_else(|e| panic!("push failed (round {round}, rec {i}): {e}"));
        }

        let body = srv.scrape_metrics();

        let accepted = parse_metric(&body, "weir_records_accepted_total{tier=\"sync\"}");
        let acked = parse_metric(&body, "weir_records_ack_total{tier=\"sync\"}");

        assert!(
            accepted <= u64::from(PUSHES_PER_ROUND),
            "round {round}: records_accepted ({accepted}) exceeds pushes made \
             ({PUSHES_PER_ROUND}) — phantom records in counter"
        );
        assert!(
            acked <= accepted,
            "round {round}: records_ack ({acked}) > records_accepted ({accepted})"
        );

        if round + 1 < ROUNDS {
            srv.restart_in_place();
        }
    }
}

/// `records_accepted_total` and `records_ack_total` are in-process atomics —
/// they must reset to 0 on every restart, even if records were on disk.
/// This documents the deliberate non-persistence: Prometheus counters are
/// cumulative *within a process*, and an external scraper handles restart
/// gaps via the `_created` timestamp.
#[test]
fn metrics_reset_to_zero_after_restart() {
    let mut srv = weir_server!("metrics_reset").start();

    // Drive both counters above zero.
    let mut client = srv.client();
    for i in 0..5u32 {
        client
            .push(format!("pre-restart-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }
    drop(client);

    let before = srv.scrape_metrics();
    let accepted_before = parse_metric(&before, "weir_records_accepted_total{tier=\"sync\"}");
    let acked_before = parse_metric(&before, "weir_records_ack_total{tier=\"sync\"}");
    assert!(
        accepted_before >= 5 && acked_before >= 5,
        "expected counters to be ≥5 before restart (accepted={accepted_before}, acked={acked_before})",
    );

    srv.restart_in_place();

    // Immediately after restart, before any new pushes.
    let after = srv.scrape_metrics();
    let accepted_after = parse_metric(&after, "weir_records_accepted_total{tier=\"sync\"}");
    let acked_after = parse_metric(&after, "weir_records_ack_total{tier=\"sync\"}");
    assert_eq!(
        accepted_after, 0,
        "records_accepted_total should reset to 0 after restart, got {accepted_after}"
    );
    assert_eq!(
        acked_after, 0,
        "records_ack_total should reset to 0 after restart, got {acked_after}"
    );
}

/// Crash recovery must replay the records on disk and increment
/// `weir_recovery_records_replayed_total` accordingly. Closes the gap that
/// the audit flagged: the metric exists but no test asserted it advances.
///
/// Procedure:
/// 1. Push N Sync records — guaranteed durable in the active WAB segment.
/// 2. SIGKILL the server (active segment left as `.wab`, no footer).
/// 3. Restart — recovery should seal the active segment and replay it.
/// 4. Scrape metrics; assert `weir_recovery_records_replayed_total >= N`.
#[test]
fn recovery_replays_records_after_crash() {
    const N: u32 = 25;

    let mut srv = weir_server!("recovery_replay").shard_count(1).start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("recover-{i:05}").as_bytes(), Durability::Sync)
            .unwrap();
    }
    drop(client);

    srv.kill_ungracefully();
    srv.restart_in_place();

    // Give the drain a moment to process the replayed segment so the counter
    // has actually been incremented before we scrape.
    thread::sleep(Duration::from_millis(200));

    let body = srv.scrape_metrics();
    let replayed = parse_metric(&body, "weir_recovery_records_replayed_total");
    assert!(
        replayed >= u64::from(N),
        "expected weir_recovery_records_replayed_total >= {N}, got {replayed}\n\
         recovery did not replay all crashed records — metric: {replayed}\n\
         metrics body excerpt:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_recovery") || l.starts_with("weir_wab_segments"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ── MySQL sink integration ────────────────────────────────────────────────────

/// End-to-end check that records pushed to a daemon configured with
/// `sink_type = "mysql"` arrive in the configured table — and arrive there
/// as one multi-row INSERT per batch, demonstrating the IOPS-compression
/// story.
///
/// Ignored by default because it requires a running MySQL server reachable
/// at the URL in `WEIR_TEST_MYSQL_URL`. Setup, e.g. with Docker:
///
/// ```sh
/// docker run --rm -d --name weir-test-mysql \
///   -e MYSQL_ROOT_PASSWORD=test \
///   -e MYSQL_DATABASE=weir_test \
///   -p 3306:3306 mysql:8.0
/// # Wait ~10s for mysqld to come up.
/// docker exec weir-test-mysql mysql -ptest weir_test -e "
///   CREATE TABLE weir_records (
///     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
///     payload VARBINARY(4096) NOT NULL,
///     UNIQUE KEY(payload(255))
///   );"
/// WEIR_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:3306/weir_test \
///   cargo test -p weir-server --test system -- --ignored mysql_sink_end_to_end
/// ```
#[test]
#[ignore = "requires WEIR_TEST_MYSQL_URL pointing at a running MySQL with a prepared schema (see docstring)"]
fn mysql_sink_end_to_end() {
    const N: u32 = 100;

    let mysql_url = std::env::var("WEIR_TEST_MYSQL_URL").expect(
        "WEIR_TEST_MYSQL_URL not set — see the test docstring for the docker-compose recipe",
    );

    // sink_type = mysql; URL passed via env so credentials never touch the
    // config file (production-shaped — see the operations docs). Other
    // mysql-specific knobs ride along via extra_config.
    let handle = weir_server!("mysql")
        .batch_size(200)
        .batch_deadline_ms(5)
        .shutdown_timeout_secs(5)
        .env("WEIR_SINK_URL", &mysql_url)
        .extra_config("sink_type             = \"mysql\"")
        .extra_config("sink_max_batch_size   = 1000")
        .extra_config("sink_mysql_table      = \"weir_records\"")
        .extra_config("sink_mysql_column     = \"payload\"")
        .extra_config("sink_mysql_insert_mode = \"ignore\"")
        .start();

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("mysql-rec-{i:05}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);

    // Give the drain a moment to drain the sealed segment into MySQL.
    thread::sleep(Duration::from_secs(2));

    let body = handle.scrape_metrics();
    let committed = parse_metric(
        &body,
        "weir_sink_commit_records_total{outcome=\"committed\"}",
    );
    let commit_count = parse_metric(&body, "weir_sink_commit_duration_seconds_count");

    assert!(
        committed >= u64::from(N),
        "expected ≥{N} committed records, got {committed}\nmetrics excerpt:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_sink_"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        commit_count > 0,
        "expected at least one Sink::commit() call to have been recorded"
    );
    // The point of MySqlSink: many records per commit. With N=100 and the
    // drain reading whole sealed segments at once, ratio should be ≥ 10×.
    // Loose bound so the test isn't flaky on tiny-batch edge cases.
    let ratio = committed as f64 / commit_count as f64;
    assert!(
        ratio >= 10.0,
        "expected ≥10:1 records-per-commit IOPS compression, got {ratio:.1}:1 \
         ({committed} records / {commit_count} commits)"
    );
}

/// End-to-end check that records pushed to a daemon configured with
/// `sink_type = "postgres"` arrive in the configured table via a single
/// multi-row INSERT per batch — the Postgres counterpart of
/// `mysql_sink_end_to_end`.
///
/// Ignored by default because it requires a running PostgreSQL server
/// reachable at the URL in `WEIR_TEST_POSTGRES_URL`. The runner script
/// `scripts/run-sink-integration-tests.sh` brings up a docker-compose
/// stack with the right schema pre-seeded; manual setup with Docker:
///
/// ```sh
/// docker run --rm -d --name weir-test-postgres \
///   -e POSTGRES_PASSWORD=test \
///   -e POSTGRES_DB=weir_test \
///   -p 5432:5432 postgres:16
/// # Wait ~5s for postgres to come up.
/// docker exec weir-test-postgres psql -U postgres weir_test -c "
///   CREATE TABLE weir_records (
///     id BIGSERIAL PRIMARY KEY,
///     payload BYTEA NOT NULL,
///     payload_sha256 BYTEA GENERATED ALWAYS AS (sha256(payload)) STORED,
///     UNIQUE (payload_sha256)
///   );"
/// WEIR_TEST_POSTGRES_URL=postgres://postgres:test@127.0.0.1:5432/weir_test \
///   cargo test -p weir-server --test system -- --ignored postgres_sink_end_to_end
/// ```
#[test]
#[ignore = "requires WEIR_TEST_POSTGRES_URL pointing at a running Postgres with a prepared schema (see docstring)"]
fn postgres_sink_end_to_end() {
    const N: u32 = 100;

    let postgres_url = std::env::var("WEIR_TEST_POSTGRES_URL").expect(
        "WEIR_TEST_POSTGRES_URL not set — see the test docstring for the docker-compose recipe",
    );

    // sink_type = postgres; URL passed via env so credentials never touch
    // the config file (production-shaped — see the operations docs). The
    // postgres-specific knobs are mirror images of the mysql ones above
    // (different defaults, same shape).
    let handle = weir_server!("postgres")
        .batch_size(200)
        .batch_deadline_ms(5)
        .shutdown_timeout_secs(5)
        .env("WEIR_SINK_URL", &postgres_url)
        .extra_config("sink_type                 = \"postgres\"")
        .extra_config("sink_max_batch_size       = 1000")
        .extra_config("sink_postgres_table       = \"weir_records\"")
        .extra_config("sink_postgres_column      = \"payload\"")
        .extra_config("sink_postgres_insert_mode = \"on_conflict_do_nothing\"")
        .start();

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("postgres-rec-{i:05}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);

    // Give the drain a moment to drain the sealed segment into Postgres.
    thread::sleep(Duration::from_secs(2));

    let body = handle.scrape_metrics();
    let committed = parse_metric(
        &body,
        "weir_sink_commit_records_total{outcome=\"committed\"}",
    );
    let commit_count = parse_metric(&body, "weir_sink_commit_duration_seconds_count");

    assert!(
        committed >= u64::from(N),
        "expected ≥{N} committed records, got {committed}\nmetrics excerpt:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_sink_"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        commit_count > 0,
        "expected at least one Sink::commit() call to have been recorded"
    );
    // Same IOPS-compression assertion as the MySQL test — the Postgres
    // sink shares the multi-row INSERT shape, so the records-per-commit
    // ratio should be in the same ballpark.
    let ratio = committed as f64 / commit_count as f64;
    assert!(
        ratio >= 10.0,
        "expected ≥10:1 records-per-commit IOPS compression, got {ratio:.1}:1 \
         ({committed} records / {commit_count} commits)"
    );
}

fn parse_metric(body: &str, prefix: &str) -> u64 {
    for line in body.lines() {
        if line.starts_with(prefix)
            && let Some(val) = line.split_whitespace().next_back()
            && let Ok(n) = val.parse()
        {
            return n;
        }
    }
    0
}
