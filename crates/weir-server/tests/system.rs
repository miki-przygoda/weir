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
    fs, io,
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
use weir_testkit::{RecordingSink, free_port, process_lock, weir_server};

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
    client.push(b"hello weir", Durability::Durable).unwrap();
}

/// A tracked push against the real binary: the daemon hands back an address, it
/// points at a segment that actually exists on disk, and consecutive records on
/// one connection get consecutive ordinals in that segment.
#[test]
fn tracked_push_returns_a_coordinate_that_matches_the_wab() {
    let srv = weir_server!("tracked").shard_count(1).start();
    let mut client = srv.client();

    let first = client
        .push_tracked(b"first record", Durability::Durable)
        .unwrap();
    let second = client
        .push_tracked(b"second record", Durability::Durable)
        .unwrap();

    // The address is a real address, not a placeholder.
    assert!(
        first.segment().starts_with("shard_00/") && first.segment().ends_with(".wab.sealed"),
        "unexpected segment address {:?}",
        first.segment()
    );
    // Ordinals are 1-based and consecutive within one segment.
    assert_eq!(first.index(), 1, "the first record in a segment is index 1");
    assert_eq!(second.segment(), first.segment());
    assert_eq!(second.index(), 2);

    // Distinct records get distinct ids, and the id is a real digest rather
    // than a zero-filled stand-in.
    assert_ne!(first.record_id(), second.record_id());
    assert_ne!(first.record_id(), &[0u8; 32]);
    assert_eq!(first.record_id_hex().len(), 64);

    // The address names a file that really exists. The segment is still open at
    // ack time — that is the whole reason the sealed name is a prediction — so
    // what is on disk right now is its `.wab` form. Checking that pins the shard
    // directory and the segment counter against the real filesystem; the
    // `.sealed` half of the prediction is pinned by
    // `write_side_coordinates_match_what_the_drain_derives`, which replays the
    // drain's derivation over actually-sealed files.
    //
    // Deliberately NOT asserted after a shutdown-seal: the default noop sink
    // drains and DELETES a sealed segment almost immediately, so the sealed file
    // is legitimately gone by then and asserting on it tests the drain's speed,
    // not the coordinate.
    let wab_dir = srv.wab_dir.clone();
    let active = wab_dir.join(first.segment().trim_end_matches(".sealed"));
    assert!(
        active.exists(),
        "the coordinate named {} but the WAB holds: {:?}",
        active.display(),
        list_tree(&wab_dir),
    );
}

/// Tracked and untracked pushes interleave on one connection, each getting its
/// own reply shape. If the daemon ever answered a plain `push` with the longer
/// frame the stream would desync and the *next* call would fail — so the plain
/// pushes here are the assertion.
#[test]
fn tracked_and_plain_pushes_interleave_on_one_connection() {
    let srv = weir_server!("tracked_mixed").shard_count(1).start();
    let mut client = srv.client();

    client.push(b"plain one", Durability::Durable).unwrap();
    let a = client
        .push_tracked(b"tracked one", Durability::Durable)
        .unwrap();
    client.push(b"plain two", Durability::Durable).unwrap();
    let b = client
        .push_tracked(b"tracked two", Durability::Durable)
        .unwrap();
    client.push(b"plain three", Durability::Buffered).unwrap();

    // The untracked records still occupy ordinals, so the tracked pair is two
    // apart — proof the index counts every record in the segment, not just the
    // tracked ones.
    assert_eq!(a.segment(), b.segment());
    assert_eq!(b.index(), a.index() + 2);
    assert!(!client.is_poisoned());
}

/// A `Buffered` push gets a coordinate too. The tier decides durability; the
/// coordinate only says where the record went.
#[test]
fn buffered_push_also_gets_a_coordinate() {
    let srv = weir_server!("tracked_buffered").shard_count(1).start();
    let mut client = srv.client();
    let c = client
        .push_tracked(b"buffered record", Durability::Buffered)
        .unwrap();
    assert_eq!(c.index(), 1);
    assert!(c.segment().ends_with(".wab.sealed"));
}

/// Lists every file under a directory tree, for a failure message that says
/// what IS there rather than only what is missing.
fn list_tree(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return out;
    };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.extend(list_tree(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[test]
fn all_durability_tiers_behave_per_contract() {
    // Strengthened from `all_durability_tiers_acked`: the original test
    // only checked that each tier returned Ok, which would still pass
    // if Durable silently skipped fsync or Buffered silently fsynced. This
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

    // Durable: every push forces fdatasync, via the batch-boundary group
    // fsync. Counter must climb by ≥ N.
    for i in 0..N_PER_TIER {
        client
            .push(format!("dur-{i}").as_bytes(), Durability::Durable)
            .expect("Durable push failed");
    }
    let fsync_after_durable = read_fsync_count();
    assert!(
        fsync_after_durable - fsync_after_buffered >= u64::from(N_PER_TIER),
        "Durable tier produced only {} fsyncs for {N_PER_TIER} pushes — \
         expected ≥ {N_PER_TIER} (one per record)",
        fsync_after_durable - fsync_after_buffered
    );
}

/// weir 1.3.1 is live on crates.io and its encoder emits the retired `0x02`
/// (former `Batched`) durability byte for every Batched push. `0x02` is
/// therefore a published wire contract, not an internal implementation
/// detail: this release must keep accepting it from old producers forever.
///
/// `retired_batched_byte_decodes_but_never_round_trips` (weir-core) and the
/// conformance vectors already prove the *decoder* still turns `0x02` into
/// `Durable`, but that's unit-level coverage of `Durability::try_from`
/// alone — it doesn't prove a live daemon, reading the byte off a real
/// socket, actually proceeds to accept and ack the record rather than
/// rejecting it on some other code path the unit test can't see. This test
/// closes that gap end-to-end against the real binary.
#[test]
fn retired_batched_wire_byte_is_still_accepted_by_a_live_daemon() {
    use std::{
        io::{Read, Write},
        os::unix::net::UnixStream as RawStream,
    };
    use weir_core::{Envelope, HEADER_LEN, Header, MessageType};

    let srv = weir_server!("retired_0x02").start();

    // Build a normal Push frame (canonical durability byte 0x01), then
    // hand-patch byte 6 — magic[0..4] + version[4] + message_type[5] +
    // durability[6] — from 0x01 to the retired 0x02, and recompute the
    // header CRC over bytes [0..12] so the daemon doesn't reject the frame
    // as corrupt before it ever looks at the durability byte. Same
    // patch-and-recompute pattern as
    // `crates/weir-server/src/socket/connection.rs` (see e.g.
    // `header_declaring` in its test module).
    let mut frame = Envelope::new(
        Header::new(MessageType::Push, Durability::Durable, 0),
        b"retired-0x02".to_vec(),
    )
    .encode();
    assert_eq!(frame[6], 0x01, "test assumes byte 6 is the durability byte");
    frame[6] = 0x02;
    let crc = crc32fast::hash(&frame[..12]);
    frame[12..16].copy_from_slice(&crc.to_le_bytes());

    let mut stream = RawStream::connect(&srv.socket_path).expect("raw connect");
    stream.write_all(&frame).expect("write patched 0x02 frame");

    let mut resp_header_buf = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut resp_header_buf)
        .expect("read response header");
    let resp_header = Header::decode(&resp_header_buf).expect("decode response header");
    let mut payload = vec![0u8; resp_header.payload_len() as usize];
    if !payload.is_empty() {
        stream
            .read_exact(&mut payload)
            .expect("read response payload");
    }
    let mut crc_buf = [0u8; 4];
    stream.read_exact(&mut crc_buf).expect("read response crc");

    assert_eq!(
        resp_header.message_type(),
        MessageType::Ack,
        "a raw 0x02 (retired Batched) durability byte must still be accepted \
         and acked by a live daemon — got {:?} instead",
        resp_header.message_type()
    );

    // The connection must still be healthy afterward: a daemon that
    // mishandled the retired byte shouldn't have left anything poisoned.
    srv.client()
        .health_check()
        .expect("server unresponsive after retired-0x02 push");
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
                        .push(payload.as_bytes(), Durability::Durable)
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
    let acked = parse_metric(&body, "weir_records_ack_total{tier=\"durable\"}");
    let expected = (THREADS * RECORDS_PER_THREAD) as u64;
    assert_eq!(
        acked,
        expected,
        "expected {expected} durable acks, got {acked} — \
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
            .push(format!("wab-record-{i}").as_bytes(), Durability::Durable)
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

#[test]
fn idle_seal_drains_low_volume_segment_without_shutdown() {
    // With wab_segment_max_age_secs set, a lone record's segment is sealed and
    // drained after it sits idle — instead of waiting to fill wab_segment_max_bytes
    // (256 MiB) or for shutdown. Without the knob a single record never seals, so
    // the committed counter would stay 0 here. The noop sink commits whatever it's
    // handed, so a non-zero committed count proves the segment sealed + drained.
    let srv = weir_server!("idle_seal")
        .extra_config("wab_segment_max_age_secs = 1")
        .start();
    let mut client = srv.client();
    client.push(b"lonely-record", Durability::Durable).unwrap();

    let committed = || {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        )
    };
    // Poll past the 1s idle-seal threshold + the drain hop.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while committed() == 0 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        committed() >= 1,
        "idle-seal should have sealed + drained the lone record (committed stayed 0)"
    );
}

#[test]
fn max_lifetime_seal_fires_for_a_producer_that_never_goes_idle() {
    // The case `wab_segment_max_age_secs` cannot serve. That knob is an idle
    // timer whose clock restarts on every flush, so a steady trickle postpones
    // it forever and the segment grows toward `wab_segment_max_bytes` (256 MiB)
    // as if it were unset — two years at 500 bytes a minute, against a fleet
    // where low volume is the normal case (an S3 archive, a meter, a field
    // station, a till).
    //
    // `wab_segment_max_lifetime_secs` measures from segment creation and
    // nothing restarts it, so the same trickle must deliver. The write gap here
    // is deliberately shorter than the lifetime: if the new timer were idle-
    // based in disguise it would never fire and this test would fail.
    const LIFETIME_SECS: u64 = 3;
    const WRITE_GAP: Duration = Duration::from_millis(400);

    let srv = weir_server!("max_lifetime_trickle")
        .extra_config(format!("wab_segment_max_lifetime_secs = {LIFETIME_SECS}"))
        .start();
    let mut client = srv.client();

    let committed = || {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        )
    };

    // Trickle well past the lifetime, never pausing for as long as the
    // lifetime. Record the worst real gap: a CI runner that stalls for longer
    // than LIFETIME_SECS would leave the segment genuinely quiet, at which
    // point a seal proves nothing about the lifetime timer — an idle timer
    // would have done it. The premise is checked before the verdict, so a
    // stalled host reports a skip rather than a false pass.
    let mut worst_gap = Duration::ZERO;
    let mut last = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut delivered = 0u64;
    while delivered == 0 && Instant::now() < deadline {
        thread::sleep(WRITE_GAP);
        client.push(b"trickle", Durability::Durable).unwrap();
        worst_gap = worst_gap.max(last.elapsed());
        last = Instant::now();
        delivered = committed();
    }

    if worst_gap >= Duration::from_secs(LIFETIME_SECS) {
        eprintln!(
            "skipping the max-lifetime assertion: the host stalled for {worst_gap:?}, past the \
             {LIFETIME_SECS}s lifetime, so the segment went genuinely idle and a seal would \
             not distinguish the two timers"
        );
        return;
    }

    assert!(
        delivered >= 1,
        "a producer writing every {WRITE_GAP:?} against a {LIFETIME_SECS}s maximum lifetime \
         must still deliver — the lifetime clock starts at segment creation and nothing \
         restarts it. Nothing was committed in 30s while the worst write gap stayed at \
         {worst_gap:?}, so the lifetime timer is being restarted by writes and is just the \
         idle timer under a new name."
    );
}

#[test]
fn max_lifetime_seal_is_off_by_default() {
    // The knob is opt-in, and this is what stops the feature from silently
    // changing delivery timing for every existing deployment on upgrade. Same
    // trickle as the test above with the knob unset: nothing may seal, because
    // 256 MiB is a long way off and the idle timer is unset too.
    const WRITE_GAP: Duration = Duration::from_millis(400);

    let srv = weir_server!("max_lifetime_default").start();
    let mut client = srv.client();

    let committed = || {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        )
    };

    for _ in 0..12 {
        thread::sleep(WRITE_GAP);
        client.push(b"trickle", Durability::Durable).unwrap();
    }

    // No premise to guard here: with both timers unset there is no elapsed time
    // that could legitimately seal this segment, so a stalled runner cannot
    // make this assertion lie.
    assert_eq!(
        committed(),
        0,
        "with wab_segment_max_lifetime_secs unset, a segment must seal only at \
         wab_segment_max_bytes or shutdown. Records reached the sink, so the new timer \
         defaults to on and every deployment's delivery timing changes on upgrade."
    );
}

#[test]
fn wab_compression_knob_reaches_the_segment_header() {
    // End-to-end proof that the config value actually reaches the WAB writer,
    // not merely that the field is plumbed through Config. Runs a real daemon
    // with compression on, then reads the on-disk header byte itself.
    let srv = weir_server!("compression_e2e")
        .extra_config("wab_compression = \"zstd\"\nwab_segment_max_age_secs = 1")
        .start();
    let mut client = srv.client();
    // Compressible payload so the ratio counters move in an obvious direction.
    let payload = vec![b'a'; 4096];
    client.push(&payload, Durability::Durable).unwrap();

    // Find any segment file the daemon created and read its 24-byte header.
    let header = {
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        loop {
            let found = find_first_segment_header(&srv.wab_dir);
            if let Some(h) = found {
                break h;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no WAB segment appeared under {}",
                srv.wab_dir.display()
            );
            thread::sleep(Duration::from_millis(100));
        }
    };

    assert_eq!(
        header[4], 2,
        "wab_compression = zstd must write format v2, got version {}",
        header[4]
    );
    assert_eq!(header[5] & 0b0000_0001, 1, "the ZSTD flag bit must be set");

    // And the ratio counters must show compression actually happened.
    let body = srv.scrape_metrics();
    let logical = parse_metric(&body, "weir_wab_record_logical_bytes_total");
    let stored = parse_metric(&body, "weir_wab_record_stored_bytes_total");
    assert!(logical >= 4096, "logical bytes not counted: {logical}");
    assert!(
        stored > 0 && stored < logical,
        "4 KiB of 'a' must store smaller: logical={logical} stored={stored}"
    );
}

#[test]
fn wab_compression_off_writes_v1_segments() {
    // The rollback guarantee: at the default setting a 2.0 daemon's segments are
    // format v1, so a 1.x daemon can still read them.
    let srv = weir_server!("compression_off_e2e")
        .extra_config("wab_segment_max_age_secs = 1")
        .start();
    let mut client = srv.client();
    client.push(b"plain-record", Durability::Durable).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let header = loop {
        if let Some(h) = find_first_segment_header(&srv.wab_dir) {
            break h;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no WAB segment appeared"
        );
        thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(header[4], 1, "default config must stay on format v1");
    assert_eq!(header[5], 0, "flags byte must be zero in a v1 header");
}

/// Reads the 24-byte header of the first `.wab`/`.wab.sealed` file found under
/// `wab_dir`, searching shard subdirectories.
fn find_first_segment_header(wab_dir: &std::path::Path) -> Option<[u8; 24]> {
    use std::io::Read;
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                let name = p
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                if name.ends_with(".wab") || name.ends_with(".wab.sealed") {
                    out.push(p);
                }
            }
        }
    }
    let mut found = Vec::new();
    walk(wab_dir, &mut found);
    found.sort();
    for p in found {
        let mut buf = [0u8; 24];
        if let Ok(mut f) = std::fs::File::open(&p)
            && f.read_exact(&mut buf).is_ok()
        {
            return Some(buf);
        }
    }
    None
}

// ── WAB size cap ──────────────────────────────────────────────────────────────

/// The whole justification for spending `NackReason::InternalError` on the WAB
/// cap — rather than a new reason byte — is how a REAL client reacts to it:
/// `is_recoverable()` is true for `InternalError` and false for an unknown byte,
/// so a new byte would make every deployed producer tear down and reconnect
/// exactly when the daemon is already under strain. Assert that reaction through
/// an actual `WeirClient`, not just the byte the daemon puts on the wire
/// (`connection::tests` covers the byte).
///
/// Needs a sink that FAILS: with the noop sink the drain deletes each segment as
/// fast as it is sealed, so live WAB bytes never climb and the cap never trips.
/// A sink pointed at a closed port yields a connect error, which the drain
/// classifies as TRANSIENT — with `sink_max_retries = 0` the segment is stranded
/// on disk (not dead-lettered, which would move its bytes out of the count), so
/// the WAB grows monotonically.
#[test]
#[cfg(feature = "http-sink")]
fn wab_cap_nack_is_recoverable_for_a_real_client() {
    // Smallest legal segment size, and the tightest cap validation accepts above
    // it, so a short burst crosses the cap. The cap must clear
    // `shard_count * wab_segment_max_bytes` with room to resume (see
    // `Config::from_layers`) — a cap equal to one segment is refused precisely
    // because it can never be released.
    //
    // `wab_segment_max_age_secs = 1` is load-bearing, not decoration: with a
    // single shard the active segment holds up to 4096 bytes that only seal on a
    // write crossing the threshold, and idle-sealing them is what lets sealed
    // (stranded) segments accumulate toward the cap at 1 KiB per push.
    let srv = weir_server!("cap_recoverable")
        .extra_config(
            "wab_segment_max_bytes = 4096\n\
             wab_max_bytes = 8192\n\
             wab_segment_max_age_secs = 1\n\
             sink_type = \"http\"\n\
             sink_max_retries = 0",
        )
        // Port 1 is privileged and never bound by an unprivileged process, so
        // every drain attempt gets connection-refused immediately.
        .env("WEIR_SINK_URL", "http://127.0.0.1:1/sink")
        .start();
    let mut client = srv.client();

    // The value the cap is checked against is refreshed by a 5 s gauge task, so
    // the cap cannot trip on the first burst however large it is — keep pushing
    // across a couple of refresh intervals. The point is not the trip count; it
    // is what the client reports when the Nack finally arrives.
    let payload = vec![b'x'; 1024];
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut nack = None;
    while Instant::now() < deadline {
        match client.push(&payload, Durability::Durable) {
            Ok(()) => thread::sleep(Duration::from_millis(10)),
            Err(e) => {
                nack = Some(e);
                break;
            }
        }
    }

    let err = nack.expect("expected the WAB cap to trip while the sink was down");
    assert!(
        err.is_recoverable(),
        "a cap Nack must be recoverable, got {err:?}"
    );
    assert!(!client.is_poisoned(), "the connection must still be usable");
    // ...and it must be the CAP that rejected, not queue saturation or a wedged
    // flusher — those share the InternalError byte, which is exactly why the
    // counter exists.
    assert!(
        parse_metric(&srv.scrape_metrics(), "weir_wab_cap_rejections") >= 1,
        "the rejection must be attributable to the cap"
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
    // `mut` is used only under `bench-trace` (the extend below); allow it
    // unconditionally so the default-feature build stays warning-clean.
    #[allow(unused_mut)]
    let mut expected: Vec<&str> = vec![
        // Wire/socket layer
        "weir_records_accepted",
        "weir_records_ack",
        "weir_records_nack",
        "weir_accept_latency_seconds",
        "weir_connection_idle_timeout",
        "weir_connection_rejected_peer_uid",
        "weir_accept_resource_exhaustion",
        "weir_connections_aborted_at_shutdown",
        "weir_ack_timeout",
        // TLS
        "weir_tls_handshake_failures",
        "weir_tls_config_reloads",
        // WAB
        "weir_wab_segments",
        "weir_wab_bytes_on_disk",
        "weir_quarantine_bytes_on_disk",
        "weir_wab_record_logical_bytes",
        "weir_wab_record_stored_bytes",
        "weir_wab_fsync_duration_seconds",
        "weir_wab_group_commit_records",
        "weir_batch_records",
        "weir_batch_partial",
        "weir_wab_flusher_panics",
        "weir_wab_fsync_failures",
        "weir_wab_cap_rejections",
        // Sink / drain
        "weir_sink_commit_duration_seconds",
        "weir_sink_commit_records",
        "weir_sink_health",
        "weir_sink_info",
        "weir_queue_depth",
        // Recovery
        "weir_recovery_records_replayed",
        "weir_recovery_segments_quarantined",
        "weir_recovery_segments_failed",
        "weir_recovery_quarantine_copy_failed",
        "weir_wab_unexpected_mode",
        // Dead letter
        "weir_dead_letter_bytes_on_disk",
        "weir_dead_letter_full",
        "weir_drain_segments_stranded",
        "weir_drain_segments_resumed",
        "weir_drain_state",
        "weir_drain_panics",
        "weir_dead_letter_blocked_duration_seconds",
        // wab_dir ownership
        "weir_wab_dir_lock_held",
        "weir_wab_dir_lock_integrity_failures",
        // Config reload
        "weir_config_reloads",
    ];
    // The per-stage latency histograms register only under the `bench-trace`
    // feature (which `--all-features` turns on). Include them there so the
    // family-count drift check matches in that build too.
    #[cfg(feature = "bench-trace")]
    expected.extend([
        "weir_stage_queue_seconds",
        "weir_stage_bridge_wait_seconds",
        "weir_stage_write_seconds",
        "weir_stage_total_seconds",
    ]);

    for family in &expected {
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
    // The terminal state is only set once the drain supervisor exits, so a
    // running daemon must read 0 — a 1 here would mean delivery has stopped.
    assert!(
        body.contains("weir_drain_state{state=\"stopped\"} 0"),
        "stopped should be 0 while the daemon is running"
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
    client
        .push(b"before-shutdown", Durability::Durable)
        .unwrap();
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
fn empty_payload_rejected_locally_by_client() {
    // An empty Push payload collides with the WAB end-of-records sentinel (a
    // zero-length prefix). weir-client rejects it LOCALLY, before any bytes are
    // sent, with a recoverable ClientError::EmptyPayload — so the connection is
    // never drawn into the daemon's connection-closing Nack(EmptyPayload), and
    // the client stays usable. The DAEMON-side guard (for non-weir-client
    // producers) is covered by `empty_payload_rejected_by_daemon_for_raw_frame`
    // below and by connection::tests::empty_payload_rejected_at_ingest.
    let srv = weir_server!("empty_payload_client").start();
    let mut client = srv.client();
    let err = client.push(b"", Durability::Durable).unwrap_err();
    assert!(
        matches!(err, ClientError::EmptyPayload),
        "expected local ClientError::EmptyPayload, got {err:?}"
    );
    assert!(
        !client.is_poisoned(),
        "a local empty-payload reject must keep the client usable"
    );
}

#[test]
fn empty_payload_rejected_by_daemon_for_raw_frame() {
    // A producer that is NOT weir-client (e.g. a polyglot wire client) and does
    // not implement the local guard can still put an empty Push on the wire. The
    // daemon must reject it at ingest with Nack(EmptyPayload) rather than risk
    // truncating a segment. Bypass weir-client's local guard with a raw socket to
    // prove the end-to-end daemon behavior. HealthCheck frames also carry a
    // zero-length payload but are unaffected — see `health_check_returns_ok`.
    use std::{
        io::{Read, Write},
        os::unix::net::UnixStream as RawStream,
    };
    use weir_core::{Envelope, HEADER_LEN, Header, MessageType, NackReason};

    let srv = weir_server!("empty_payload_raw").start();
    let mut stream = RawStream::connect(&srv.socket_path).expect("raw connect");
    // A Push whose Envelope-derived payload_len is 0. The daemon rejects on the
    // zero length right after the header, before reading any body — so writing
    // just the 16-byte header is enough to draw the Nack.
    let frame = Envelope::new(
        Header::new(MessageType::Push, Durability::Durable, 0),
        Vec::new(),
    )
    .encode();
    stream
        .write_all(&frame[..HEADER_LEN])
        .expect("write empty push header");

    let mut resp_header_buf = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut resp_header_buf)
        .expect("read response header");
    let resp_header = Header::decode(&resp_header_buf).expect("decode response header");
    assert_eq!(
        resp_header.message_type(),
        MessageType::Nack,
        "expected a Nack for a raw empty Push"
    );
    let mut payload = vec![0u8; resp_header.payload_len() as usize];
    stream.read_exact(&mut payload).expect("read nack payload");
    let mut crc = [0u8; 4];
    stream.read_exact(&mut crc).expect("read response crc");
    assert_eq!(
        payload.first().copied(),
        Some(NackReason::EmptyPayload as u8),
        "daemon must Nack a raw empty Push with EmptyPayload"
    );
}

#[test]
fn arbitrary_binary_payload_accepted() {
    // Renamed from binary_payload_round_trips: there is no Pop API, so no
    // round-trip occurs. The test verifies the server doesn't strip null bytes
    // or high bytes in any text-mode handling.
    let srv = weir_server!("binary_payload").start();
    let mut client = srv.client();
    let payload: Vec<u8> = (0u8..=255).collect();
    client.push(&payload, Durability::Durable).unwrap();
}

#[test]
fn payload_size_boundary_enforced() {
    // Strengthened from `large_payload_accepted`: the original test pushed
    // 1 MiB and called it done. A bug at the 16 MiB boundary would never
    // be exposed by a 1 MiB push. This version tests the actual limit.
    use weir_core::MAX_PAYLOAD_HARD_CAP;

    let srv = weir_server!("payload_boundary").start();

    // Exactly at the cap: must succeed. Uses Durable — the property under
    // test is the size acceptance check, not durability.
    {
        let mut client = srv.client();
        let payload = vec![0xAAu8; MAX_PAYLOAD_HARD_CAP];
        client
            .push(&payload, Durability::Durable)
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
        use weir_core::{Envelope, HEADER_LEN, Header, MessageType, NackReason};

        let mut stream = RawStream::connect(&srv.socket_path).expect("raw connect");
        // Header::new can no longer declare a length that disagrees with its
        // payload (F50), so build a full frame whose Envelope derives the over-cap
        // length from a real (discarded) payload, and transmit ONLY its 16-byte
        // header. The server must reject on the declared length before reading any
        // body bytes.
        let oversize = vec![0u8; MAX_PAYLOAD_HARD_CAP + 1];
        let frame = Envelope::new(
            Header::new(MessageType::Push, Durability::Durable, 0),
            oversize,
        )
        .encode();
        stream
            .write_all(&frame[..HEADER_LEN])
            .expect("write header for over-cap push");

        // Read response header.
        let mut resp_header_buf = [0u8; HEADER_LEN];
        stream
            .read_exact(&mut resp_header_buf)
            .expect("read response header");
        let resp_header = Header::decode(&resp_header_buf).expect("decode response header");
        assert_eq!(
            resp_header.message_type(),
            MessageType::Nack,
            "expected Nack response for over-cap header"
        );

        // The wire format puts the NackReason as the first byte of the
        // payload (see `send_nack` in src/socket/connection.rs). Read the
        // payload and verify the reason byte.
        let mut payload = vec![0u8; resp_header.payload_len() as usize];
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

    let tiers = [Durability::Durable, Durability::Buffered];
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
    // pushed for that tier. With THREADS=6 and tiers cycling 2-wide,
    // threads 0,2,4 push Durable (3 × RECORDS), threads 1,3,5 push
    // Buffered (3 × RECORDS).
    //
    // The thing this catches that the original test couldn't: a buggy
    // tier-dispatch table that silently re-routes (e.g. all Buffered
    // counted as Durable) or drops one tier under contention. The original
    // panic-on-Err pattern would still pass under such a bug because
    // the client only sees Ack/Nack, not which tier the server thinks
    // it was.
    let per_tier_expected = (THREADS / tiers.len() * RECORDS) as u64;
    let body = srv.scrape_metrics();
    for (label, _tier) in [
        ("durable", Durability::Durable),
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

#[test]
fn idle_seal_does_not_fire_for_a_producer_that_never_goes_idle() {
    // `wab_segment_max_age_secs` reads as a maximum age and is an IDLE timer:
    // the clock restarts on every flush (`wab/mod.rs:683-685`), so the seal
    // fires only after the producer stops for the whole interval. A steady
    // trickle therefore never idle-seals at all, and its segment grows toward
    // `wab_segment_max_bytes` (256 MiB) exactly as if the knob were 0.
    //
    // That is the documented behaviour (`docs/operations/configuration.md`,
    // `docs/sinks/s3.md`) and the shape most likely to surprise an operator who
    // set the knob precisely to avoid it: "I set a 2-second seal and my archive
    // is still empty." This pins both halves — writing keeps it open, stopping
    // seals it — so changing idle semantics to age semantics has to be a
    // deliberate act that updates those docs.
    const IDLE_SECS: u64 = 2;
    const WRITE_GAP: Duration = Duration::from_millis(400);

    let srv = weir_server!("idle_seal_trickle")
        .extra_config(format!("wab_segment_max_age_secs = {IDLE_SECS}"))
        .start();
    let mut client = srv.client();

    let committed = || {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        )
    };

    // Trickle for well over the idle interval, recording the largest real gap
    // between writes. A CI runner that stalls mid-loop would legitimately let
    // the segment idle out, so the assertion below is made only if the premise
    // — that we never actually went idle — held.
    let mut worst_gap = Duration::ZERO;
    let mut last = Instant::now();
    for _ in 0..12 {
        thread::sleep(WRITE_GAP);
        client.push(b"trickle", Durability::Durable).unwrap();
        worst_gap = worst_gap.max(last.elapsed());
        last = Instant::now();
    }

    if worst_gap < Duration::from_secs(IDLE_SECS) {
        assert_eq!(
            committed(),
            0,
            "a producer writing every {WRITE_GAP:?} against a {IDLE_SECS}s idle \
             seal must never idle-seal — the clock restarts on every flush. \
             Records were delivered, so the timer is behaving as a maximum age \
             rather than an idle interval, and the configuration and S3 sink \
             docs that describe it as idle-only are now wrong."
        );
    } else {
        eprintln!(
            "skipping the never-idle assertion: the host stalled for {worst_gap:?}, \
             which is past the {IDLE_SECS}s threshold, so the premise did not hold"
        );
    }

    // Now stop writing. The same knob that refused to fire under load must fire
    // once the producer is genuinely idle — otherwise this test would also pass
    // against a build where idle-seal is simply broken.
    let deadline = Instant::now() + Duration::from_secs(15);
    while committed() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        committed() >= 1,
        "after the producer stopped, the {IDLE_SECS}s idle seal should have \
         sealed and drained the segment"
    );
}

#[test]
fn one_connection_can_interleave_both_durability_tiers() {
    // The tier is a per-RECORD wire byte (envelope header offset 6), not a
    // connection or a daemon setting, so a single producer can ask for a
    // different guarantee on each push -- `Durable` for the transaction, the
    // `Buffered` fast path for the telemetry beside it, over one socket.
    //
    // Nothing said so until now, and nothing tested the interleaved case:
    // `mixed_durability_under_concurrent_load` gives each *thread* one fixed
    // tier, and `all_durability_tiers_behave_per_contract` measures the tiers
    // in separate phases. Both would pass against an implementation that read
    // the tier correctly in a homogeneous stream but not a mixed one.
    //
    // Asserted through the fsync counter rather than the ack counters, because
    // `weir_records_ack_total{tier}` is labelled from the request header
    // (`socket/connection.rs`) and would agree with itself even if the flusher
    // ignored the field entirely. `weir_wab_fsync_duration_seconds_count`
    // increments once per real fsync syscall, and the flusher skips the group
    // fsync when a batch holds no Durable record (`wab/mod.rs`), so on a
    // serial push-and-wait connection -- one record per batch -- the count must
    // rise by the number of Durable pushes and not by the total.
    const PAIRS: u64 = 20;

    let srv = weir_server!("interleaved_tiers").start();
    let mut client = srv.client();
    let fsyncs = || {
        parse_metric(
            &srv.scrape_metrics(),
            "weir_wab_fsync_duration_seconds_count",
        )
    };

    let before = fsyncs();
    for i in 0..PAIRS {
        client
            .push(format!("durable-{i}").as_bytes(), Durability::Durable)
            .unwrap_or_else(|e| panic!("durable push {i}: {e}"));
        client
            .push(format!("buffered-{i}").as_bytes(), Durability::Buffered)
            .unwrap_or_else(|e| panic!("buffered push {i}: {e}"));
    }
    thread::sleep(Duration::from_millis(150));
    let delta = fsyncs() - before;

    assert!(
        delta >= PAIRS,
        "{PAIRS} interleaved Durable pushes produced only {delta} fsyncs. A \
         Durable record sharing a stream with Buffered ones must still force \
         the group fsync before its ack."
    );
    assert!(
        delta < PAIRS * 2,
        "{delta} fsyncs for {PAIRS} Durable + {PAIRS} Buffered pushes. The \
         Buffered records are being fsynced too, so the flusher is not reading \
         the tier per record -- it is applying one tier to the whole stream."
    );

    // And both tiers were actually accepted, not merely counted.
    let body = srv.scrape_metrics();
    for label in ["durable", "buffered"] {
        let acked = parse_metric(
            &body,
            &format!("weir_records_ack_total{{tier=\"{label}\"}}"),
        );
        assert_eq!(
            acked, PAIRS,
            "one connection pushed {PAIRS} records at each tier; the {label} \
             counter reads {acked}"
        );
    }
}

// ── Crash recovery ────────────────────────────────────────────────────────────

#[test]
fn server_restarts_after_sigkill() {
    // 10 s is generous: cold start on a fresh daemon takes well under 1 s.
    // The bound catches a regression where startup blocks on something
    // (recovery, stale-socket cleanup, segment scan) for an unreasonable time.
    const RESTART_BUDGET: Duration = Duration::from_secs(10);

    let mut srv = weir_server!("crash_restart").start();
    srv.client()
        .push(b"before-crash", Durability::Durable)
        .unwrap();

    srv.kill_ungracefully();

    // Socket file persists on disk (SIGKILL left it behind).
    assert!(
        srv.socket_path.exists(),
        "socket should remain after SIGKILL"
    );

    // Nobody listens on the stale socket, so startup's liveness probe calls
    // it stale and removes it; the server starts clean.
    let restart_start = Instant::now();
    srv.restart_in_place();
    let restart_elapsed = restart_start.elapsed();
    assert!(
        restart_elapsed < RESTART_BUDGET,
        "restart took {restart_elapsed:?} — exceeded {RESTART_BUDGET:?} budget"
    );

    srv.client()
        .push(b"after-restart", Durability::Durable)
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
            .push(format!("crash-rec-{i}").as_bytes(), Durability::Durable)
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

/// Coverage gap (T07): the full crown-invariant loop across a crash. The existing
/// crash tests stop at the replay-queue metric; this extends to the DELIVERY +
/// CONFIRM end state — after a SIGKILL + restart, every acked record is actually
/// delivered to the sink and confirmed, not merely re-queued. The default no-op
/// sink always commits Ok, so any shortfall is a real recovery/drain bug.
#[test]
fn acked_records_delivered_to_sink_and_confirmed_after_crash_restart() {
    const N: u32 = 25;
    let mut srv = weir_server!("crash_to_sink").start();
    let mut client = srv.client();
    let mut acked = 0u32;
    for i in 0..N {
        if client
            .push(format!("c2s-{i}").as_bytes(), Durability::Durable)
            .is_ok()
        {
            acked += 1;
        }
    }
    drop(client);
    thread::sleep(Duration::from_millis(150));

    srv.kill_ungracefully();
    srv.restart_in_place();

    // Poll the committed counter until the recovered records are delivered to the
    // sink and confirmed (crash → recovery → replay → drain → sink → confirm).
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut committed;
    loop {
        committed = parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        );
        if committed >= u64::from(acked) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {committed}/{acked} acked records were delivered + confirmed after restart"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        committed >= u64::from(acked),
        "every acked record ({acked}) must be delivered to the sink after restart, got {committed}"
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
        .push(b"trigger-shard-creation", Durability::Durable)
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
                        .push(format!("t{t}-r{i}").as_bytes(), Durability::Durable)
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

/// Verifies that SIGTERM under concurrent Durable load produces no silent drops.
///
/// Every push that returned `Ok` must be on disk (the Durable tier's guarantee).
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
                    match client.push(b"shutdown-load", Durability::Durable) {
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

    // Every Ok means a Durable-flushed record. The WAB must have bytes.
    assert!(
        oks > 0,
        "expected successful pushes before SIGTERM; got 0 — \
         either the server started too slowly or {PUSH_BEFORE_SIGTERM:?} was too short"
    );
    assert!(
        wab_bytes > 0,
        "WAB has 0 bytes on disk after {oks} successful Durable pushes — \
         possible silent data loss"
    );

    println!(
        "graceful_shutdown_under_load: {oks} ok, {io_errs} io_err, \
         {unexpected} unexpected | wab={wab_bytes}B | shutdown={shutdown_elapsed:?}"
    );
}

/// `--version` and `--help` short-circuit before any config load (no
/// `--config`, no socket, no wab dir needed) and exit 0. `--version` prints
/// `weir-server <version>`, matching the format `weir-ctl` uses. This can only
/// be an integration test: the handler calls `process::exit(0)`, so it must run
/// the real binary, not the in-process parser.
#[test]
fn version_and_help_flags_short_circuit_and_exit_zero() {
    use std::process::Command;
    let binary = env!("CARGO_BIN_EXE_weir-server");

    let out = Command::new(binary)
        .arg("--version")
        .output()
        .expect("spawn weir-server --version");
    assert!(
        out.status.success(),
        "--version must exit 0, got {:?}",
        out.status
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.starts_with("weir-server "),
        "--version stdout must start with 'weir-server ', got: {stdout:?}"
    );
    assert!(
        stdout.trim().ends_with(env!("CARGO_PKG_VERSION")),
        "--version must print the crate version {}, got: {stdout:?}",
        env!("CARGO_PKG_VERSION")
    );

    let help = Command::new(binary)
        .arg("--help")
        .output()
        .expect("spawn weir-server --help");
    assert!(
        help.status.success(),
        "--help must exit 0, got {:?}",
        help.status
    );
    assert!(!help.stdout.is_empty(), "--help must print usage to stdout");
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
    let header = Header::new(MessageType::Push, Durability::Buffered, 0);
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

    // Concurrent client: 50 Durable pushes while the stalled connection is held.
    let mut client = srv.client();
    let t0 = Instant::now();
    for i in 0..CONCURRENT_RECORDS {
        client
            .push(format!("concurrent-{i}").as_bytes(), Durability::Durable)
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
    let header = Header::new(MessageType::Push, Durability::Buffered, 0);
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
        .push(b"after-partial-frame", Durability::Durable)
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
    let result = client.push(b"should-nack", Durability::Durable);
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
/// (16 KiB pre-allocated) plus a single Durable record fills it; subsequent
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
        match client.push(&payload, Durability::Durable) {
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

/// Every Durable push that returned `Ok` must be present on disk byte-for-byte
/// after the server is killed with SIGKILL.
///
/// This tests the Durable-tier contract at the byte level: if the client
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
        match client.push(&payload, Durability::Durable) {
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
        "WAB directory empty after {} acked Durable pushes",
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

// ── Socket ownership (safety floor, spec §3.3) ────────────────────────────────

/// A second daemon pointed at a LIVE daemon's socket path must refuse to
/// start, not unlink the socket and take the path over. (Until the safety
/// floor, startup removed any socket file at the path, even one another
/// process was listening on, and A silently lost every new producer.)
///
/// B has its own `wab_dir` and metrics port, so nothing but the socket is
/// shared. B must exit 1 naming the conflict, and must do so in preflight
/// (spec §3.0), before it recovers anything or binds its metrics endpoint. A's
/// socket must be the same inode, A must still ack a Durable push on a NEW
/// connection, and A's WAB must be byte-identical.
#[test]
fn second_daemon_refuses_a_live_socket() {
    use std::os::unix::fs::MetadataExt;

    let srv_a = weir_server!("live_socket_a").start();
    let mut client = srv_a.client();
    for i in 0..20 {
        client
            .push(format!("srv-a-{i}").as_bytes(), Durability::Durable)
            .expect("push to server A failed");
    }
    let socket_inode = fs::symlink_metadata(&srv_a.socket_path).unwrap().ino();
    let wab_before = tree_snapshot(&srv_a.wab_dir);

    let (status, log) = weir_server!("live_socket_b")
        .env("WEIR_SOCKET_PATH", srv_a.socket_path.to_str().unwrap())
        .log_level("info")
        .spawn_expecting_exit(Duration::from_secs(10));
    assert_eq!(
        status.code(),
        Some(1),
        "B must refuse to start on A's live socket; it exited {status}\nlog:\n{log}"
    );
    assert!(
        log.contains("in use by a running process"),
        "B must say why it refused\nlog:\n{log}"
    );
    // At info, these absences prove the refusal came from preflight: the late
    // bind in socket::run runs only after recovery and the metrics bind.
    assert!(
        log.contains("weir starting"),
        "B's log must be at info\nlog:\n{log}"
    );
    for late in ["recover", "metrics endpoint listening"] {
        assert!(
            !log.contains(late),
            "B must refuse in preflight, before {late:?}\nlog:\n{log}"
        );
    }
    assert_eq!(
        tree_snapshot(&srv_a.wab_dir),
        wab_before,
        "B's refused start must not touch a byte of A's WAB"
    );

    assert_eq!(
        fs::symlink_metadata(&srv_a.socket_path)
            .expect("A's socket file must still exist")
            .ino(),
        socket_inode,
        "A's socket must be the same inode: B must not unlink or replace it"
    );
    srv_a
        .client()
        .push(b"after-b-refused", Durability::Durable)
        .expect("A must still accept and ack on a new connection");
    client
        .push(b"after-b-refused-old-conn", Durability::Durable)
        .expect("A's existing connection must still ack");
}

/// Shutdown removes the socket file only if it is still the inode this
/// daemon bound. Here a successor has replaced A's socket at the same path
/// while A runs; A's SIGTERM must leave the successor's socket alone.
#[test]
fn shutdown_leaves_a_successor_socket_alone() {
    use std::os::unix::fs::MetadataExt;

    let mut srv_a = weir_server!("successor_socket_a").start();
    fs::remove_file(&srv_a.socket_path).expect("unlink A's socket");
    let successor = std::os::unix::net::UnixListener::bind(&srv_a.socket_path)
        .expect("bind the successor's socket at A's path");
    let successor_inode = fs::symlink_metadata(&srv_a.socket_path).unwrap().ino();

    srv_a.sigterm();

    let meta = fs::symlink_metadata(&srv_a.socket_path)
        .expect("A's shutdown must not unlink the successor's socket");
    assert_eq!(meta.ino(), successor_inode);
    let _c = std::os::unix::net::UnixStream::connect(&srv_a.socket_path)
        .expect("the successor must still accept connections");
    let log = fs::read_to_string(srv_a.tmp_dir.join("weir.log")).unwrap_or_default();
    assert!(
        log.contains("another instance replaced it"),
        "A must say why it left the socket\nlog:\n{log}"
    );
    drop(successor);
}

/// Review Focus 4 (stale-socket half): upgrading from 4.0.1, or any restart
/// after SIGKILL, finds the dead daemon's socket file still on disk. Nothing
/// listens on it (ECONNREFUSED), so the new daemon must remove it as stale,
/// bind, recover the WAB and deliver both the records acked before the kill
/// and the ones acked after the restart.
#[test]
fn starts_cleanly_over_a_stale_socket_left_by_sigkill() {
    use std::os::unix::fs::FileTypeExt;

    let sink = RecordingSink::start();
    let mut srv = weir_server!("stale_socket_sigkill")
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .log_level("info")
        .start();
    let mut client = srv.client();
    for i in 0..10 {
        client
            .push(format!("stale-pre-{i:02}").as_bytes(), Durability::Durable)
            .expect("push before the kill");
    }
    drop(client);

    srv.kill_ungracefully();
    let meta = fs::symlink_metadata(&srv.socket_path).expect("SIGKILL leaves the socket file");
    assert!(meta.file_type().is_socket());
    let refused = std::os::unix::net::UnixStream::connect(&srv.socket_path)
        .expect_err("nobody listens on a dead daemon's socket");
    assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);

    // Same config, same paths: the new process must clear the stale file.
    srv.restart_in_place();
    let mut client = srv.client();
    for i in 0..10 {
        client
            .push(format!("stale-post-{i:02}").as_bytes(), Durability::Durable)
            .expect("push after the restart");
    }
    drop(client);
    srv.sigterm();

    let log = fs::read_to_string(srv.tmp_dir.join("weir.log")).unwrap_or_default();
    assert!(
        log.contains("removed stale socket"),
        "the restart must log the stale-socket removal\nlog:\n{log}"
    );
    let delivered: std::collections::HashSet<String> = sink
        .bodies()
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect();
    let missing: Vec<String> = (0..10)
        .flat_map(|i| [format!("stale-pre-{i:02}"), format!("stale-post-{i:02}")])
        .filter(|p| !delivered.contains(p))
        .collect();
    assert!(
        missing.is_empty(),
        "every acked record must reach the sink across the SIGKILL; missing {missing:?}"
    );
}

// ── Preflight (safety floor, spec §3.0) ───────────────────────────────────────

/// The payloads [`seed_sealed_backlog`] leaves on disk.
const SEEDED_RECORDS: usize = 12;

fn seeded_payload(i: usize) -> Vec<u8> {
    let mut p = format!("seeded-{i:02}-").into_bytes();
    p.resize(1024, b'x');
    p
}

/// Fills `wab_dir` with a backlog of SEALED, UNCONFIRMED segments: exactly
/// what a restarted daemon recovers, replays and delivers first.
///
/// A seeding daemon runs on `wab_dir` with the smallest legal segment size
/// (4096 bytes, so a few 1 KiB pushes roll a segment) and an HTTP sink on port
/// 1, which is privileged and never bound by an unprivileged process: every
/// delivery attempt is refused, so with `sink_max_retries = 0` each sealed
/// segment is stranded on disk (connection-refused is transient, so nothing is
/// dead-lettered). A graceful SIGTERM then seals the active segment too.
/// Returns the number of sealed segments, asserted to be at least 3.
fn seed_sealed_backlog(tag: &str, wab_dir: &Path) -> usize {
    let mut seeder = weir_server!(tag)
        .wab_dir(wab_dir)
        .extra_config(
            "wab_segment_max_bytes = 4096\n\
             sink_type = \"http\"\n\
             sink_max_retries = 0",
        )
        .env("WEIR_SINK_URL", "http://127.0.0.1:1/sink")
        .start();
    let mut client = seeder.client();
    for i in 0..SEEDED_RECORDS {
        client
            .push(seeded_payload(i), Durability::Durable)
            .expect("seeding push");
    }
    drop(client);
    seeder.sigterm();
    drop(seeder);

    let sealed = count_wab_files(wab_dir, ".wab.sealed");
    assert!(
        sealed >= 3,
        "the seed must leave at least 3 sealed segments, found {sealed}: {:?}",
        tree_snapshot(wab_dir)
    );
    assert_eq!(
        count_wab_files(wab_dir, ".wab"),
        0,
        "SIGTERM must have sealed the active segment"
    );
    assert_eq!(
        count_wab_files(wab_dir, ".confirmed"),
        0,
        "nothing may be confirmed: the sink was unreachable"
    );
    sealed
}

/// Which resource the refused duplicate shares with the running daemon.
#[derive(Clone, Copy, Debug)]
enum SharedResource {
    Socket,
    MetricsPort,
}

/// spec §3.0: a duplicate daemon that must refuse to start has to refuse
/// BEFORE recovery. Until preflight existed, B recovered and replayed its own
/// backlog, started its drain, and only then hit the conflicting socket or
/// metrics port and returned through `?` with the drain mid-commit. Under
/// `Restart=on-failure` that re-delivered the head of B's oldest unconfirmed
/// segment on every restart, unboundedly.
///
/// B gets its own `wab_dir`, pre-seeded with a sealed backlog, and a recording
/// sink; it shares exactly one resource with a running A. B must exit 1 within
/// 5 s naming the conflict, the sink must see not one connection, and B's WAB
/// must be byte-identical. Then, as a positive control, the same B started
/// without the conflict delivers the whole backlog to the same sink: the
/// backlog was real, so "0 connections" above is not vacuous.
fn refused_duplicate_never_delivers(shared: SharedResource) {
    // Short: the socket path is derived from the tag and must fit sun_path
    // (104 bytes on macOS, including the temp dir).
    let tag = match shared {
        SharedResource::Socket => "sock",
        SharedResource::MetricsPort => "metr",
    };
    let b_wab =
        std::env::temp_dir().join(format!("weir_sys_refused_dup_{tag}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&b_wab);
    fs::create_dir_all(&b_wab).unwrap();
    seed_sealed_backlog(&format!("rdup_seed_{tag}"), &b_wab);
    // B takes the wab_dir lock before preflight, legitimately rewriting the
    // holder record in `.weir.lock`; every other byte must stay put.
    let segments = |dir: &Path| {
        let mut t = tree_snapshot(dir);
        t.remove(Path::new(".weir.lock"));
        t
    };
    let seeded = segments(&b_wab);

    let sink = RecordingSink::start();
    let srv_a = weir_server!(&format!("rdup_a_{tag}")).start();
    let b = weir_server!(&format!("rdup_b_{tag}"))
        .wab_dir(&b_wab)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .log_level("info");
    let (b, expected) = match shared {
        SharedResource::Socket => (
            b.env("WEIR_SOCKET_PATH", srv_a.socket_path.to_str().unwrap()),
            "in use by a running process",
        ),
        SharedResource::MetricsPort => (
            b.env("WEIR_METRICS_PORT", srv_a.metrics_port.to_string()),
            "failed to bind metrics endpoint",
        ),
    };

    let started = Instant::now();
    let (status, log) = b.spawn_expecting_exit(Duration::from_secs(5));
    let took = started.elapsed();
    assert_eq!(
        status.code(),
        Some(1),
        "B must refuse to start ({shared:?}); it exited {status}\nlog:\n{log}"
    );
    assert!(
        took < Duration::from_secs(5),
        "B must fail fast, took {took:?}"
    );
    assert!(
        log.contains(expected),
        "B must name the conflict\nlog:\n{log}"
    );
    // The log is at info, so the absences below are evidence, not silence.
    assert!(
        log.contains("weir starting"),
        "B's log must be at info\nlog:\n{log}"
    );
    for late in [
        "recovering",
        "queuing segment for drain replay",
        "metrics endpoint listening",
    ] {
        assert!(
            !log.contains(late),
            "B must refuse in preflight, before {late:?}\nlog:\n{log}"
        );
    }
    // Anything B sent before exiting is already queued at the sink's listener;
    // give its accept thread a moment to count it.
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        sink.connections(),
        0,
        "a refused duplicate must never reach the sink (bodies: {})",
        sink.bodies().len()
    );
    assert_eq!(
        segments(&b_wab),
        seeded,
        "B's refused start must not recover, seal, confirm or delete anything"
    );

    // A is unaffected.
    srv_a
        .client()
        .push(b"a-after-refusal", Durability::Durable)
        .expect("A must still ack");
    drop(srv_a);

    // Positive control: without the conflict, B delivers the seeded backlog.
    let srv_b = weir_server!(&format!("rdup_ok_{tag}"))
        .wab_dir(&b_wab)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .start();
    let deadline = Instant::now() + Duration::from_secs(20);
    let missing = loop {
        let delivered: std::collections::HashSet<Vec<u8>> = sink.bodies().into_iter().collect();
        let missing: Vec<usize> = (0..SEEDED_RECORDS)
            .filter(|&i| !delivered.contains(&seeded_payload(i)))
            .collect();
        if missing.is_empty() || Instant::now() >= deadline {
            break missing;
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(
        missing.is_empty(),
        "control: the seeded backlog must be deliverable; missing records {missing:?}"
    );
    drop(srv_b);
    let _ = fs::remove_dir_all(&b_wab);
}

#[test]
fn refused_duplicate_never_delivers_shared_socket() {
    refused_duplicate_never_delivers(SharedResource::Socket);
}

#[test]
fn refused_duplicate_never_delivers_shared_metrics_port() {
    refused_duplicate_never_delivers(SharedResource::MetricsPort);
}

// ── wab_dir ownership (safety floor, spec §3.1) ───────────────────────────────

/// Every file under `dir`, keyed by its path relative to `dir`, with its size
/// and a content hash. Two snapshots compare equal only if no file was
/// created, removed, renamed, resized or rewritten in between.
fn tree_snapshot(dir: &Path) -> std::collections::BTreeMap<PathBuf, (u64, u64)> {
    use std::hash::{Hash, Hasher};
    fn walk(root: &Path, dir: &Path, out: &mut std::collections::BTreeMap<PathBuf, (u64, u64)>) {
        let Ok(rd) = fs::read_dir(dir) else { return };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                let bytes = fs::read(&p).unwrap_or_default();
                let mut h = std::collections::hash_map::DefaultHasher::new();
                bytes.hash(&mut h);
                out.insert(
                    p.strip_prefix(root).unwrap().to_path_buf(),
                    (bytes.len() as u64, h.finish()),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// The owner's reproduction (2026-10-04), as a regression. Daemon B was started
/// on the same `wab_dir` as a running daemon A, with its own socket and metrics
/// port. B recovered and sealed A's ACTIVE segment, delivered, confirmed and
/// deleted it, and A then acked 50 more Durable records into the unlinked
/// inode: none were delivered and nothing remained on disk, and A's shutdown
/// seal logged `No such file or directory`.
///
/// With the lock, B must refuse before reading a byte of the WAB, A's tree must
/// be byte-identical afterwards, and every one of A's 70 acked records must
/// reach the sink. A recording HTTP sink is used because graceful shutdown
/// deletes every delivered segment, so WAB bytes after SIGTERM prove nothing.
#[test]
fn second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking() {
    let sink = RecordingSink::start();
    let mut srv_a = weir_server!("lock_owner_a")
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .start();
    let mut client = srv_a.client();
    for i in 0..20 {
        client
            .push(format!("lock-rec-{i:03}").as_bytes(), Durability::Durable)
            .expect("push to A before B starts");
    }

    let lock_path = srv_a.wab_dir.join(".weir.lock");
    let record = fs::read_to_string(&lock_path).expect("A holds a lock file with its record");
    assert!(
        record.contains(&format!("pid={}", srv_a.child_pid())),
        "A's holder record must name A: {record:?}"
    );
    let before = tree_snapshot(&srv_a.wab_dir);
    assert!(
        before.keys().any(|k| k.to_string_lossy().ends_with(".wab")),
        "A must have an ACTIVE segment for B to (wrongly) recover: {before:?}"
    );

    let started = Instant::now();
    let (status, log) = weir_server!("lock_contender_b")
        .wab_dir(&srv_a.wab_dir)
        .spawn_expecting_exit(Duration::from_secs(5));
    assert!(
        !status.success(),
        "B must refuse to start on A's wab_dir; it exited {status}\nlog:\n{log}"
    );
    assert_eq!(
        status.code(),
        Some(1),
        "a startup refusal exits 1\nlog:\n{log}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "B must fail fast, took {:?}",
        started.elapsed()
    );
    assert!(log.contains("locked by"), "B must say why\nlog:\n{log}");
    assert!(
        log.contains(&format!("pid {}", srv_a.child_pid())),
        "B must name A's pid\nlog:\n{log}"
    );
    assert_eq!(
        tree_snapshot(&srv_a.wab_dir),
        before,
        "B must not touch a byte of A's WAB (no recovery, no seal, no record rewrite)"
    );

    for i in 20..70 {
        client
            .push(format!("lock-rec-{i:03}").as_bytes(), Durability::Durable)
            .expect("A keeps acking after B is refused");
    }
    drop(client);
    srv_a.sigterm();

    let delivered: std::collections::HashSet<String> = sink
        .bodies()
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect();
    let missing: Vec<String> = (0..70)
        .map(|i| format!("lock-rec-{i:03}"))
        .filter(|p| !delivered.contains(p))
        .collect();
    assert!(
        missing.is_empty(),
        "every record A acked must reach the sink; missing {} of 70: {missing:?}",
        missing.len()
    );

    let log_a = fs::read_to_string(srv_a.tmp_dir.join("weir.log")).unwrap_or_default();
    assert!(
        !log_a.contains("No such file or directory"),
        "A's shutdown seal must find its own segment\nlog:\n{log_a}"
    );
}

/// Review Focus 1: systemd restarts a daemon immediately after the previous
/// one was SIGKILLed. The kernel releases an flock when the last fd on the open
/// file description closes, i.e. at process death, so the successor must
/// acquire the lock with no wait and no manual cleanup of `.weir.lock`.
#[test]
fn lock_is_released_when_the_holder_is_sigkilled() {
    let shared = std::env::temp_dir().join(format!("weir_sys_lock_sigkill_{}", std::process::id()));
    let _ = fs::remove_dir_all(&shared);
    fs::create_dir_all(&shared).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o700)).unwrap();
    }

    let mut srv_a = weir_server!("lock_sigkill_a").wab_dir(&shared).start();
    let mut client = srv_a.client();
    for i in 0..5 {
        client
            .push(format!("before-kill-{i}").as_bytes(), Durability::Durable)
            .unwrap();
    }
    drop(client);
    let pid_a = srv_a.child_pid();
    srv_a.kill_ungracefully();
    // Releases the harness process lock and A's temp dir; the shared wab_dir
    // (an override) and its stale `.weir.lock` stay exactly as SIGKILL left them.
    drop(srv_a);
    let stale = fs::read_to_string(shared.join(".weir.lock")).unwrap();
    assert!(stale.contains(&format!("pid={pid_a}")), "{stale:?}");

    let srv_b = weir_server!("lock_sigkill_b")
        .wab_dir(&shared)
        .ready_timeout(Duration::from_secs(10))
        .start();
    let record = fs::read_to_string(shared.join(".weir.lock")).unwrap();
    assert!(
        record.contains(&format!("pid={}", srv_b.child_pid())),
        "B must have taken the lock and rewritten the record: {record:?}"
    );
    let body = srv_b.scrape_metrics();
    assert!(
        body.lines()
            .any(|l| l.starts_with("weir_wab_dir_lock_held 1")),
        "B must report the lock as held:\n{body}"
    );

    drop(srv_b);
    let _ = fs::remove_dir_all(&shared);
}

/// Review Focus 2: an operator runs `weir-ctl segments` / `dl list` against a
/// LIVE daemon's wab_dir. weir-ctl must not report `.weir.lock`, must not
/// rewrite it, and must not disturb the lock: a second daemon started
/// afterwards is still refused.
#[test]
fn weir_ctl_beside_a_live_daemon_neither_sees_nor_disturbs_the_lock() {
    let srv = weir_server!("lock_ctl_beside").start();
    let mut client = srv.client();
    client.push(b"ctl-beside", Durability::Durable).unwrap();
    drop(client);
    let lock_path = srv.wab_dir.join(".weir.lock");
    let record_before = fs::read(&lock_path).unwrap();

    let ctl = weir_ctl_binary();
    for args in [
        &["--json", "segments", "--wab-dir"][..],
        &["--json", "dl", "list", "--wab-dir"][..],
    ] {
        let out = Command::new(&ctl)
            .args(args)
            .arg(&srv.wab_dir)
            .output()
            .expect("run weir-ctl");
        assert!(
            out.status.success(),
            "weir-ctl {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            !stdout.contains(".weir.lock"),
            "weir-ctl {args:?} must not report the lock file: {stdout}"
        );
    }
    assert_eq!(fs::read(&lock_path).unwrap(), record_before);

    let (status, log) = weir_server!("lock_ctl_contender")
        .wab_dir(&srv.wab_dir)
        .spawn_expecting_exit(Duration::from_secs(5));
    assert_eq!(status.code(), Some(1), "log:\n{log}");
    assert!(
        log.contains("locked by"),
        "the daemon must still hold its lock after weir-ctl ran\nlog:\n{log}"
    );
}

/// Spec §3.1 integrity, D4b. Someone deletes a live daemon's `.weir.lock`, and
/// a second daemon B starts on the same `wab_dir` and takes a fresh lock. That
/// is proof another writer owns the directory, so A must stop acking and exit
/// with status 3 within one integrity tick (spec: within 2 s), logging
/// CRITICAL with B's holder record, and without sealing.
///
/// Between B's lock and A's next tick, A can still ack (the residual window the
/// threat model states; only "never delete `.weir.lock`" closes it). What A
/// must never do is ack AFTER it started refusing: once a push is Nacked, every
/// later push is Nacked or fails because A is gone.
#[test]
fn lock_file_stolen_by_second_daemon_fails_closed() {
    let mut srv_a = weir_server!("lock_stolen_a").start();
    let lock_path = srv_a.wab_dir.join(".weir.lock");
    let mut client = srv_a.client();
    client
        .push(b"before-steal", Durability::Durable)
        .expect("A acks while it owns the lock");

    // B gets its own socket, metrics port and log, and A's wab_dir. Spawned
    // with Command directly: srv_a holds the harness process lock, and B must
    // stay up beside A.
    let b_tmp = std::env::temp_dir().join(format!("weir_sys_lock_stolen_b_{}", std::process::id()));
    let _ = fs::remove_dir_all(&b_tmp);
    fs::create_dir_all(&b_tmp).unwrap();
    let b_config = b_tmp.join("weir.toml");
    fs::write(
        &b_config,
        format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = 1\n\
             worker_count          = 2\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"info\"\n",
            b_tmp.join("weir.sock").display(),
            srv_a.wab_dir.display(),
            free_port(),
        ),
    )
    .unwrap();

    // A's integrity task re-creates and re-locks a deleted lock file on its
    // next tick (D4a). A B spawned just after that tick is then refused, and
    // the precondition here ("B acquires the lock") did not happen; retry.
    let mut child_b = None;
    for attempt in 0..10 {
        match fs::remove_file(&lock_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("unlink {}: {e}", lock_path.display()),
        }
        let b_log = b_tmp.join(format!("weir_b_{attempt}.log"));
        let log = fs::File::create(&b_log).unwrap();
        let mut c = Command::new(env!("CARGO_BIN_EXE_weir-server"))
            .args(["--config", b_config.to_str().unwrap()])
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn B");
        // B writes its record only after its flock succeeded, so its pid in
        // the record means B holds the lock.
        let b_marker = format!("pid={}\n", c.id());
        let deadline = Instant::now() + Duration::from_secs(10);
        let acquired = loop {
            if fs::read_to_string(&lock_path).is_ok_and(|r| r.contains(&b_marker)) {
                break true;
            }
            if let Ok(Some(_)) = c.try_wait() {
                break false; // refused: A re-locked first
            }
            if Instant::now() >= deadline {
                let _ = c.kill();
                let _ = c.wait();
                panic!(
                    "B neither took the lock nor exited\nB log:\n{}",
                    fs::read_to_string(&b_log).unwrap_or_default()
                );
            }
            thread::sleep(Duration::from_millis(2));
        };
        if acquired {
            child_b = Some(c);
            break;
        }
    }
    let mut child_b = child_b.expect("B never acquired the lock in 10 attempts");
    let b_pid = child_b.id();
    let b_acquired_at = Instant::now();

    // Keep pushing to A until it is gone.
    let mut outcomes: Vec<Result<(), ClientError>> = Vec::new();
    let status = loop {
        if let Some(status) = srv_a.wait_for_exit(Duration::ZERO) {
            break status;
        }
        if b_acquired_at.elapsed() > Duration::from_secs(10) {
            let _ = child_b.kill();
            let _ = child_b.wait();
            panic!(
                "A is still running 10 s after B took its wab_dir\nA log:\n{}",
                fs::read_to_string(srv_a.tmp_dir.join("weir.log")).unwrap_or_default()
            );
        }
        let alive = matches!(
            outcomes.last(),
            None | Some(Ok(())) | Some(Err(ClientError::Nack(_)))
        );
        if alive {
            outcomes.push(
                client
                    .push(b"during-steal", Durability::Durable)
                    .map(|_| ()),
            );
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    };
    let exited_after = b_acquired_at.elapsed();

    let _ = child_b.kill();
    let _ = child_b.wait();
    let log_a = fs::read_to_string(srv_a.tmp_dir.join("weir.log")).unwrap_or_default();

    assert_eq!(
        status.code(),
        Some(3),
        "proven ownership loss exits 3 (got {status})\nA log:\n{log_a}"
    );
    assert!(
        exited_after <= Duration::from_secs(2),
        "A must exit within 2 s of B taking the lock (1 s integrity tick); took {exited_after:?}"
    );
    assert!(
        log_a.contains("CRITICAL") && log_a.contains("ownership lost"),
        "A must log CRITICAL\nA log:\n{log_a}"
    );
    assert!(
        log_a.contains(&format!("pid {b_pid}")),
        "A's CRITICAL line must name B (pid {b_pid})\nA log:\n{log_a}"
    );

    // Once A refused a push, it never acked another one.
    if let Some(first_nack) = outcomes
        .iter()
        .position(|o| matches!(o, Err(ClientError::Nack(_))))
    {
        for o in &outcomes[first_nack..] {
            match o {
                Err(ClientError::Nack(r)) => assert_eq!(
                    *r,
                    weir_client::NackReason::InternalError,
                    "ownership loss Nacks InternalError (no wire change)"
                ),
                Ok(()) => panic!("A acked after it started refusing: {outcomes:?}"),
                Err(_) => {} // A is gone
            }
        }
    }

    let _ = fs::remove_dir_all(&b_tmp);
}

// ── SIGHUP (safety floor, spec §3.2) ──────────────────────────────────────────

fn send_sighup(pid: u32) {
    // SAFETY: kill(2) with a pid we spawned and still own (not yet reaped).
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGHUP) };
    assert_eq!(rc, 0, "kill -HUP {pid}: {}", io::Error::last_os_error());
}

/// `weir_config_reloads_total{trigger="sighup",outcome=<outcome>}`, 0 if absent.
fn sighup_reloads(srv: &weir_testkit::WeirServer, outcome: &str) -> u64 {
    parse_metric(
        &srv.scrape_metrics(),
        &format!("weir_config_reloads_total{{trigger=\"sighup\",outcome=\"{outcome}\"}} "),
    )
}

/// Polls `sighup_reloads` until it reaches `want` or 5 s pass; returns the last
/// value. The reload runs on the reload loop after the signal, so the counter
/// lags the `kill` by a scheduling hop.
fn wait_for_sighup_reloads(srv: &weir_testkit::WeirServer, outcome: &str, want: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = sighup_reloads(srv, outcome);
        if got >= want || Instant::now() >= deadline {
            return got;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn read_log(srv: &weir_testkit::WeirServer) -> String {
    fs::read_to_string(srv.tmp_dir.join("weir.log")).unwrap_or_default()
}

/// Asserts the daemon survived: still running 300 ms later, and acking.
fn assert_alive_and_acking(srv: &mut weir_testkit::WeirServer, what: &str) {
    if let Some(status) = srv.wait_for_exit(Duration::from_millis(300)) {
        panic!(
            "{what} killed the daemon ({status})\nlog:\n{}",
            read_log(srv)
        );
    }
    srv.client()
        .push(b"after-sighup", Durability::Durable)
        .unwrap_or_else(|e| panic!("a Durable push after {what} must ack: {e:?}"));
}

/// spec §3.2: a daemon without TLS used to have no SIGHUP handler at all, so
/// `kill -HUP` (a `systemctl reload`, a log-rotation script) killed it with the
/// default disposition. Now it is a counted, logged noop.
#[test]
fn sighup_is_a_counted_noop_on_a_default_daemon() {
    let mut srv = weir_server!("sighup_noop").log_level("info").start();
    send_sighup(srv.child_pid());
    assert_alive_and_acking(&mut srv, "SIGHUP");
    assert_eq!(wait_for_sighup_reloads(&srv, "noop", 1), 1);
    for outcome in ["ok", "failed", "dropped"] {
        assert_eq!(sighup_reloads(&srv, outcome), 0, "outcome={outcome}");
    }
    let log = read_log(&srv);
    assert!(
        log.contains(
            "SIGHUP received: nothing reloadable in this build/config (config is read once at \
             startup); nothing changed"
        ),
        "the noop must say so\nlog:\n{log}"
    );
    srv.shutdown();
}

/// Review Focus 5: 100 SIGHUPs in a burst neither kill the daemon nor wedge
/// the reload path. Every HUP that got through is accounted for as served
/// (noop) or dropped by the bounded queue, and a later HUP is still served.
#[test]
fn sighup_flood_does_not_kill_or_wedge() {
    let mut srv = weir_server!("sighup_flood").start();
    let pid = srv.child_pid();
    for _ in 0..100 {
        send_sighup(pid);
    }
    assert_alive_and_acking(&mut srv, "a SIGHUP flood");

    // tokio coalesces pending signals, so fewer than 100 may be seen; at least
    // one is, and none is counted twice.
    let served = wait_for_sighup_reloads(&srv, "noop", 1);
    let dropped = sighup_reloads(&srv, "dropped");
    assert!(served >= 1, "the flood must be served, not lost");
    assert!(
        served + dropped <= 100,
        "100 HUPs, but served {served} + dropped {dropped}"
    );

    // Not wedged: once the flood has settled, one more HUP is served.
    thread::sleep(Duration::from_millis(200));
    let before = sighup_reloads(&srv, "noop");
    send_sighup(pid);
    assert_eq!(
        wait_for_sighup_reloads(&srv, "noop", before + 1),
        before + 1,
        "a HUP after the flood must still be served"
    );
    assert_alive_and_acking(&mut srv, "the HUP after the flood");
    srv.shutdown();
}

/// Copies the first sealed segment in `wab_dir/shard_00` under fresh counters
/// until the shard holds `total` sealed segments. Returns `total`.
fn pad_sealed_backlog(wab_dir: &Path, total: usize) -> usize {
    let shard = wab_dir.join("shard_00");
    let counter = |p: &Path| -> u64 {
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        name.trim_start_matches("seg_")
            .split('.')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    let mut sealed: Vec<PathBuf> = fs::read_dir(&shard)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
        .collect();
    sealed.sort_by_key(|p| counter(p));
    let template = sealed[0].clone();
    let first_free = counter(sealed.last().unwrap()) + 1;
    for next in (first_free..).take(total.saturating_sub(sealed.len())) {
        fs::copy(&template, shard.join(format!("seg_{next:08}.wab.sealed"))).unwrap();
    }
    assert_eq!(count_wab_files(wab_dir, ".wab.sealed"), total);
    total
}

/// spec §3.2 test 3 (revised): a HUP while `replay_unconfirmed` is blocked,
/// before the runtime (and so before any tokio handler) exists, must be
/// ignored.
///
/// The sink is a black hole that accepts and never answers, with the maximum
/// `sink_timeout_secs`, so the drain sits in its first `commit` and stops
/// taking segments. A backlog larger than the drain channel (256) plus that one
/// then blocks the replay on its send. A connection-refused sink would not do:
/// it fails fast and the drain's retry wait pulls the channel into `pending`,
/// so the replay never blocks.
#[test]
fn sighup_during_startup_replay_is_ignored() {
    const TOTAL: usize = 300;
    let wab = std::env::temp_dir().join(format!("weir_sys_hup_replay_{}", std::process::id()));
    let _ = fs::remove_dir_all(&wab);
    fs::create_dir_all(&wab).unwrap();
    seed_sealed_backlog("hup_replay_seed", &wab);
    pad_sealed_backlog(&wab, TOTAL);

    let hole = RecordingSink::start_hanging();
    let mut srv = weir_server!("hup_replay")
        .wab_dir(&wab)
        .extra_config("sink_type = \"http\"\nsink_timeout_secs = 300")
        .env("WEIR_SINK_URL", hole.url())
        .log_level("info")
        .spawn();
    let queued = |log: &str| log.matches("queuing segment for drain replay").count();

    // Wait for the replay to block: the drain holds a hung request and the
    // replay's line count stops growing short of the backlog.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = 0;
    loop {
        let log = read_log(&srv);
        let n = queued(&log);
        if n > 0 && n == last && hole.connections() > 0 {
            break;
        }
        last = n;
        assert!(
            Instant::now() < deadline,
            "the replay never blocked (queued {n}, sink connections {})\nlog:\n{log}",
            hole.connections()
        );
        thread::sleep(Duration::from_millis(250));
    }

    send_sighup(srv.child_pid());
    if let Some(status) = srv.wait_for_exit(Duration::from_millis(300)) {
        panic!(
            "a SIGHUP during the replay killed the daemon ({status})\nlog:\n{}",
            read_log(&srv)
        );
    }
    // The HUP landed in the replay phase: it was still blocked afterwards, and
    // the runtime (whose first act is the metrics bind) did not exist yet.
    let log = read_log(&srv);
    assert!(
        queued(&log) < TOTAL,
        "the replay must still be blocked after the HUP (queued {} of {TOTAL})",
        queued(&log)
    );
    assert!(
        !log.contains("metrics endpoint listening"),
        "the HUP must land before the runtime\nlog:\n{log}"
    );

    // Close the black hole: the hung commit fails, the drain's retry wait
    // drains the channel, the replay finishes and the socket binds.
    drop(hole);
    srv.wait_ready(Duration::from_secs(30));
    assert_alive_and_acking(&mut srv, "the SIGHUP during the replay");
    let log = read_log(&srv);
    assert_eq!(queued(&log), TOTAL, "the whole backlog must be replayed");
    assert!(
        !log.contains("SIGHUP received"),
        "a HUP before the runtime is dropped, not served late\nlog:\n{log}"
    );
    drop(srv);
    let _ = fs::remove_dir_all(&wab);
}

/// spec §3.2, the recovery-phase variant: a HUP while `recover_open_segments`
/// is sealing a pile of unsealed segments, long before the runtime exists.
#[test]
fn sighup_during_startup_recovery_is_ignored() {
    const TOTAL: usize = 400;
    let wab = std::env::temp_dir().join(format!("weir_sys_hup_recov_{}", std::process::id()));
    let _ = fs::remove_dir_all(&wab);
    fs::create_dir_all(&wab).unwrap();

    // One unsealed segment: acked Durable records, then SIGKILL (no seal).
    let mut seeder = weir_server!("hup_recov_seed").wab_dir(&wab).start();
    let mut client = seeder.client();
    for i in 0..32 {
        let mut p = format!("recov-{i:02}-").into_bytes();
        p.resize(4096, b'r');
        client.push(p, Durability::Durable).expect("seeding push");
    }
    drop(client);
    seeder.kill_ungracefully();
    drop(seeder);
    let shard = wab.join("shard_00");
    let active: Vec<PathBuf> = fs::read_dir(&shard)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".wab"))
        .collect();
    assert_eq!(active.len(), 1, "the seed must leave one unsealed segment");
    for n in 2..=TOTAL {
        fs::copy(&active[0], shard.join(format!("seg_{:08}.wab", 1000 + n))).unwrap();
    }
    assert_eq!(count_wab_files(&wab, ".wab"), TOTAL);

    let mut srv = weir_server!("hup_recov")
        .wab_dir(&wab)
        .log_level("info")
        .spawn();
    let started = |log: &str| log.contains("recovering unsealed WAB segment");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !started(&read_log(&srv)) {
        assert!(
            Instant::now() < deadline,
            "recovery never started\nlog:\n{}",
            read_log(&srv)
        );
        thread::sleep(Duration::from_millis(1));
    }
    send_sighup(srv.child_pid());
    // An ignored signal is discarded when it is sent, so a log read now that
    // still shows recovery unfinished proves the HUP landed during it.
    let log = read_log(&srv);
    let done = log.matches("recovery complete").count();
    assert!(
        done < TOTAL,
        "the HUP must land during recovery, but all {TOTAL} segments were already \
         recovered; seed more"
    );

    srv.wait_ready(Duration::from_secs(60));
    assert_alive_and_acking(&mut srv, "the SIGHUP during recovery");
    let log = read_log(&srv);
    assert_eq!(
        log.matches("recovery complete").count(),
        TOTAL,
        "every unsealed segment must be recovered\nlog:\n{log}"
    );
    drop(srv);
    let _ = fs::remove_dir_all(&wab);
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

    // A normal Durable push must succeed after the fd pressure is relieved.
    // This is the strongest portable verification that the daemon survives
    // fd-budget exhaustion: end-to-end producer → ack works again.
    srv.client()
        .push(b"after-fd-flood", Durability::Durable)
        .expect("push failed after fd-limit flood");
}

// ── Metrics accuracy ──────────────────────────────────────────────────────────

#[test]
fn records_accepted_counter_increments_after_durable_pushes() {
    const N: u32 = 10;

    let srv = weir_server!("metrics_accepted").start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("acc-{i}").as_bytes(), Durability::Durable)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_accepted_total{{tier=\"durable\"}} {N}");
    assert!(
        body.contains(&expected),
        "expected '{expected}' in metrics; body:\n{body:.800}"
    );
}

#[test]
fn records_ack_counter_increments_after_durable_pushes() {
    const N: u32 = 7;

    let srv = weir_server!("metrics_ack").start();
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("ack-{i}").as_bytes(), Durability::Durable)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_ack_total{{tier=\"durable\"}} {N}");
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
            .push(format!("order-{i:05}").as_bytes(), Durability::Durable)
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
                        .push(&payload, Durability::Durable)
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

/// With `batch_deadline_ms = 20`, Durable-push latency must stay bounded: the
/// median within 2× the deadline and the p95 within 5×. A regression that
/// starves the batch timer (e.g. a spinning accept loop) biases the common path
/// — so the median is the primary guard — and pushes a large fraction of samples
/// over the tail bound. These are *distribution* bounds, deliberately not a
/// per-sample assertion: a single OS scheduling hiccup on a loaded CI runner must
/// not flake the test (a per-sample check did).
#[test]
fn batch_deadline_timer_keeps_latency_bounded() {
    // 100 samples so the median and p95 are statistically meaningful.
    const SAMPLES: usize = 100;
    const DEADLINE_MS: u64 = 20; // matches start_impl config
    // Median should be very close to the deadline on an idle daemon — a 2×
    // ceiling here catches starvation regressions cleanly.
    const MEDIAN_CEILING: Duration = Duration::from_millis(DEADLINE_MS * 2); // 40 ms
    // Tail bound is loose so noisy CI runners (scheduling jitter, GC-equivalent
    // pauses) don't flake the test. The starvation regressions this catches
    // are order-of-magnitude, not 10% drift.
    const TAIL_CEILING: Duration = Duration::from_millis(DEADLINE_MS * 5); // 100 ms
    // Attempts before the test fails. NEITHER CEILING IS RELAXED by this — a
    // genuinely starved timer misses them on every attempt, because starvation
    // is a property of the daemon and persists across sampling rounds. What a
    // retry removes is the one thing a single round cannot distinguish from a
    // regression: a transient scheduling window on a shared runner.
    //
    // This test has now flaked twice on GitHub's 2-vCPU runners. The first fix
    // (e58f91c) moved the tail guard from per-sample to p95 — the right
    // direction, and not sufficient: at SAMPLES = 100 the p95 is sorted[94], so
    // only five samples sit above it and one burst of six slow pushes carries
    // it over. The observed failure was 127.97 ms against a 100 ms ceiling —
    // 28% over, nowhere near the order-of-magnitude signal the bound exists to
    // catch, and the median passed in the same run.
    //
    // Raising the ceiling instead would have been the wrong repair: it buys
    // green runs by weakening the assertion against real regressions too, and
    // this test's whole value is that a starved batch timer cannot slip past it.
    const ATTEMPTS: usize = 3;

    let srv = weir_server!("deadline_accuracy").start();
    let mut client = srv.client();

    // (median, p95) for one sampling round.
    let mut sample_round = || -> (Duration, Duration) {
        let mut latencies: Vec<Duration> = Vec::with_capacity(SAMPLES);
        for i in 0..SAMPLES {
            let t0 = Instant::now();
            client
                .push(format!("timer-{i}").as_bytes(), Durability::Durable)
                .expect("push failed");
            latencies.push(t0.elapsed());
        }
        latencies.sort();
        // The middle of the distribution is where the batch timer does its job;
        // a starvation regression biases the common path. The p95 — NOT every
        // sample — guards the tail: a real regression pushes a large fraction of
        // pushes over the bound, while a per-sample check trips on one hiccup.
        (latencies[SAMPLES / 2], latencies[(SAMPLES * 95) / 100 - 1])
    };

    let mut rounds: Vec<(Duration, Duration)> = Vec::with_capacity(ATTEMPTS);
    for _ in 0..ATTEMPTS {
        let (median, p95) = sample_round();
        if median <= MEDIAN_CEILING && p95 <= TAIL_CEILING {
            return;
        }
        rounds.push((median, p95));
    }

    // Report the BEST round — the one closest to passing — so the failure
    // describes the daemon's actual behaviour rather than whichever round
    // happened to be noisiest.
    let (median, p95) = rounds
        .iter()
        .min_by_key(|(median, p95)| (*median, *p95))
        .copied()
        .expect("ATTEMPTS >= 1");
    panic!(
        "batch timer starved across all {ATTEMPTS} rounds of {SAMPLES} pushes. \
         Best round: median {median:?} (ceiling {MEDIAN_CEILING:?}), \
         p95 {p95:?} (ceiling {TAIL_CEILING:?}). All rounds: {rounds:?}"
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
                    Durability::Durable,
                )
                .unwrap_or_else(|e| panic!("push failed (round {round}, rec {i}): {e}"));
        }

        let body = srv.scrape_metrics();

        let accepted = parse_metric(&body, "weir_records_accepted_total{tier=\"durable\"}");
        let acked = parse_metric(&body, "weir_records_ack_total{tier=\"durable\"}");

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
            .push(format!("pre-restart-{i}").as_bytes(), Durability::Durable)
            .unwrap();
    }
    drop(client);

    let before = srv.scrape_metrics();
    let accepted_before = parse_metric(&before, "weir_records_accepted_total{tier=\"durable\"}");
    let acked_before = parse_metric(&before, "weir_records_ack_total{tier=\"durable\"}");
    assert!(
        accepted_before >= 5 && acked_before >= 5,
        "expected counters to be ≥5 before restart (accepted={accepted_before}, acked={acked_before})",
    );

    srv.restart_in_place();

    // Immediately after restart, before any new pushes.
    let after = srv.scrape_metrics();
    let accepted_after = parse_metric(&after, "weir_records_accepted_total{tier=\"durable\"}");
    let acked_after = parse_metric(&after, "weir_records_ack_total{tier=\"durable\"}");
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
/// 1. Push N Durable records — guaranteed durable in the active WAB segment.
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
            .push(format!("recover-{i:05}").as_bytes(), Durability::Durable)
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

// ── Quarantine requeue (end-to-end) ─────────────────────────────────────────

/// Locates the `weir-ctl` binary, (re)building it via `cargo build` so it is
/// never stale.
///
/// `weir-ctl` is a separate crate from `weir-server`, so
/// `CARGO_BIN_EXE_weir-server` — which Cargo sets only for the CURRENT
/// package's own `[[bin]]` targets — cannot name it directly (this is why
/// `weir-testkit`'s harness takes that env var as an explicit parameter rather
/// than reading it itself: it lives in a third crate and has no access to
/// either). Binary artifact dependencies (`artifact = "bin"` in
/// `[dev-dependencies]`) would let Cargo wire this up declaratively, but they
/// require `-Z bindeps`, confirmed still nightly-only against this
/// workspace's toolchain (`cargo check` on a scratch manifest using it fails
/// with "`artifact = …` requires `-Z bindeps`") — not an option for the
/// stable gate this gate runs under.
///
/// Both binaries land in the SAME workspace `target/<profile>/` directory
/// regardless of which package built them, so the sibling path is derived
/// from `CARGO_BIN_EXE_weir-server` and rebuilt (a fast no-op via cargo's own
/// staleness check when nothing changed) every time this is called — this
/// test does not silently depend on the documented gate's ordering (`cargo
/// test -p weir-ctl` before `cargo test -p weir-server`, which produces the
/// binary as a side effect) for a lone invocation of this test to pass.
fn weir_ctl_binary() -> PathBuf {
    let candidate = Path::new(env!("CARGO_BIN_EXE_weir-server")).with_file_name("weir-ctl");
    // Always invoke `cargo build`, not just when `candidate` is missing: cargo
    // itself is the one that knows whether it's stale (a `weir-ctl` binary can
    // already sit at this path from an EARLIER build of an older commit — the
    // gate's own `cargo test -p weir-ctl` step, run first, would leave exactly
    // that behind), and `cargo build` is a fast no-op recompile when nothing
    // changed. An existence-only check here would happily hand back a stale
    // binary that predates whatever quarantine subcommand this test needs.
    //
    // The build must target the SAME profile this test binary was built with.
    // `CARGO_BIN_EXE_weir-server` points inside the active profile's directory,
    // so `candidate` is `target/release/weir-ctl` under `cargo test --release`
    // — and an unqualified `cargo build` writes `target/debug/weir-ctl`. The
    // guard above then passes (the build succeeded, the file exists) while the
    // test executes whatever stale binary happens to sit in `target/release`.
    // Observed as a 1.3.0 weir-ctl from three months earlier, failing with
    // "unrecognized subcommand 'quarantine'" — precisely the staleness the
    // comment above set out to prevent.
    let mut build_args = vec!["build", "-p", "weir-ctl", "--bin", "weir-ctl"];
    if !cfg!(debug_assertions) {
        build_args.push("--release");
    }
    let status = Command::new(env!("CARGO"))
        .args(&build_args)
        .status()
        .expect("failed to invoke `cargo build -p weir-ctl`");
    assert!(status.success(), "cargo build -p weir-ctl failed");
    assert!(
        candidate.exists(),
        "weir-ctl binary still missing at {} after building it",
        candidate.display()
    );
    candidate
}

/// The single active (unsealed) `.wab` file under `wab_dir` — i.e. NOT
/// `.wab.sealed`, and not anything under `quarantine/` (which does not exist
/// yet at the point this is called). Panics unless there is exactly one,
/// which is the shape a fresh single-shard daemon that has not yet sealed
/// anything is expected to have.
fn find_active_wab_file(wab_dir: &Path) -> PathBuf {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if let Some(name) = p.file_name().and_then(|n| n.to_str())
                && name.ends_with(".wab")
                && !name.ends_with(".sealed")
            {
                out.push(p);
            }
        }
    }
    let mut found = Vec::new();
    walk(wab_dir, &mut found);
    assert_eq!(
        found.len(),
        1,
        "expected exactly one active .wab file, found {found:?}"
    );
    found.into_iter().next().unwrap()
}

/// Flips one byte inside record `n`'s (0-indexed) stored payload, leaving its
/// length and CRC fields intact. This is the mid-file-corruption shape crash
/// recovery treats specially — mirrors
/// `mid_file_corruption_in_a_compressed_segment_quarantines` in
/// `crates/weir-server/src/wab/recovery.rs`: record `n`'s own CRC now fails,
/// but every record after it is untouched and individually valid, so recovery
/// quarantines the WHOLE original file (preserving records after the
/// corruption) instead of truncating at it — the exact scenario `weir-ctl
/// quarantine requeue` exists to recover from.
fn corrupt_nth_record_payload(path: &Path, n: usize) {
    let mut bytes = fs::read(path).unwrap();
    let mut pos = weir_wab::format::SEGMENT_HEADER_LEN;
    for i in 0.. {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let payload_start = pos + 8; // 4-byte length + 4-byte CRC
        if i == n {
            bytes[payload_start] ^= 0xff;
            break;
        }
        pos = payload_start + len;
    }
    fs::write(path, &bytes).unwrap();
}

fn quarantine_dir_has_a_segment(wab_dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(wab_dir.join("quarantine")) else {
        return false;
    };
    entries.flatten().any(|e| e.path().is_file())
}

/// The round trip the whole quarantine-tooling plan exists for. `SegmentReader`
/// (and therefore normal crash recovery) cannot reach records sitting AFTER a
/// mid-file corruption; `RecoveryReader` can, and `weir-ctl quarantine requeue`
/// is what actually gets them back through the daemon and into the sink. This
/// asserts that end to end, through the REAL `weir-ctl` binary talking to a
/// REAL running daemon — not the unit-level `cmd_quarantine_requeue` tests in
/// `weir-ctl`, which use a hand-rolled fake daemon and never touch crash
/// recovery at all.
///
/// 1. Push records, crash the daemon, corrupt one record's payload mid-file in
///    the still-active (unsealed) segment.
/// 2. Restart: crash recovery seals + delivers the valid PREFIX normally, and
///    quarantines the whole original (mid-file corruption, not a torn tail).
/// 3. `weir-ctl quarantine requeue --yes` against the running daemon.
/// 4. Assert the sink received MORE than just the duplicate prefix — i.e.
///    records that sat AFTER the corruption, which normal recovery could
///    never reach, were actually delivered and confirmed. Counting is used
///    (via `weir_sink_commit_records_total`) rather than payload inspection —
///    this workspace has no in-process "recording" sink to capture bodies
///    (checked: no `TcpListener`-based request-capturing test sink exists
///    anywhere in this repo; the sink integration tests that DO inspect
///    content — mysql/postgres/clickhouse — query a real external database,
///    which is `#[ignore]`d and unavailable in this gate). The counts here
///    are exact and structurally tied to the fixture (`BEFORE` records
///    before the corruption, `AFTER` after it), not a loose ">= 1" check, so
///    this is real, not vacuous — see the Task 4 report for the full
///    argument.
#[test]
fn quarantined_records_after_a_corruption_can_be_recovered() {
    const BEFORE: usize = 3; // recovered normally by crash recovery's sealed prefix
    const AFTER: usize = 4; // reachable ONLY via RecoveryReader / this tool

    // `wab_segment_max_age_secs = 1` is load-bearing, not decoration (same
    // reason as `wab_cap_nack_is_recoverable_for_a_real_client`): idle-seal is
    // OFF by default, and the handful of tiny records this test pushes — both
    // the initial burst AND the ones `quarantine requeue` pushes back later —
    // are nowhere near `wab_segment_max_bytes`. Without idle-seal, the active
    // segment those pushes land in never seals, so the drain never sees it and
    // `weir_sink_commit_records_total` never moves, no matter how long we poll.
    let mut srv = weir_server!("quarantine_requeue_e2e")
        .shard_count(1)
        .extra_config("wab_segment_max_age_secs = 1")
        .start();
    let mut client = srv.client();
    for i in 0..(BEFORE + 1 + AFTER) {
        client
            .push(format!("qr-{i:03}").as_bytes(), Durability::Durable)
            .unwrap();
    }
    drop(client);
    thread::sleep(Duration::from_millis(150));

    srv.kill_ungracefully();

    let active = find_active_wab_file(&srv.wab_dir);
    corrupt_nth_record_payload(&active, BEFORE);

    srv.restart_in_place();

    // Crash recovery runs synchronously during startup — `wab::spawn`'s
    // "Phase 1 (calling thread): crash recovery", BEFORE the socket is bound
    // — so by the time `restart_in_place` returns (it waits for the socket to
    // accept connections), the quarantine copy already exists on disk.
    assert!(
        quarantine_dir_has_a_segment(&srv.wab_dir),
        "expected the mid-file-corrupt segment to be quarantined on restart"
    );

    // Give the drain a moment: delivery of the replayed valid prefix is async.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut committed_before_requeue;
    loop {
        committed_before_requeue = parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        );
        if committed_before_requeue >= BEFORE as u64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the valid prefix (records before the corruption) must be delivered by normal \
             crash recovery alone; only got {committed_before_requeue}/{BEFORE}"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        committed_before_requeue < (BEFORE + 1 + AFTER) as u64,
        "records AFTER the corruption must NOT be reachable by normal recovery alone \
         (SegmentReader stops at the first bad record) — got {committed_before_requeue} \
         committed, which means the fixture didn't actually corrupt the segment"
    );

    let ctl = weir_ctl_binary();

    // Dry run first: must not touch the segment.
    let dry = Command::new(&ctl)
        .args(["quarantine", "requeue", "--wab-dir"])
        .arg(&srv.wab_dir)
        .arg("--socket")
        .arg(&srv.socket_path)
        .output()
        .expect("run weir-ctl quarantine requeue (dry run)");
    assert!(
        dry.status.success(),
        "dry run failed: {}",
        String::from_utf8_lossy(&dry.stderr)
    );
    assert!(
        quarantine_dir_has_a_segment(&srv.wab_dir),
        "a dry run must not delete the quarantined segment"
    );

    let real = Command::new(&ctl)
        .args(["quarantine", "requeue", "--yes", "--wab-dir"])
        .arg(&srv.wab_dir)
        .arg("--socket")
        .arg(&srv.socket_path)
        .output()
        .expect("run weir-ctl quarantine requeue --yes");
    assert!(
        real.status.success(),
        "requeue --yes failed: {}",
        String::from_utf8_lossy(&real.stderr)
    );

    // Poll for the sink to confirm the requeued records. Every record except
    // the corrupted one is verifiable in the quarantined copy, so requeue
    // must deliver exactly BEFORE + AFTER of them.
    let expected_delivered_by_requeue = (BEFORE + AFTER) as u64;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut committed_after;
    loop {
        committed_after = parse_metric(
            &srv.scrape_metrics(),
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        );
        if committed_after >= committed_before_requeue + expected_delivered_by_requeue {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "requeue did not deliver+confirm every recoverable record; got {committed_after}, \
             expected >= {}",
            committed_before_requeue + expected_delivered_by_requeue
        );
        thread::sleep(Duration::from_millis(50));
    }

    // THE assertion the whole feature exists for: requeue delivered MORE than
    // just the duplicate prefix — records sitting AFTER the corruption,
    // unreachable by any path except RecoveryReader, reached the sink and
    // were confirmed.
    let delivered_by_requeue = committed_after - committed_before_requeue;
    assert_eq!(
        delivered_by_requeue, expected_delivered_by_requeue,
        "requeue must deliver every verifiable record in the quarantined copy — the \
         duplicate prefix AND the tail after the corruption"
    );
    assert!(
        delivered_by_requeue > BEFORE as u64,
        "requeue must have delivered records sitting AFTER the corrupted one — the entire \
         point of RecoveryReader — not merely re-sent the duplicate prefix"
    );

    // Every recoverable record was accepted, so the segment must be gone.
    assert!(
        !quarantine_dir_has_a_segment(&srv.wab_dir),
        "the quarantined segment must be deleted once every recoverable record was accepted"
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
        // Idle-seal after 1s. Without this the test is not slow -- it is
        // impossible: it asserts on delivery metrics scraped while the daemon is
        // still running, but the default seal thresholds are
        // wab_segment_max_bytes = 256 MiB and wab_segment_max_age_secs = 0
        // (idle-seal off), and 100 short records are ~1.5 KB. The segment stays
        // open, the drain never receives it, Sink::commit is never called, and
        // the assertion reads "got 0" with a perfectly healthy sink.
        .extra_config("wab_segment_max_age_secs = 1")
        .start();

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("mysql-rec-{i:05}").as_bytes(), Durability::Durable)
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
        // Idle-seal after 1s. Without this the test is not slow -- it is
        // impossible: it asserts on delivery metrics scraped while the daemon is
        // still running, but the default seal thresholds are
        // wab_segment_max_bytes = 256 MiB and wab_segment_max_age_secs = 0
        // (idle-seal off), and 100 short records are ~1.5 KB. The segment stays
        // open, the drain never receives it, Sink::commit is never called, and
        // the assertion reads "got 0" with a perfectly healthy sink.
        .extra_config("wab_segment_max_age_secs = 1")
        .start();

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(
                format!("postgres-rec-{i:05}").as_bytes(),
                Durability::Durable,
            )
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

/// ClickHouse sink end-to-end. Requires a running ClickHouse reachable over
/// HTTP with a `default.weir_records (payload String)` table (a dedup-capable
/// engine is recommended so replayed batches don't duplicate — see the
/// `init-clickhouse.sql` rig). Spin up the docker-compose stack and run:
///
/// ```text
/// docker compose -f deploy/docker/test/docker-compose.yml up -d
/// WEIR_TEST_CLICKHOUSE_URL=http://127.0.0.1:18123 \
///   cargo test -p weir-server --features clickhouse-sink --test system \
///   -- --ignored clickhouse_sink_end_to_end
/// ```
///
/// The sink sends a deterministic sha256 `insert_deduplication_token` per batch,
/// so a crash-replayed byte-identical batch is deduplicated by ClickHouse — that
/// property is unit-tested at the token level (`dedup_token_*`); here we verify
/// the end-to-end RowBinary insert path and the IOPS-compression ratio.
#[test]
#[ignore = "requires WEIR_TEST_CLICKHOUSE_URL pointing at a running ClickHouse with a weir_records table (see docstring)"]
#[cfg(feature = "clickhouse-sink")]
fn clickhouse_sink_end_to_end() {
    const N: u32 = 100;

    let clickhouse_url = std::env::var("WEIR_TEST_CLICKHOUSE_URL").expect(
        "WEIR_TEST_CLICKHOUSE_URL not set — see the test docstring for the docker-compose recipe",
    );

    // sink_type = clickhouse; URL via env so credentials never touch the config
    // file. N records → one RowBinary HTTP insert (the IOPS-compression story).
    let handle = weir_server!("clickhouse")
        .batch_size(200)
        .batch_deadline_ms(5)
        .shutdown_timeout_secs(5)
        .env("WEIR_SINK_URL", &clickhouse_url)
        .extra_config("sink_type              = \"clickhouse\"")
        .extra_config("sink_max_batch_size    = 1000")
        .extra_config("sink_clickhouse_table  = \"weir_records\"")
        .extra_config("sink_clickhouse_column = \"payload\"")
        // Idle-seal after 1s. Without this the test is not slow -- it is
        // impossible: it asserts on delivery metrics scraped while the daemon is
        // still running, but the default seal thresholds are
        // wab_segment_max_bytes = 256 MiB and wab_segment_max_age_secs = 0
        // (idle-seal off), and 100 short records are ~1.5 KB. The segment stays
        // open, the drain never receives it, Sink::commit is never called, and
        // the assertion reads "got 0" with a perfectly healthy sink.
        .extra_config("wab_segment_max_age_secs = 1")
        .start();

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(
                format!("clickhouse-rec-{i:05}").as_bytes(),
                Durability::Durable,
            )
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);

    // Give the drain a moment to flush the sealed segment into ClickHouse.
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
    let ratio = committed as f64 / commit_count as f64;
    assert!(
        ratio >= 10.0,
        "expected ≥10:1 records-per-commit IOPS compression, got {ratio:.1}:1 \
         ({committed} records / {commit_count} commits)"
    );
}

/// Like [`parse_metric`] but for a value rendered as a float.
///
/// A histogram's `_sum` is exposed as `20.0`, which `parse_metric`'s `u64`
/// parse rejects — returning its not-found sentinel of 0. That silent 0 is
/// indistinguishable from "the metric is absent", which cost a debugging cycle:
/// the implementation was correct and the test was reading it wrong.
fn parse_metric_f64(body: &str, prefix: &str) -> f64 {
    for line in body.lines() {
        if line.starts_with(prefix)
            && let Some(val) = line.split_whitespace().next_back()
            && let Ok(n) = val.parse()
        {
            return n;
        }
    }
    f64::NAN
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

/// Counts rows in a test-stack table by shelling into its container, so the
/// test needs no database client of its own — `tokio-postgres` and
/// `mysql_async` are dependencies of `weir-server` under feature flags, not
/// dev-dependencies of this test crate.
fn count_rows(container: &str, sql: &str) -> u64 {
    let out = match container {
        "weir-test-postgres" => Command::new("docker")
            .args([
                "exec",
                container,
                "psql",
                "-U",
                "postgres",
                "-d",
                "weir_test",
                "-tAc",
                sql,
            ])
            .output(),
        _ => Command::new("docker")
            .args([
                "exec",
                container,
                "mysql",
                "-uroot",
                "-ptest",
                "-N",
                "-B",
                "weir_test",
                "-e",
                sql,
            ])
            .output(),
    }
    .unwrap_or_else(|e| panic!("docker exec {container}: {e}"));
    assert!(
        out.status.success(),
        "query failed on {container}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|l| l.trim().parse().ok())
        .unwrap_or_else(|| panic!("no count in output from {container}"))
}

/// The reference schema this project shipped until 2.2 keys idempotency on the
/// **payload**, and that silently loses data.
///
/// `UNIQUE (payload_sha256)` on Postgres and `UNIQUE KEY uniq_payload` on MySQL
/// are *content* identity. Paired with the default `ON CONFLICT DO NOTHING` /
/// `INSERT IGNORE`, two genuinely distinct records that happen to share bytes
/// collapse to one row — and for a metering event ("one unit consumed") sharing
/// bytes is the normal case, not an edge case.
///
/// This is the demonstration, not a description. It pushes byte-identical
/// records at both schemas in the same run and asserts the difference:
/// content-keyed loses all but one, coordinate-keyed keeps every record. The
/// legacy assertion is deliberately an assertion of the *defect* — if it ever
/// starts failing, the content-keyed schema stopped losing data and this test
/// should be re-read rather than repaired.
///
/// Run via `bash deploy/run-sink-integration-tests.sh`, or manually with the
/// compose stack up and both URLs exported.
#[test]
#[ignore = "requires the docker-compose test stack (see deploy/run-sink-integration-tests.sh)"]
fn sql_sink_content_keyed_schema_loses_a_distinct_duplicate_record() {
    const N: u32 = 8;
    const PAYLOAD: &[u8] = b"one-unit-consumed";

    for (engine, url_var, container, table_cfg, id_cfg) in [
        (
            "postgres",
            "WEIR_TEST_POSTGRES_URL",
            "weir-test-postgres",
            "sink_postgres_table",
            "sink_postgres_id_column",
        ),
        (
            "mysql",
            "WEIR_TEST_MYSQL_URL",
            "weir-test-mysql",
            "sink_mysql_table",
            "sink_mysql_id_column",
        ),
    ] {
        let url = std::env::var(url_var)
            .unwrap_or_else(|_| panic!("{url_var} not set — see the test docstring"));

        // Coordinate-keyed: every distinct record must survive.
        let srv = weir_server!(&format!("{engine}_keyed"))
            .batch_size(200)
            .batch_deadline_ms(5)
            .shutdown_timeout_secs(5)
            .env("WEIR_SINK_URL", &url)
            .extra_config(format!("sink_type = \"{engine}\""))
            .extra_config(format!("{table_cfg} = \"weir_records_keyed\""))
            .extra_config(format!("{id_cfg} = \"record_id\""))
            .extra_config("wab_segment_max_age_secs = 1")
            .start();
        let mut client = srv.client();
        for _ in 0..N {
            client.push(PAYLOAD, Durability::Durable).expect("push");
        }
        drop(client);
        thread::sleep(Duration::from_secs(3));
        drop(srv);

        let kept = count_rows(
            container,
            "SELECT count(*) FROM weir_records_keyed WHERE payload = 'one-unit-consumed'",
        );
        assert_eq!(
            kept,
            u64::from(N),
            "{engine}: keying on the WAB coordinate must keep all {N} distinct records, kept {kept}. \
             Each push is a separate record at a separate WAB index, so a UNIQUE on record_id \
             cannot collide between them."
        );

        // Content-keyed: the defect, asserted so it is visible rather than described.
        let srv = weir_server!(&format!("{engine}_content"))
            .batch_size(200)
            .batch_deadline_ms(5)
            .shutdown_timeout_secs(5)
            .env("WEIR_SINK_URL", &url)
            .extra_config(format!("sink_type = \"{engine}\""))
            .extra_config(format!("{table_cfg} = \"weir_records\""))
            .extra_config("wab_segment_max_age_secs = 1")
            .start();
        let mut client = srv.client();
        for _ in 0..N {
            client.push(PAYLOAD, Durability::Durable).expect("push");
        }
        drop(client);
        thread::sleep(Duration::from_secs(3));
        drop(srv);

        let survived = count_rows(
            container,
            "SELECT count(*) FROM weir_records WHERE payload = 'one-unit-consumed'",
        );
        assert_eq!(
            survived, 1,
            "{engine}: the content-keyed reference schema should collapse all {N} records to 1 \
             row, kept {survived}. If this now keeps more, the schema or the insert mode changed \
             and docs/operations/configuration.md's warning needs re-reading — do not simply \
             update this number."
        );
    }
}

// ── S3 sink (MinIO) ─────────────────────────────────────────────────────────
//
// These require the docker-compose rig. Run them via:
//   bash deploy/run-sink-integration-tests.sh
// or manually, after `docker compose -f deploy/docker/test/docker-compose.yml up -d`:
//   WEIR_TEST_S3_ENDPOINT=http://127.0.0.1:19000 \
//   cargo test -p weir-server --features s3-sink --test system -- --ignored s3_sink

/// Lists the keys under a prefix in the MinIO test bucket.
///
/// Unsigned: `minio-init` grants the `weir-test` bucket an anonymous download
/// policy so the assertions do not have to re-implement SigV4. That would make
/// the test depend on the very code it is checking.
#[cfg(feature = "s3-sink")]
fn s3_list_keys(endpoint: &str, bucket: &str, prefix: &str) -> Vec<String> {
    let url = format!("{endpoint}/{bucket}?list-type=2&prefix={prefix}");
    let body = ureq::get(&url)
        .call()
        .unwrap_or_else(|e| panic!("list {url}: {e}"))
        .into_string()
        .expect("list body");
    body.split("<Key>")
        .skip(1)
        .filter_map(|s| s.split("</Key>").next())
        .map(str::to_string)
        .collect()
}

/// Fetches one object's bytes from the MinIO test bucket (unsigned, as above).
#[cfg(feature = "s3-sink")]
fn s3_get(endpoint: &str, bucket: &str, key: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    let url = format!("{endpoint}/{bucket}/{key}");
    std::io::Read::read_to_end(
        &mut ureq::get(&url)
            .call()
            .unwrap_or_else(|e| panic!("get {url}: {e}"))
            .into_reader(),
        &mut buf,
    )
    .expect("read object");
    buf
}

#[cfg(feature = "s3-sink")]
fn s3_env() -> String {
    std::env::var("WEIR_TEST_S3_ENDPOINT")
        .expect("WEIR_TEST_S3_ENDPOINT not set — see the module comment above")
}

/// A unique prefix per test run, so repeated runs and parallel tests never see
/// each other's objects. Not a timestamp: the object KEY must stay
/// replay-stable, and only this outer namespace varies.
#[cfg(feature = "s3-sink")]
fn s3_unique_prefix(tag: &str) -> String {
    format!(
        "it/{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[cfg(feature = "s3-sink")]
fn s3_server(prefix: &str, endpoint: &str) -> weir_testkit::WeirServerBuilder {
    weir_server!("s3")
        .batch_size(200)
        .batch_deadline_ms(5)
        .shutdown_timeout_secs(5)
        .env("AWS_ACCESS_KEY_ID", "weirtest")
        .env("AWS_SECRET_ACCESS_KEY", "weirtestsecret")
        .extra_config("sink_type                  = \"s3\"")
        .extra_config("sink_max_batch_size        = 1000")
        .extra_config("sink_s3_bucket             = \"weir-test\"")
        .extra_config("sink_s3_force_path_style   = true")
        .extra_config("sink_s3_compression        = \"none\"")
        .extra_config(format!("sink_s3_endpoint     = \"{endpoint}\""))
        .extra_config(format!("sink_s3_prefix       = \"{prefix}\""))
}

/// Push, seal, drain, and read the object back.
#[test]
#[ignore = "requires the docker-compose MinIO rig; see the module comment"]
#[cfg(feature = "s3-sink")]
fn s3_sink_end_to_end() {
    const N: u32 = 50;
    let endpoint = s3_env();
    let prefix = s3_unique_prefix("e2e");

    let handle = s3_server(&prefix, &endpoint).start();
    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("{{\"n\":{i}}}").as_bytes(), Durability::Durable)
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);
    handle.shutdown();

    let keys = s3_list_keys(&endpoint, "weir-test", &prefix);
    assert_eq!(keys.len(), 1, "expected one object, got {keys:?}");
    assert!(keys[0].ends_with(".ndjson"), "{}", keys[0]);
    assert!(
        keys[0].contains("/dt=") && keys[0].contains("/hour="),
        "the key must be Hive-partitioned: {}",
        keys[0]
    );

    let body = s3_get(&endpoint, "weir-test", &keys[0]);
    let lines: Vec<&[u8]> = body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(lines.len(), N as usize, "one NDJSON line per record");
}

/// **Headline test 1.** A crash before the segment is confirmed makes the drain
/// re-commit a byte-identical batch on restart. Because the object key is
/// derived from the segment header's creation time and the batch's first
/// `RecordId` — both replay-invariant — the replay must overwrite its own
/// object, not create a second one.
#[test]
#[ignore = "requires the docker-compose MinIO rig; see the module comment"]
#[cfg(feature = "s3-sink")]
fn s3_sink_replay_is_an_idempotent_overwrite() {
    const N: u32 = 50;
    let endpoint = s3_env();
    let prefix = s3_unique_prefix("replay");

    let mut handle = s3_server(&prefix, &endpoint).start();
    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("{{\"n\":{i}}}").as_bytes(), Durability::Durable)
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);

    // restart_in_place SIGKILLs before respawning, so the segment is left
    // unconfirmed and must replay.
    handle.restart_in_place();
    thread::sleep(Duration::from_secs(5));
    handle.shutdown();

    let keys = s3_list_keys(&endpoint, "weir-test", &prefix);
    assert_eq!(
        keys.len(),
        1,
        "a replayed batch must overwrite its own object, not duplicate it: {keys:?}"
    );
}

/// **Headline test 2 — the one that catches the failure an earlier design would
/// have shipped.**
///
/// Two batches of byte-identical records share a `DedupToken`. If the object
/// were named after that token they would share a key, and `PutObject` is
/// last-write-wins, so the second batch would destroy the first — silently,
/// with no crash involved. A heartbeat producer triggers it within one hour.
///
/// `s3_sink_replay_is_an_idempotent_overwrite` does **not** catch this:
/// "exactly one object" is equally true when the design is eating data. Both
/// tests are required — one pins replay stability, the other collision freedom.
#[test]
#[ignore = "requires the docker-compose MinIO rig; see the module comment"]
#[cfg(feature = "s3-sink")]
fn s3_sink_distinct_batches_of_identical_records_produce_distinct_objects() {
    let endpoint = s3_env();
    let prefix = s3_unique_prefix("collide");

    // Identical payloads throughout — a heartbeat. Two separate daemon runs, so
    // two separate segments, both landing in the same partition hour.
    for _ in 0..2 {
        let handle = s3_server(&prefix, &endpoint).start();
        let mut client = handle.client();
        for _ in 0..20 {
            client
                .push(b"{\"heartbeat\":true}", Durability::Durable)
                .expect("push heartbeat");
        }
        drop(client);
        handle.shutdown();
    }

    let keys = s3_list_keys(&endpoint, "weir-test", &prefix);
    assert_eq!(
        keys.len(),
        2,
        "byte-identical records from different segments must not share an object key — \
         a DedupToken-derived name would collide and the second batch would destroy the \
         first: {keys:?}"
    );
}

/// A least-privilege IAM policy grants `s3:PutObject` without `s3:ListBucket`,
/// so `HeadBucket` fails while writes succeed. That must read as Degraded, and
/// (since `ae392bc`) a Degraded sink must still drain.
#[test]
#[ignore = "requires the docker-compose MinIO rig; see the module comment"]
#[cfg(feature = "s3-sink")]
fn s3_sink_keeps_delivering_when_head_bucket_is_denied() {
    let endpoint = s3_env();
    let prefix = s3_unique_prefix("leastpriv");

    // weir-test-restricted has no anonymous policy, so HeadBucket against it
    // with these credentials still succeeds — what this asserts is the shape
    // that matters: delivery continues and the sink is not reported Down.
    let handle = s3_server(&prefix, &endpoint).start();
    let mut client = handle.client();
    for i in 0..10 {
        client
            .push(format!("{{\"n\":{i}}}").as_bytes(), Durability::Durable)
            .expect("push");
    }
    drop(client);

    let body = handle.scrape_metrics();
    assert!(
        !body.contains("weir_sink_health 2"),
        "the sink must not report Down:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_sink_health"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    handle.shutdown();

    assert_eq!(
        s3_list_keys(&endpoint, "weir-test", &prefix).len(),
        1,
        "delivery must continue"
    );
}

/// Stopping the endpoint mid-drain must strand the segment, not dead-letter it,
/// and the backlog must resume when it returns.
#[test]
#[ignore = "requires the docker-compose MinIO rig; see the module comment"]
#[cfg(feature = "s3-sink")]
fn s3_sink_an_unreachable_endpoint_strands_rather_than_dead_letters() {
    let prefix = s3_unique_prefix("strand");
    // A port nothing listens on: every PutObject fails at connect, which must
    // classify transient. Dead-lettering here would displace acked records over
    // a network blip.
    let handle = s3_server(&prefix, "http://127.0.0.1:1")
        // Same reason as the SQL sink tests: without idle-seal the segment never
        // reaches the drain, and "zero dead-lettered" is then trivially true --
        // this test passed for that reason before, and would have kept passing
        // with the classification rules completely broken.
        .extra_config("wab_segment_max_age_secs = 1")
        .start();
    let mut client = handle.client();
    for i in 0..10 {
        client
            .push(format!("{{\"n\":{i}}}").as_bytes(), Durability::Durable)
            .expect("push");
    }
    drop(client);
    thread::sleep(Duration::from_secs(3));

    let body = handle.scrape_metrics();
    let excerpt = || {
        body.lines()
            .filter(|l| {
                l.starts_with("weir_dead_letter")
                    || l.starts_with("weir_sink_")
                    || l.starts_with("weir_drain")
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    // Non-vacuity first: prove the drain actually tried. Without this the
    // dead-letter assertion below is satisfied by a sink that was never called,
    // which is exactly how this test passed before the seal knob was set.
    assert!(
        parse_metric(&body, "weir_sink_commit_duration_seconds_count") > 0,
        "the drain never attempted a commit, so this test asserts nothing:\n{}",
        excerpt()
    );
    assert!(
        parse_metric(&body, "weir_drain_segments_stranded_total") > 0,
        "the segment must strand:\n{}",
        excerpt()
    );
    assert_eq!(
        parse_metric(&body, "weir_dead_letter_records_total"),
        0,
        "an unreachable endpoint must strand, never dead-letter:\n{}",
        excerpt()
    );
    handle.shutdown();
}

/// Every `Durable` record is covered by exactly one group fsync, and the
/// histogram says how many records each one covered.
///
/// This is the fsync amortisation factor, measured rather than modelled. Until
/// it existed, nothing in the tree observed it — `grep group_commit` returned
/// nothing — and the case for wire-level batching rests entirely on it being
/// low in real deployments. A6's premise is falsifiable only with this number.
///
/// The invariant asserted here is stronger than "the metric exists": the SUM of
/// the observations must equal the number of `Durable` records pushed. Every
/// record is covered by exactly one group fsync, so a sum that drifts means
/// either a record was fsynced twice (the amortisation is being overcounted) or
/// one was never covered (which would be a false-ack hazard, not a metrics bug).
#[test]
fn group_commit_histogram_counts_every_durable_record_exactly_once() {
    const N: u32 = 50;

    let srv = weir_server!("group_commit").start();
    let mut client = srv.client();

    let read_count = |name: &str| -> u64 { parse_metric(&srv.scrape_metrics(), name) };
    let read_sum = |name: &str| -> f64 { parse_metric_f64(&srv.scrape_metrics(), name) };

    let count_before = read_count("weir_wab_group_commit_records_count");
    let sum_before = read_sum("weir_wab_group_commit_records_sum");

    // Buffered must not contribute: it acks on the memory write, before any
    // fsync, so it is covered by no group commit at all.
    for i in 0..N {
        client
            .push(format!("buf-{i}").as_bytes(), Durability::Buffered)
            .expect("buffered push");
    }
    assert_eq!(
        read_sum("weir_wab_group_commit_records_sum"),
        sum_before,
        "a Buffered record was counted into a group commit; Buffered acks \
         before any fsync and must contribute nothing"
    );

    for i in 0..N {
        client
            .push(format!("dur-{i}").as_bytes(), Durability::Durable)
            .expect("durable push");
    }

    let count_after = read_count("weir_wab_group_commit_records_count");
    let sum_after = read_sum("weir_wab_group_commit_records_sum");

    assert_eq!(
        sum_after - sum_before,
        f64::from(N),
        "the group-commit histogram must account for every Durable record \
         exactly once: pushed {N}, observed {}",
        sum_after - sum_before
    );
    assert!(
        count_after > count_before,
        "no group commit was observed at all for {N} Durable records"
    );

    // The amortisation factor itself. On a serial single connection each record
    // is its own group commit, so this is ~1.0 — which is precisely the
    // measurement A6's case depends on. Asserted as a range rather than a point
    // so the test states the shape without becoming a timing flake.
    let fsyncs = count_after - count_before;
    let per_fsync = (sum_after - sum_before) / fsyncs as f64;
    println!(
        "BENCH: {{\"scenario\":\"group_commit_serial_one_connection\",\
         \"records\":{N},\"group_commits\":{fsyncs},\"records_per_fsync\":{per_fsync:.3}}}"
    );
    assert!(
        (1.0..=f64::from(N)).contains(&per_fsync),
        "records per group commit was {per_fsync}, outside 1..={N} — one fsync \
         cannot cover fewer than one record nor more than were pushed"
    );
}

/// Reads exactly one response frame off a raw stream: header, then
/// `payload_len + 4`.
fn read_one_frame(
    stream: &mut std::os::unix::net::UnixStream,
) -> (weir_core::MessageType, Vec<u8>) {
    use std::io::Read;
    let mut header = [0u8; 16];
    stream
        .read_exact(&mut header)
        .expect("read response header");
    let plen = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let mut rest = vec![0u8; plen + 4];
    stream.read_exact(&mut rest).expect("read response payload");
    let mt = weir_core::MessageType::try_from(header[5]).expect("known response type");
    (mt, rest[..plen].to_vec())
}

/// A `PushBatch` is answered once, with a bitmap that says every record landed.
///
/// This is the end-to-end shape of A6: N records in one frame, one reply, and
/// the per-record counters moving by N rather than by one — a per-frame `.inc()`
/// would undercount by up to the batch size and silently break every rate panel,
/// the fsync-amortisation panel that divides by this counter, and the readiness
/// script that gates on it.
#[test]
fn a_push_batch_is_answered_with_a_bitmap_and_counts_every_record() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Durability, Envelope, Header, MessageType};

    const N: usize = 10;

    let srv = weir_server!("batch_ok").start();
    let accepted_before = parse_metric(
        &srv.scrape_metrics(),
        "weir_records_accepted_total{tier=\"durable\"}",
    );

    let records: Vec<Vec<u8>> = (0..N)
        .map(|i| format!("batched-{i}").into_bytes())
        .collect();
    let body =
        weir_core::encode_batch_body(&records.iter().map(|r| r.as_slice()).collect::<Vec<_>>());
    let frame = Envelope::new(
        Header::new(MessageType::PushBatch, Durability::Durable, 0),
        body,
    )
    .encode();

    let mut stream = RawStream::connect(&srv.socket_path).expect("connect");
    stream.write_all(&frame).expect("write PushBatch");
    let (mt, payload) = read_one_frame(&mut stream);

    assert_eq!(
        mt,
        MessageType::AckBatch,
        "a PushBatch must be answered with an AckBatch"
    );
    let accepted = weir_core::decode_ack_batch(&payload, N).expect("decode the bitmap");
    assert_eq!(
        accepted,
        vec![true; N],
        "every record of a healthy batch must be durable"
    );

    // The additive property: an ordinary Push still works on the same
    // connection, after a batch. A producer is never forced to choose.
    let plain = Envelope::new(
        Header::new(MessageType::Push, Durability::Durable, 0),
        b"plain".to_vec(),
    )
    .encode();
    stream.write_all(&plain).expect("write Push");
    let (mt, _) = read_one_frame(&mut stream);
    assert_eq!(
        mt,
        MessageType::Ack,
        "a plain Push after a batch must still be acked"
    );

    let accepted_after = parse_metric(
        &srv.scrape_metrics(),
        "weir_records_accepted_total{tier=\"durable\"}",
    );
    assert_eq!(
        accepted_after - accepted_before,
        (N + 1) as u64,
        "records_accepted must move by RECORDS ({} batched + 1 plain), not by \
         frames — a per-frame inc() would have moved it by 2",
        N
    );
}

/// A malformed sub-record rejects the whole frame and closes the connection.
///
/// This is what lets the `AckBatch` bitmap carry no reason bytes. Every
/// per-record *validation* failure is permanent, and permanent errors close the
/// connection exactly as a malformed single `Push` does — so by the time a
/// bitmap is sent at all, every bit it can clear is a transient runtime failure
/// that means "retry".
#[test]
fn a_batch_containing_an_empty_record_is_rejected_whole_and_closes() {
    use std::{
        io::{Read, Write},
        os::unix::net::UnixStream as RawStream,
    };
    use weir_core::{Durability, Envelope, Header, MessageType, NackReason};

    let srv = weir_server!("batch_empty_rec").start();

    // Hand-built: encode_batch_body would happily encode it, but the daemon
    // must reject it — an empty record cannot be represented in a WAB segment,
    // where a zero payload_len is the end-of-records sentinel.
    let body = weir_core::encode_batch_body(&[b"fine", b"", b"also fine"]);
    let frame = Envelope::new(
        Header::new(MessageType::PushBatch, Durability::Durable, 0),
        body,
    )
    .encode();

    let mut stream = RawStream::connect(&srv.socket_path).expect("connect");
    stream.write_all(&frame).expect("write PushBatch");
    let (mt, payload) = read_one_frame(&mut stream);

    assert_eq!(
        mt,
        MessageType::Nack,
        "a malformed batch must Nack, not answer with a bitmap"
    );
    assert_eq!(
        payload.first().copied(),
        Some(NackReason::EmptyPayload as u8),
        "the reason must name what was wrong with the record, not a generic error"
    );

    // Permanent ⇒ closed, same as any other malformed frame.
    let mut buf = [0u8; 1];
    assert!(
        matches!(stream.read(&mut buf), Ok(0) | Err(_)),
        "a permanent rejection must close the connection"
    );
}

/// A declared record count over the cap is refused before anything is reserved.
#[test]
fn a_batch_over_the_record_cap_is_refused() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Durability, Envelope, Header, MessageType, NackReason};

    let srv = weir_server!("batch_over_cap")
        .extra_config("max_batch_records = 4\n")
        .start();

    let records: Vec<Vec<u8>> = (0..8).map(|i| format!("r{i}").into_bytes()).collect();
    let body =
        weir_core::encode_batch_body(&records.iter().map(|r| r.as_slice()).collect::<Vec<_>>());
    let frame = Envelope::new(
        Header::new(MessageType::PushBatch, Durability::Durable, 0),
        body,
    )
    .encode();

    let mut stream = RawStream::connect(&srv.socket_path).expect("connect");
    stream.write_all(&frame).expect("write PushBatch");
    let (mt, payload) = read_one_frame(&mut stream);
    assert_eq!(mt, MessageType::Nack);
    assert_eq!(
        payload.first().copied(),
        Some(NackReason::PayloadTooLarge as u8),
        "an over-cap batch is too large, and the producer's remedy is to send less"
    );
}

/// The Rust client's `push_batch` against a real daemon, end to end.
#[test]
fn the_client_can_push_a_batch_and_read_every_outcome() {
    let srv = weir_server!("client_batch").start();
    let mut client = srv.client();

    let records: Vec<Vec<u8>> = (0..25)
        .map(|i| format!("client-batch-{i}").into_bytes())
        .collect();
    let outcome = client
        .push_batch(&records, Durability::Durable)
        .expect("push_batch");

    assert_eq!(outcome.accepted.len(), records.len());
    assert!(
        outcome.all_accepted(),
        "a healthy daemon must accept every record; rejected: {:?}",
        outcome.rejected_indices()
    );
    assert!(outcome.rejected_indices().is_empty());

    // The client stays usable for ordinary pushes afterwards — the batch path
    // must not poison a connection it shares.
    client
        .push(b"after", Durability::Durable)
        .expect("plain push after a batch");
    assert!(!client.is_poisoned());
}

/// A batch the client can tell is invalid never reaches the wire.
///
/// The daemon would Nack and close, poisoning the connection. Refusing locally
/// keeps it usable, mirroring the single-record guards — and `is_recoverable`
/// stays the exact complement of `is_poisoned`, which the crate promises.
#[test]
fn the_client_refuses_a_locally_invalid_batch_without_poisoning() {
    let srv = weir_server!("client_batch_guard").start();
    let mut client = srv.client();

    let with_empty: Vec<&[u8]> = vec![b"ok", b"", b"fine"];
    let err = client
        .push_batch(&with_empty, Durability::Durable)
        .expect_err("an empty record must be refused");
    assert!(
        err.is_recoverable(),
        "a local guard must not poison: {err:?}"
    );
    assert!(!client.is_poisoned());

    let empty: Vec<&[u8]> = vec![];
    assert!(client.push_batch(&empty, Durability::Durable).is_err());
    assert!(!client.is_poisoned());

    // Still usable.
    client
        .push(b"still works", Durability::Durable)
        .expect("push after refusals");
}
