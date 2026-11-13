//! Acceptance tests for epic d10-and-batch-error, behaviour group
//! client-batch (spec flow 4): the error `WeirClient::push_batch` returns for a
//! batch with too many records, `ClientError::TooManyRecords`.
//!
//! Each test drives the public `weir-client` API against the built
//! `weir-server` binary (spawned through `weir-testkit`), or against a peer the
//! test owns where the question is "what reached the wire". Nothing here
//! reaches into the client's internals.

#![cfg(unix)]

use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use weir_client::{ClientError, NackReason, WeirClient};
use weir_core::{
    BATCH_HEADER_LEN, Durability, HEADER_LEN, Header, MAX_BATCH_RECORDS_HARD_CAP,
    MAX_PAYLOAD_HARD_CAP, MessageType, WIRE_VERSION,
};
use weir_testkit::weir_server;

// ── helpers ──────────────────────────────────────────────────────────────────

/// `n` one-byte records.
fn one_byte_records(n: usize) -> Vec<[u8; 1]> {
    vec![[1u8]; n]
}

/// Asserts `err` is exactly `TooManyRecords { count, limit: MAX_BATCH_RECORDS_HARD_CAP }`.
fn assert_too_many(err: &ClientError, count: usize) {
    match err {
        ClientError::TooManyRecords { count: c, limit } => {
            assert_eq!(*c, count, "count must be records.len()");
            assert_eq!(
                *limit, MAX_BATCH_RECORDS_HARD_CAP,
                "limit must be the hard cap"
            );
        }
        other => panic!("expected TooManyRecords {{ count: {count}, .. }}, got {other:?}"),
    }
}

/// A Unix-socket proxy between a client and the real daemon that records every
/// byte the client sends. Lets a test say what reached the wire while the
/// daemon still answers for real.
struct RecordingProxy {
    path: PathBuf,
    _dir: TempDir,
    sent: Arc<Mutex<Vec<u8>>>,
}

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl RecordingProxy {
    fn start(tag: &str, upstream: &Path) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "weir_acc_cb_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("proxy.sock");
        let listener = UnixListener::bind(&path).expect("bind proxy socket");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let upstream = upstream.to_path_buf();
        let sent_c = Arc::clone(&sent);
        thread::spawn(move || {
            let Ok((mut client_side, _)) = listener.accept() else {
                return;
            };
            let mut daemon_side = UnixStream::connect(&upstream).expect("proxy: connect daemon");
            let mut daemon_rd = daemon_side.try_clone().unwrap();
            let mut client_wr = client_side.try_clone().unwrap();
            // daemon -> client
            thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match daemon_rd.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if client_wr.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                    }
                }
                let _ = client_wr.shutdown(std::net::Shutdown::Write);
            });
            // client -> daemon, recorded
            let mut buf = [0u8; 8192];
            loop {
                match client_side.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        sent_c.lock().unwrap().extend_from_slice(&buf[..n]);
                        if daemon_side.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = daemon_side.shutdown(std::net::Shutdown::Write);
        });
        RecordingProxy {
            path,
            _dir: TempDir(dir),
            sent,
        }
    }

    fn sent(&self) -> Vec<u8> {
        self.sent.lock().unwrap().clone()
    }
}

// ── CLIENT-BATCH-01 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-01: 2,049 one-byte records against a real daemon return
/// `TooManyRecords { count: 2049, limit: 2048 }`.
#[test]
fn client_batch_01_too_many_records_returns_too_many_records() {
    assert_eq!(MAX_BATCH_RECORDS_HARD_CAP, 2048);
    let srv = weir_server!("acc_cb_01").start();
    let mut client = srv.client();

    let records = one_byte_records(2049);
    let err = client
        .push_batch(&records, Durability::Durable)
        .map(|o| o.accepted)
        .expect_err("2,049 records must be refused");
    assert!(
        matches!(
            err,
            ClientError::TooManyRecords {
                count: 2049,
                limit: 2048
            }
        ),
        "expected TooManyRecords {{ count: 2049, limit: 2048 }}, got {err:?}"
    );
}

// ── CLIENT-BATCH-02 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-02: every client batch entry point (`push_batch`, over every
/// transport and record type) reports a record-count overflow as
/// `TooManyRecords { count: records.len(), limit: MAX_BATCH_RECORDS_HARD_CAP }`,
/// never `PayloadTooLarge`.
#[test]
fn client_batch_02_every_batch_entry_point_reports_the_count() {
    let srv = weir_server!("acc_cb_02").start();

    for count in [
        MAX_BATCH_RECORDS_HARD_CAP + 1,
        MAX_BATCH_RECORDS_HARD_CAP + 7,
        3000,
        2 * MAX_BATCH_RECORDS_HARD_CAP,
        10_000,
    ] {
        for durability in [Durability::Buffered, Durability::Durable] {
            // Unix socket, connected by path.
            let mut client = srv.client();

            // `&[[u8; 1]]`
            let arrays = one_byte_records(count);
            let err = client.push_batch(&arrays, durability).unwrap_err();
            assert_too_many(&err, count);

            // `&[Vec<u8>]`
            let vecs: Vec<Vec<u8>> = (0..count).map(|i| vec![(i % 251) as u8 + 1; 3]).collect();
            let err = client.push_batch(&vecs, durability).unwrap_err();
            assert_too_many(&err, count);

            // `&[&[u8]]`
            let slices: Vec<&[u8]> = vecs.iter().map(Vec::as_slice).collect();
            let err = client.push_batch(&slices, durability).unwrap_err();
            assert_too_many(&err, count);

            // `&[String]`
            let strings: Vec<String> = (0..count).map(|i| format!("r{i}")).collect();
            let err = client.push_batch(&strings, durability).unwrap_err();
            assert_too_many(&err, count);

            // A client made with a default durability.
            let mut with_default =
                WeirClient::connect_with_default(&srv.socket_path, durability).unwrap();
            let err = with_default.push_batch(&arrays, durability).unwrap_err();
            assert_too_many(&err, count);
        }
    }

    // The TCP+mTLS transport takes the same entry point.
    #[cfg(feature = "tls")]
    {
        use weir_client::ClientTlsConfig;
        use weir_testkit::tls::TlsFixture;

        drop(srv);
        let fx = TlsFixture::generate("acc_cb_02_tls");
        let srv = weir_server!("acc_cb_02_tls").tls(&fx).start();
        let mut client = WeirClient::connect_tls(
            srv.tcp_addr(),
            ClientTlsConfig {
                client_cert: &fx.client_cert_path,
                client_key: &fx.client_key_path,
                ca_cert: &fx.ca_cert_path,
                server_name: "weir-server",
                default_durability: None,
            },
        )
        .expect("connect_tls");
        for count in [MAX_BATCH_RECORDS_HARD_CAP + 1, 5000] {
            let err = client
                .push_batch(&one_byte_records(count), Durability::Durable)
                .unwrap_err();
            assert_too_many(&err, count);
        }
        client
            .push(b"tls still usable", Durability::Durable)
            .expect("TLS push after TooManyRecords");
    }
}

// ── CLIENT-BATCH-03 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-03: exactly `MAX_BATCH_RECORDS_HARD_CAP` small records are not
/// refused for their count. The daemon is configured to accept the full hard
/// cap (its default is 1024), so the batch goes through end to end.
#[test]
fn client_batch_03_a_batch_at_the_cap_is_not_rejected_for_its_count() {
    let srv = weir_server!("acc_cb_03")
        .extra_config(format!("max_batch_records = {MAX_BATCH_RECORDS_HARD_CAP}"))
        .start();
    let mut client = srv.client();

    let records: Vec<Vec<u8>> = (0..MAX_BATCH_RECORDS_HARD_CAP)
        .map(|i| format!("cap-{i}").into_bytes())
        .collect();
    match client.push_batch(&records, Durability::Durable) {
        Ok(outcome) => {
            assert_eq!(outcome.accepted.len(), MAX_BATCH_RECORDS_HARD_CAP);
            assert!(
                outcome.all_accepted(),
                "a healthy daemon accepts every record; rejected: {:?}",
                outcome.rejected_indices()
            );
        }
        Err(e @ ClientError::TooManyRecords { .. }) => {
            panic!("a batch of exactly the hard cap was refused for its count: {e}")
        }
        Err(e) => panic!("push_batch at the cap failed: {e:?}"),
    }
}

// ── CLIENT-BATCH-04 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-04: `TooManyRecords` is a local rejection. No bytes reach the
/// wire, the error is recoverable and the client is not poisoned.
#[test]
fn client_batch_04_too_many_records_is_local_and_recoverable() {
    let srv = weir_server!("acc_cb_04").start();
    let proxy = RecordingProxy::start("04", &srv.socket_path);
    let mut client = WeirClient::connect(&proxy.path).expect("connect via proxy");

    let err = client
        .push_batch(&one_byte_records(2049), Durability::Durable)
        .unwrap_err();
    assert_too_many(&err, 2049);
    assert!(err.is_recoverable(), "TooManyRecords must be recoverable");
    assert!(!client.is_poisoned(), "TooManyRecords must not poison");

    // Give any stray write time to land in the proxy, then check nothing did.
    thread::sleep(Duration::from_millis(200));
    assert!(
        proxy.sent().is_empty(),
        "no bytes may be sent for a refused batch; the proxy saw {} bytes",
        proxy.sent().len()
    );

    // A following push is acked by the real daemon only once it has read the
    // frame, so after the ack the proxy holds everything the client ever sent:
    // exactly one Push frame and nothing before it.
    client
        .push(b"after", Durability::Durable)
        .expect("push after TooManyRecords");
    let sent = proxy.sent();
    assert_eq!(
        sent.len(),
        HEADER_LEN + b"after".len() + 4,
        "the wire must carry only the following Push frame"
    );
    let header = Header::decode(sent[..HEADER_LEN].try_into().unwrap()).expect("decode header");
    assert_eq!(header.message_type(), MessageType::Push);
}

// ── CLIENT-BATCH-05 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-05: the same client pushes successfully after a
/// `TooManyRecords { count: 2049, limit: 2048 }`, and a batch within the cap
/// works too.
#[test]
fn client_batch_05_the_client_stays_usable_after_too_many_records() {
    let srv = weir_server!("acc_cb_05").start();
    let mut client = srv.client();

    let err = client
        .push_batch(&one_byte_records(2049), Durability::Durable)
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::TooManyRecords {
                count: 2049,
                limit: 2048
            }
        ),
        "got {err:?}"
    );

    client
        .push(b"next push", Durability::Durable)
        .expect("the next push on the same client must succeed");
    assert!(!client.is_poisoned());

    let outcome = client
        .push_batch(&[b"a".as_slice(), b"b", b"c"], Durability::Durable)
        .expect("a batch within the cap on the same client");
    assert!(outcome.all_accepted());
}

// ── CLIENT-BATCH-06 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-06: the Display text, both for a constructed error and for the
/// one `push_batch` returns.
#[test]
fn client_batch_06_too_many_records_display_text() {
    const WANT: &str = "too many records: 2049 exceeds the 2048-record batch hard cap";
    let err = ClientError::TooManyRecords {
        count: 2049,
        limit: 2048,
    };
    assert_eq!(err.to_string(), WANT);
    assert_eq!(format!("{err}"), WANT);

    let srv = weir_server!("acc_cb_06").start();
    let mut client = srv.client();
    let err = client
        .push_batch(&one_byte_records(2049), Durability::Durable)
        .unwrap_err();
    assert_eq!(err.to_string(), WANT);
}

// ── CLIENT-BATCH-07 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-07: within the record cap, `PayloadTooLarge` reports bytes, for
/// one record over the byte hard cap and for a body over it; a record-count
/// overflow is never `PayloadTooLarge`, even when a record is also too large.
#[test]
fn client_batch_07_payload_too_large_means_bytes_only() {
    let srv = weir_server!("acc_cb_07").start();
    let mut client = srv.client();

    // A single record one byte over the hard cap.
    let big = vec![7u8; MAX_PAYLOAD_HARD_CAP + 1];
    let records: Vec<&[u8]> = vec![b"small", &big];
    let err = client
        .push_batch(&records, Durability::Durable)
        .unwrap_err();
    match err {
        ClientError::PayloadTooLarge { len, limit } => {
            assert_eq!(len, MAX_PAYLOAD_HARD_CAP + 1, "len is the record's bytes");
            assert_eq!(limit, MAX_PAYLOAD_HARD_CAP, "limit is the byte hard cap");
        }
        other => panic!("expected PayloadTooLarge for an over-cap record, got {other:?}"),
    }
    assert!(!client.is_poisoned());

    // Records that each fit but whose whole body is over the cap.
    let nine_mib = vec![9u8; 9 * 1024 * 1024];
    let records = [nine_mib.clone(), nine_mib];
    let body_len = BATCH_HEADER_LEN + 2 * (4 + 9 * 1024 * 1024);
    let err = client
        .push_batch(&records, Durability::Durable)
        .unwrap_err();
    match err {
        ClientError::PayloadTooLarge { len, limit } => {
            assert_eq!(len, body_len, "len is the body's bytes");
            assert_eq!(limit, MAX_PAYLOAD_HARD_CAP);
        }
        other => panic!("expected PayloadTooLarge for an over-cap body, got {other:?}"),
    }
    assert!(!client.is_poisoned());

    // Too many records, one of them also over the byte cap: the count wins.
    let mut over: Vec<&[u8]> = vec![b"x"; MAX_BATCH_RECORDS_HARD_CAP];
    over.push(&big);
    let err = client.push_batch(&over, Durability::Durable).unwrap_err();
    assert!(
        !matches!(err, ClientError::PayloadTooLarge { .. }),
        "a record-count overflow must never be PayloadTooLarge: {err:?}"
    );
    assert_too_many(&err, MAX_BATCH_RECORDS_HARD_CAP + 1);

    client
        .push(b"still usable", Durability::Durable)
        .expect("push after local refusals");
}

// ── CLIENT-BATCH-08 ──────────────────────────────────────────────────────────

/// CLIENT-BATCH-08: `TooManyRecords { count: usize, limit: usize }` is a new
/// public variant, and every variant the previous release (v4.1.0) had keeps
/// its meaning: the same Display text and the same `is_recoverable` answer as
/// in v4.1.0 (the expected values below are copied from that release).
/// `ClientError` stays `#[non_exhaustive]`, so a downstream match needs a
/// wildcard arm; the match below has one and would still compile if it did not
/// need it, so non-exhaustiveness itself is checked by the docs, not here.
#[test]
fn client_batch_08_the_new_variant_is_additive() {
    // New public variant with usize fields, built from outside the crate.
    let count: usize = 2049;
    let limit: usize = MAX_BATCH_RECORDS_HARD_CAP;
    let new = ClientError::TooManyRecords { count, limit };
    let (c, l): (usize, usize) = match &new {
        ClientError::TooManyRecords { count, limit } => (*count, *limit),
        _ => unreachable!(),
    };
    assert_eq!((c, l), (2049, 2048));
    assert!(new.is_recoverable());
    // It is a std error with no source, like the other local rejections.
    assert!(std::error::Error::source(&new).is_none());

    // Every v4.1.0 variant: (error, v4.1.0 Display, v4.1.0 is_recoverable).
    let old: Vec<(ClientError, String, bool)> = vec![
        (
            ClientError::Io(std::io::Error::other("boom")),
            "socket I/O error: boom".into(),
            false,
        ),
        (
            ClientError::Nack(NackReason::InternalError),
            "server nack: InternalError".into(),
            true,
        ),
        (
            ClientError::Nack(NackReason::PayloadTooLarge),
            "server nack: PayloadTooLarge".into(),
            false,
        ),
        (
            ClientError::VersionMismatch { daemon_version: 9 },
            format!("wire version mismatch: daemon speaks v9, this client speaks v{WIRE_VERSION}"),
            false,
        ),
        (
            ClientError::UnknownNack(0xfe),
            "server nack with unknown reason 0xfe".into(),
            false,
        ),
        (
            ClientError::PayloadTooLarge {
                len: 16_777_217,
                limit: 16_777_216,
            },
            "payload too large: 16777217 bytes exceeds the 16777216-byte protocol hard cap".into(),
            true,
        ),
        (
            ClientError::Protocol("bad".into()),
            "protocol error: bad".into(),
            false,
        ),
        (
            ClientError::NoDefaultDurability,
            "push_default called but no default durability was set".into(),
            true,
        ),
        (
            ClientError::EmptyPayload,
            "payload is empty; rejected locally (an empty payload is the WAB end-of-records sentinel)"
                .into(),
            true,
        ),
    ];
    for (err, display, recoverable) in &old {
        assert_eq!(&err.to_string(), display, "Display changed for {err:?}");
        assert_eq!(
            err.is_recoverable(),
            *recoverable,
            "is_recoverable changed for {err:?}"
        );
    }

    // PayloadTooLarge is bytes-only: its fields are still `len` and `limit`
    // in bytes, and the new variant's text is distinct from it.
    assert!(!new.to_string().contains("bytes"));
    assert_ne!(
        new.to_string(),
        ClientError::PayloadTooLarge { len: count, limit }.to_string()
    );
}
