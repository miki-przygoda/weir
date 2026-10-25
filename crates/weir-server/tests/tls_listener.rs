//! TCP + mutual-TLS integration tests (feature = "tls").
//!
//! These spin up a real `weir-server` child process (built with `--features
//! tls` — `CARGO_BIN_EXE_weir-server` points at the feature-enabled binary when
//! the test suite itself is run with `--features tls`) configured with BOTH a
//! Unix socket and a TCP+mTLS listener feeding the same pipeline. A blocking
//! rustls client connects over TCP and exercises the mutual-TLS contract:
//!
//! * a client holding a CA-signed cert is admitted and its pushes are acked; and
//! * a client with NO cert, a WRONG-CA cert, or an EXPIRED cert is rejected —
//!   it must never receive an `Ack`.
//!
//! The client uses `server_name = "weir-server"` to match the fixture server
//! cert's SAN (rcgen sets every Subject CN to a generic value, so we assert on
//! SAN-driven name verification, never on CN). See the Task 6 fixture notes.

#![cfg(feature = "tls")]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;
use weir_core::{Durability, Envelope, HEADER_LEN, Header, MessageType};
use weir_testkit::{tls::TlsFixture, weir_server};

// ── PEM loading ────────────────────────────────────────────────────────────────

fn load_certs(path: &std::path::Path) -> Vec<rustls_pki_types::CertificateDer<'static>> {
    use rustls_pki_types::pem::PemObject;
    let pem = std::fs::read(path).unwrap();
    rustls_pki_types::CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .unwrap()
}

fn load_key(path: &std::path::Path) -> rustls_pki_types::PrivateKeyDer<'static> {
    use rustls_pki_types::pem::PemObject;
    let pem = std::fs::read(path).unwrap();
    rustls_pki_types::PrivateKeyDer::from_pem_slice(&pem).unwrap()
}

fn root_store(ca_path: &std::path::Path) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    for ca in load_certs(ca_path) {
        roots.add(ca).unwrap();
    }
    roots
}

/// Builds a rustls `ClientConfig` with the fixture CA as roots and the given
/// client identity (cert + key). ring provider, explicitly, to match the
/// server. Returns an `Err` only if rustls rejects the cert/key pair at build
/// time (it doesn't for our fixtures).
fn client_config_with_cert(
    ca_path: &std::path::Path,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(root_store(ca_path))
        .with_client_auth_cert(load_certs(cert_path), load_key(key_path))
        .expect("client cert/key pair rejected at build time");
    Arc::new(cfg)
}

/// Builds a rustls `ClientConfig` that presents NO client certificate.
fn client_config_no_auth(ca_path: &std::path::Path) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(root_store(ca_path))
        .with_no_client_auth();
    Arc::new(cfg)
}

// ── Connect / push helpers ──────────────────────────────────────────────────────

type TlsClient = StreamOwned<ClientConnection, TcpStream>;

/// Opens a TCP connection to `addr`, drives the TLS handshake to completion, and
/// returns the wrapped stream. The handshake is forced eagerly (via a
/// `complete_io`-style flush+read) so handshake-level rejections surface here
/// rather than on the first application write.
fn tls_connect(addr: SocketAddr, config: Arc<ClientConfig>) -> std::io::Result<TlsClient> {
    let server_name = ServerName::try_from("weir-server").unwrap();
    let conn = ClientConnection::new(config, server_name)
        .map_err(|e| std::io::Error::other(format!("ClientConnection::new: {e}")))?;
    let sock = TcpStream::connect(addr)?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    sock.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut stream = StreamOwned::new(conn, sock);

    // Force the handshake now. A write of zero bytes drives rustls to flush its
    // ClientHello and process the server's response (incl. the
    // CertificateRequest and any handshake-level alert). For a client that the
    // server will reject (no cert / wrong CA / expired), this is where the
    // rejection most often surfaces; it may also surface on the first real
    // read/write below — the tests tolerate either.
    stream.flush()?;
    Ok(stream)
}

/// Encodes a Push frame (header + payload + payload CRC), identical wire shape
/// to the connection-layer tests' `push_frame`.
fn push_frame(payload: &[u8]) -> Vec<u8> {
    let header = Header::new(MessageType::Push, Durability::Durable, 0);
    Envelope::new(header, payload.to_vec()).encode()
}

/// Writes one Push frame and reads back one full response frame, returning its
/// MessageType. Any I/O error (including a TLS alert raised mid-exchange) is
/// propagated — a rejected client surfaces here as an `Err`.
fn push_one(stream: &mut TlsClient, payload: &[u8]) -> std::io::Result<MessageType> {
    stream.write_all(&push_frame(payload))?;
    stream.flush()?;

    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf)?;
    let header = Header::decode(&header_buf)
        .map_err(|e| std::io::Error::other(format!("decode response header: {e:?}")))?;
    let mut payload_buf = vec![0u8; header.payload_len() as usize];
    if !payload_buf.is_empty() {
        stream.read_exact(&mut payload_buf)?;
    }
    let mut crc = [0u8; 4];
    stream.read_exact(&mut crc)?;
    Ok(header.message_type())
}

// ── Tests ───────────────────────────────────────────────────────────────────────

#[test]
fn valid_client_cert_pushes_succeed() {
    let fx = TlsFixture::generate("valid_push");
    let srv = weir_server!("tls_valid").tls(&fx).start();
    let addr = srv.tcp_addr();

    let cfg = client_config_with_cert(&fx.ca_cert_path, &fx.client_cert_path, &fx.client_key_path);
    let mut stream = tls_connect(addr, cfg).expect("valid-cert handshake should succeed");

    for i in 0..1000 {
        let payload = format!("record-{i}");
        let resp = push_one(&mut stream, payload.as_bytes())
            .unwrap_or_else(|e| panic!("push {i} failed: {e}"));
        assert_eq!(resp, MessageType::Ack, "push {i} was not acked");
    }

    srv.shutdown();
}

#[test]
fn no_client_cert_is_rejected() {
    let fx = TlsFixture::generate("no_cert");
    let srv = weir_server!("tls_no_cert").tls(&fx).start();
    let addr = srv.tcp_addr();

    // Client presents NO certificate; the server's verifier requires one.
    let cfg = client_config_no_auth(&fx.ca_cert_path);

    // The rejection may surface at handshake time OR on the first frame
    // exchange. A robust assertion: the client must never receive an Ack.
    // Rejection surfaces as a fatal TLS alert (observed: CertificateRequired)
    // either at handshake time or on the first frame exchange. Either way the
    // client must never receive an Ack.
    let got_ack = match tls_connect(addr, cfg) {
        Ok(mut stream) => matches!(push_one(&mut stream, b"hello"), Ok(MessageType::Ack)),
        Err(_) => false,
    };
    assert!(
        !got_ack,
        "a client presenting no certificate must never be acked"
    );

    srv.shutdown();
}

#[test]
fn wrong_ca_client_cert_is_rejected() {
    let fx = TlsFixture::generate("wrong_ca");
    let srv = weir_server!("tls_wrong_ca").tls(&fx).start();
    let addr = srv.tcp_addr();

    // Client cert is signed by a DIFFERENT CA than the one the server trusts.
    let cfg = client_config_with_cert(
        &fx.ca_cert_path,
        &fx.wrong_ca_client_cert_path,
        &fx.wrong_ca_client_key_path,
    );

    // Rejection surfaces as a fatal TLS alert (observed: DecryptError, since the
    // wrong-CA cert fails the server's WebPki path validation). Never an Ack.
    let got_ack = match tls_connect(addr, cfg) {
        Ok(mut stream) => matches!(push_one(&mut stream, b"hello"), Ok(MessageType::Ack)),
        Err(_) => false,
    };
    assert!(
        !got_ack,
        "a client with a wrong-CA certificate must never be acked"
    );

    srv.shutdown();
}

#[test]
fn expired_client_cert_is_rejected() {
    let fx = TlsFixture::generate("expired");
    let srv = weir_server!("tls_expired").tls(&fx).start();
    let addr = srv.tcp_addr();

    // Client cert is CA-signed but past its notAfter.
    let cfg = client_config_with_cert(
        &fx.ca_cert_path,
        &fx.expired_client_cert_path,
        &fx.expired_client_key_path,
    );

    // Rejection surfaces as a fatal TLS alert (observed: CertificateExpired).
    // Never an Ack.
    let got_ack = match tls_connect(addr, cfg) {
        Ok(mut stream) => matches!(push_one(&mut stream, b"hello"), Ok(MessageType::Ack)),
        Err(_) => false,
    };
    assert!(
        !got_ack,
        "a client with an expired certificate must never be acked"
    );

    srv.shutdown();
}

/// Coverage gap (T04 / F29): a client that completes the TCP handshake but never
/// sends a TLS ClientHello must be dropped within ~handshake_timeout, not pin the
/// shared max_connections permit. Proves the handshake-timeout branch fires,
/// closes the connection, and records the failure metric.
#[test]
fn tls_handshake_timeout_drops_stalled_tcp_client() {
    let fx = TlsFixture::generate("hs_timeout");
    let srv = weir_server!("tls_hs_timeout")
        .tls(&fx)
        .extra_config("tls_handshake_timeout_secs = 1")
        .start();
    let addr = srv.tcp_addr();

    // Complete the TCP handshake but send nothing. The daemon must drop us.
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 1];
    // Server closes the connection → read returns Ok(0) (EOF). If it wrongly kept
    // the connection open, the 5s read timeout fires and elapsed blows the bound.
    let n = sock.read(&mut buf).unwrap_or(0);
    let elapsed = start.elapsed();
    assert_eq!(
        n, 0,
        "the stalled-handshake connection must be closed by the server"
    );
    assert!(
        elapsed < Duration::from_secs(4),
        "must be dropped within ~handshake_timeout (1s), took {elapsed:?}"
    );

    let body = srv.scrape_metrics();
    let failures = body
        .lines()
        .find(|l| l.starts_with("weir_tls_handshake_failures_total{reason=\"timeout\"}"))
        .and_then(|l| l.split_whitespace().next_back())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        failures >= 1,
        "a handshake timeout must increment weir_tls_handshake_failures_total{{reason=\"timeout\"}}, got {failures}"
    );

    srv.shutdown();
}

/// A counter sample from a scrape, 0 if absent.
fn metric(srv: &weir_testkit::WeirServer, series: &str) -> u64 {
    srv.scrape_metrics()
        .lines()
        .find(|l| l.starts_with(series) && l[series.len()..].starts_with(' '))
        .and_then(|l| l.split_whitespace().next_back())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Whether a client with `cfg` completes the handshake and gets an Ack.
fn acked(addr: SocketAddr, cfg: Arc<ClientConfig>) -> bool {
    match tls_connect(addr, cfg) {
        Ok(mut stream) => matches!(push_one(&mut stream, b"hello"), Ok(MessageType::Ack)),
        Err(_) => false,
    }
}

/// spec §3.2: SIGHUP goes through the one reload path, which still reloads the
/// TLS material in place. Rotate the files the daemon was configured with to a
/// whole new CA, server cert and client CA, send HUP: both counters say `ok`,
/// a new handshake sees the new server cert, and the old identity is refused.
#[test]
fn sighup_reloads_tls_when_configured() {
    let fx = TlsFixture::generate("hup_rotate_old");
    let next = TlsFixture::generate("hup_rotate_new");
    let mut srv = weir_server!("tls_hup").tls(&fx).log_level("info").start();
    let addr = srv.tcp_addr();
    let old_client =
        client_config_with_cert(&fx.ca_cert_path, &fx.client_cert_path, &fx.client_key_path);
    let new_client = client_config_with_cert(
        &next.ca_cert_path,
        &next.client_cert_path,
        &next.client_key_path,
    );
    assert!(
        acked(addr, Arc::clone(&old_client)),
        "control: old identity"
    );
    assert!(
        !acked(addr, Arc::clone(&new_client)),
        "control: the next identity is not trusted before the rotation"
    );

    // Rotate in place: the configured paths now hold the next material.
    for (from, to) in [
        (&next.server_cert_path, &fx.server_cert_path),
        (&next.server_key_path, &fx.server_key_path),
        (&next.ca_cert_path, &fx.ca_cert_path),
    ] {
        std::fs::copy(from, to).unwrap();
    }
    // SAFETY: kill(2) on the child we spawned and still own.
    assert_eq!(
        unsafe { libc::kill(srv.child_pid() as libc::pid_t, libc::SIGHUP) },
        0
    );

    let tls_ok = "weir_tls_config_reloads_total{outcome=\"ok\"}";
    let cfg_ok = "weir_config_reloads_total{trigger=\"sighup\",outcome=\"ok\"}";
    let deadline = Instant::now() + Duration::from_secs(5);
    while metric(&srv, cfg_ok) < 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(metric(&srv, tls_ok), 1, "{tls_ok}");
    assert_eq!(metric(&srv, cfg_ok), 1, "{cfg_ok}");
    assert_eq!(
        metric(
            &srv,
            "weir_config_reloads_total{trigger=\"sighup\",outcome=\"noop\"}"
        ),
        0
    );
    assert!(
        srv.wait_for_exit(Duration::from_millis(100)).is_none(),
        "SIGHUP must not stop the daemon"
    );

    // A new handshake sees the new server cert.
    let mut stream = tls_connect(addr, Arc::clone(&new_client)).expect("handshake after rotation");
    assert_eq!(
        push_one(&mut stream, b"after-rotation").unwrap(),
        MessageType::Ack
    );
    let presented = stream
        .conn
        .peer_certificates()
        .expect("server presented a cert")[0]
        .clone();
    assert_eq!(
        presented,
        load_certs(&next.server_cert_path)[0],
        "the server must present the rotated cert"
    );
    drop(stream);
    assert!(
        !acked(addr, old_client),
        "the old identity must be refused after the rotation"
    );

    srv.shutdown();
}

/// spec §16 (Admin): reload with good certificates → applied; a corrupt key
/// → failed, and the certificates in force still handshake.
#[test]
fn admin_reload_applies_rotated_certs_and_keeps_them_on_a_corrupt_key() {
    let fx = TlsFixture::generate("admin_rotate_old");
    let next = TlsFixture::generate("admin_rotate_new");
    // Short label: the derived <tmp>/weir_test_<label>_<pid>/wab.admin.sock
    // must stay <= 103 bytes on macOS, or admin is disabled (path_too_long).
    let srv = weir_server!("tls_adm_rl").tls(&fx).start();
    let addr = srv.tcp_addr();
    let new_client = client_config_with_cert(
        &next.ca_cert_path,
        &next.client_cert_path,
        &next.client_key_path,
    );
    for (from, to) in [
        (&next.server_cert_path, &fx.server_cert_path),
        (&next.server_key_path, &fx.server_key_path),
        (&next.ca_cert_path, &fx.ca_cert_path),
    ] {
        std::fs::copy(from, to).unwrap();
    }

    let r: serde_json::Value = serde_json::from_str(&srv.admin("reload", "{}")).unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(r["result"]["outcome"], "applied", "{r}");
    assert_eq!(r["result"]["items"][0]["item"], "tls_certificates");
    assert_eq!(r["result"]["items"][0]["outcome"], "reloaded");
    assert_eq!(
        metric(
            &srv,
            "weir_config_reloads_total{trigger=\"admin\",outcome=\"ok\"}"
        ),
        1
    );
    assert!(
        acked(addr, Arc::clone(&new_client)),
        "the rotated identity is served"
    );

    std::fs::write(&fx.server_key_path, "not a key").unwrap();
    let r: serde_json::Value = serde_json::from_str(&srv.admin("reload", "{}")).unwrap();
    assert_eq!(
        r["ok"], true,
        "a failed reload is ok:true with outcome failed: {r}"
    );
    assert_eq!(r["result"]["outcome"], "failed");
    assert_eq!(r["result"]["items"][0]["outcome"], "failed");
    assert!(
        r["result"]["items"][0]["detail"].is_string(),
        "admin detail says why"
    );
    assert_eq!(
        metric(
            &srv,
            "weir_config_reloads_total{trigger=\"admin\",outcome=\"failed\"}"
        ),
        1
    );
    assert!(
        acked(addr, new_client),
        "the certificates in force still handshake"
    );
    srv.shutdown();
}

/// Lame duck on the TCP side (Foundations §7): within 200 ms of SIGTERM the
/// TCP listener refuses new connections, while the process is still alive
/// (its drain is spending the budget on a black-holed sink), and the exit is
/// bounded by shutdown_timeout + budget + 6 s. The 200 ms half is pinned
/// in-process by Task 4.7's `tcp_lame_duck_closes_the_listener_before_the_connection_drain`.
#[test]
fn sigterm_closes_the_tcp_listener_at_once() {
    let fx = TlsFixture::generate("tcp_lame_duck");
    let hole = weir_testkit::RecordingSink::start_hanging();
    let mut srv = weir_server!("tcp_lame_duck")
        .tls(&fx)
        .extra_config(
            "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
             drain_shutdown_budget_secs = 4",
        )
        .env("WEIR_SINK_URL", hole.url())
        .start();
    let addr = srv.tcp_addr();
    // A sealed segment for the drain to hang on.
    let cfg = client_config_with_cert(&fx.ca_cert_path, &fx.client_cert_path, &fx.client_key_path);
    let mut stream = tls_connect(addr, cfg).expect("handshake");
    for i in 0..8 {
        let mut p = format!("tcp-ld-{i}-").into_bytes();
        p.resize(2048, b't');
        assert_eq!(push_one(&mut stream, &p).unwrap(), MessageType::Ack);
    }
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(15);
    while hole.connections() == 0 {
        assert!(
            Instant::now() < deadline,
            "the drain never reached the sink"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let t0 = Instant::now();
    srv.signal(libc::SIGTERM);
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
            Err(_) => break,
            Ok(_) => assert!(
                t0.elapsed() < Duration::from_millis(200),
                "the TCP listener must close within 200 ms of SIGTERM"
            ),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        srv.wait_for_exit(Duration::from_millis(100)).is_none(),
        "the process must still be in its drain budget"
    );
    let status = srv
        .wait_for_exit(Duration::from_secs(3 + 4 + 6))
        .expect("shutdown not bounded");
    assert!(status.success());
}
