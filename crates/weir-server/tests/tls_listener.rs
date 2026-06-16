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
    time::Duration,
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
/// client identity (cert + key). aws-lc-rs provider, explicitly, to match the
/// server. Returns an `Err` only if rustls rejects the cert/key pair at build
/// time (it doesn't for our fixtures).
fn client_config_with_cert(
    ca_path: &std::path::Path,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
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
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
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
    let header = Header::new(MessageType::Push, Durability::Sync, 0);
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
