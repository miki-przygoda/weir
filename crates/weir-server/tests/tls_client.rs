//! Non-ignored integration tests for the **weir-client** mutual-TLS path
//! (`feature = "tls"`).
//!
//! `tests/tls_listener.rs` proves the *server's* cert-rejection behaviour, but
//! it hand-rolls a raw rustls client and never touches `WeirClient::connect_tls`.
//! `tests/load_tls.rs` does drive `connect_tls`, but every test there is
//! `#[ignore]`d (it's a benchmark). So the client-side mTLS trust-boundary
//! constructor — the one consumers actually use — had no coverage in a normal
//! `cargo test --features tls` run. These tests close that gap: they push the
//! happy path AND the three rejection cases (wrong CA, expired cert, mismatched
//! server_name) THROUGH `WeirClient::connect_tls`.

#![cfg(feature = "tls")]

use weir_client::{ClientTlsConfig, WeirClient};
use weir_core::Durability;
use weir_testkit::{tls::TlsFixture, weir_server};

/// A valid client config: CA-signed client cert + the server_name that matches
/// the fixture server cert's SAN (`weir-server`).
fn valid_cfg(fx: &TlsFixture) -> ClientTlsConfig<'_> {
    ClientTlsConfig {
        client_cert: &fx.client_cert_path,
        client_key: &fx.client_key_path,
        ca_cert: &fx.ca_cert_path,
        server_name: "weir-server",
        default_durability: None,
    }
}

/// Drives connect_tls then a push; rustls defers the handshake, so a rejected
/// certificate surfaces either at `connect_tls` or at the first `push`. Either
/// way the result is an `Err` — that's what we assert.
fn connect_then_push(addr: std::net::SocketAddr, cfg: ClientTlsConfig<'_>) -> Result<(), String> {
    let mut client = WeirClient::connect_tls(addr, cfg).map_err(|e| e.to_string())?;
    client
        .push(b"probe", Durability::Buffered)
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[test]
fn connect_tls_with_valid_cert_pushes_and_acks() {
    let fx = TlsFixture::generate("client_valid");
    let srv = weir_server!("tls_client_valid").tls(&fx).start();
    let addr = srv.tcp_addr();

    let mut client =
        WeirClient::connect_tls(addr, valid_cfg(&fx)).expect("connect_tls with a valid cert");
    for i in 0..16 {
        client
            .push(format!("rec-{i}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("push {i} through connect_tls failed: {e}"));
    }
    assert!(!client.is_poisoned(), "a healthy mTLS client must not be poisoned");

    srv.shutdown();
}

#[test]
fn connect_tls_with_wrong_ca_cert_is_rejected() {
    let fx = TlsFixture::generate("client_wrong_ca");
    let srv = weir_server!("tls_client_wrong_ca").tls(&fx).start();
    let addr = srv.tcp_addr();

    // A client cert signed by a DIFFERENT CA than the server trusts.
    let cfg = ClientTlsConfig {
        client_cert: &fx.wrong_ca_client_cert_path,
        client_key: &fx.wrong_ca_client_key_path,
        ca_cert: &fx.ca_cert_path,
        server_name: "weir-server",
        default_durability: None,
    };
    assert!(
        connect_then_push(addr, cfg).is_err(),
        "a wrong-CA client cert must be rejected through WeirClient::connect_tls"
    );

    srv.shutdown();
}

#[test]
fn connect_tls_with_expired_cert_is_rejected() {
    let fx = TlsFixture::generate("client_expired");
    let srv = weir_server!("tls_client_expired").tls(&fx).start();
    let addr = srv.tcp_addr();

    let cfg = ClientTlsConfig {
        client_cert: &fx.expired_client_cert_path,
        client_key: &fx.expired_client_key_path,
        ca_cert: &fx.ca_cert_path,
        server_name: "weir-server",
        default_durability: None,
    };
    assert!(
        connect_then_push(addr, cfg).is_err(),
        "an expired client cert must be rejected through WeirClient::connect_tls"
    );

    srv.shutdown();
}

#[test]
fn connect_tls_with_mismatched_server_name_is_rejected() {
    let fx = TlsFixture::generate("client_bad_sni");
    let srv = weir_server!("tls_client_bad_sni").tls(&fx).start();
    let addr = srv.tcp_addr();

    // Valid client cert, but a server_name that does not match the server cert's
    // SAN (`weir-server`) — the client must refuse to trust the server.
    let cfg = ClientTlsConfig {
        client_cert: &fx.client_cert_path,
        client_key: &fx.client_key_path,
        ca_cert: &fx.ca_cert_path,
        server_name: "not-weir-server",
        default_durability: None,
    };
    assert!(
        connect_then_push(addr, cfg).is_err(),
        "a server_name not matching the server cert SAN must be rejected client-side"
    );

    srv.shutdown();
}
