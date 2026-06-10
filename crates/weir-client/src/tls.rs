//! Blocking mutual-TLS client connector (feature = "tls").
//!
//! Connects to a weir daemon's TCP+mTLS listener using the same aws-lc-rs
//! provider and rustls 0.23 API that the server side uses.

use std::{io::BufReader, net::TcpStream, path::Path, sync::Arc};

use rustls_pki_types::ServerName;
use weir_core::Durability;

use crate::{ClientError, WeirClient};

/// TLS configuration for [`WeirClient::connect_tls`].
pub struct ClientTlsConfig<'a> {
    /// Path to the PEM-encoded client certificate file.
    pub client_cert: &'a Path,
    /// Path to the PEM-encoded client private key file.
    pub client_key: &'a Path,
    /// Path to the PEM-encoded CA certificate used to verify the server cert.
    pub ca_cert: &'a Path,
    /// DNS name the server certificate must present (matched against its SAN).
    pub server_name: &'a str,
    /// Optional default durability tier for [`WeirClient::push_default`].
    pub default_durability: Option<Durability>,
}

/// The TLS stream type returned by [`WeirClient::connect_tls`].
pub type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

impl WeirClient<TlsStream> {
    /// Connects to a weir daemon's TCP+mTLS listener at `addr`.
    ///
    /// Performs a full mutual-TLS handshake: the client presents `client_cert`
    /// (signed by the same CA the server trusts), and the server certificate is
    /// validated against `ca_cert`. The `server_name` must match a SAN in the
    /// server's certificate.
    pub fn connect_tls(
        addr: impl std::net::ToSocketAddrs,
        cfg: ClientTlsConfig<'_>,
    ) -> Result<Self, ClientError> {
        // Build root store from the provided CA cert.
        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(cfg.ca_cert)? {
            roots
                .add(c)
                .map_err(|e| ClientError::Protocol(format!("ca cert: {e}")))?;
        }

        // Load client identity.
        let certs = load_certs(cfg.client_cert)?;
        let key = load_key(cfg.client_key)?;

        // Build rustls ClientConfig using aws-lc-rs provider (matches the server).
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClientError::Protocol(format!("tls versions: {e}")))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| ClientError::Protocol(format!("client auth cert: {e}")))?;

        // Parse and validate the server name.
        let server_name = ServerName::try_from(cfg.server_name.to_string())
            .map_err(|e| ClientError::Protocol(format!("server name: {e}")))?;

        // Create the TLS connection object and wrap a TCP socket.
        let conn = rustls::ClientConnection::new(Arc::new(tls_config), server_name)
            .map_err(|e| ClientError::Protocol(format!("tls connect: {e}")))?;
        let sock = TcpStream::connect(addr)?;
        let stream = rustls::StreamOwned::new(conn, sock);

        Ok(Self::from_parts(stream, cfg.default_durability))
    }
}

// ── PEM helpers ────────────────────────────────────────────────────────────────

fn load_certs(p: &Path) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, ClientError> {
    let mut rd = BufReader::new(std::fs::File::open(p)?);
    rustls_pemfile::certs(&mut rd)
        .collect::<Result<_, _>>()
        .map_err(ClientError::from)
}

fn load_key(p: &Path) -> Result<rustls_pki_types::PrivateKeyDer<'static>, ClientError> {
    let mut rd = BufReader::new(std::fs::File::open(p)?);
    rustls_pemfile::private_key(&mut rd)?
        .ok_or_else(|| ClientError::Protocol("no private key in file".into()))
}
