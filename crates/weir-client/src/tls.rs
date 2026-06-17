//! Blocking mutual-TLS client connector (feature = "tls").
//!
//! Connects to a weir daemon's TCP+mTLS listener using the same ring
//! provider and rustls 0.23 API that the server side uses.

use std::{io, net::TcpStream, path::Path, sync::Arc, time::Duration};

use rustls_pki_types::ServerName;
use weir_core::Durability;

use crate::{ClientError, WeirClient};

/// TLS configuration for [`WeirClient::connect_tls`].
///
/// `Debug`/`Clone` are safe to derive: every field is a path, a borrowed `str`,
/// or a `Durability` — no key material, only filesystem paths (G16).
#[derive(Debug, Clone)]
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

        // Build rustls ClientConfig using the ring provider (matches the server).
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
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

    /// Sets the read timeout on the underlying TCP socket. `None` (the default)
    /// blocks indefinitely. Same opt-in availability rationale as the Unix
    /// client's [`set_read_timeout`][WeirClient::set_read_timeout] — a wedged
    /// daemon would otherwise block a producer forever in `read_response`.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.sock.set_read_timeout(timeout)
    }

    /// Sets the write timeout on the underlying TCP socket. `None` (the default)
    /// blocks indefinitely. See [`set_read_timeout`][Self::set_read_timeout].
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.sock.set_write_timeout(timeout)
    }
}

// ── PEM helpers ────────────────────────────────────────────────────────────────

fn load_certs(p: &Path) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, ClientError> {
    use rustls_pki_types::pem::PemObject;
    // Read the file ourselves so a missing/unreadable file stays a clean
    // ClientError::Io; parse the PEM via rustls-pki-types (S16 — off the
    // unmaintained rustls-pemfile).
    let pem = std::fs::read(p)?;
    rustls_pki_types::CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .map_err(|e| {
            ClientError::Protocol(format!("invalid certificate PEM in {}: {e}", p.display()))
        })
}

fn load_key(p: &Path) -> Result<rustls_pki_types::PrivateKeyDer<'static>, ClientError> {
    use rustls_pki_types::pem::PemObject;
    let pem = std::fs::read(p)?;
    rustls_pki_types::PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|e| ClientError::Protocol(format!("no private key in file {}: {e}", p.display())))
}

// ── Tests ────────────────────────────────────────────────────────────────────
//
// The full mTLS handshake round-trip (connect_tls against a live daemon,
// cert-rejection paths, the timeout setters on a real TlsStream) is covered by
// the `tls_listener` integration suite in weir-server/tests. These unit tests
// cover the PEM-loader error mapping the integration suite can't easily hit
// (G03).
#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_file(label: &str, contents: &[u8]) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("weir_tls_{label}_{}.pem", std::process::id()));
        std::fs::write(&p, contents).unwrap();
        p
    }

    #[test]
    fn load_key_missing_file_is_io_error() {
        let err = load_key(Path::new("/weir_no_such_key_xyzzy.pem")).unwrap_err();
        assert!(matches!(err, ClientError::Io(_)), "{err:?}");
    }

    #[test]
    fn load_certs_missing_file_is_io_error() {
        let err = load_certs(Path::new("/weir_no_such_cert_xyzzy.pem")).unwrap_err();
        assert!(matches!(err, ClientError::Io(_)), "{err:?}");
    }

    #[test]
    fn load_key_file_without_a_key_is_protocol_error() {
        // A readable file that contains no PEM private key → the None branch maps
        // to a clear Protocol error rather than a panic or a silent empty key.
        let p = tmp_file("nokey", b"not a pem private key\n");
        let err = load_key(&p).unwrap_err();
        match err {
            ClientError::Protocol(msg) => assert!(msg.contains("no private key"), "{msg}"),
            other => panic!("expected Protocol, got {other:?}"),
        }
        std::fs::remove_file(&p).ok();
    }
}
