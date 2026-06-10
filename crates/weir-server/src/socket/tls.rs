//! TLS server config for the TCP listener (feature = "tls").
//!
//! Builds a rustls `ServerConfig` that REQUIRES a client certificate signed by
//! the configured CA (mutual TLS). The config is held behind an `ArcSwap` so a
//! SIGHUP can hot-swap freshly-rotated certs without dropping connections — the
//! TCP accept loop reads the current `Arc<ServerConfig>` at each accept.
//!
//! Crypto provider: aws-lc-rs, explicitly, to match the rest of the workspace.

// This module's public surface is consumed by the TCP accept loop (Task 7) and
// SIGHUP handler (Task 8), neither of which is wired up yet.  Suppress the
// dead-code lint for now so clippy stays green during the incremental rollout.
#![allow(dead_code)]

use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Errors that can occur while loading TLS certificates or building the `ServerConfig`.
#[derive(Debug)]
pub enum TlsConfigError {
    Io {
        path: String,
        source: std::io::Error,
    },
    NoCertsFound(String),
    NoPrivateKey(String),
    EmptyCaStore(String),
    Verifier(String),
    Rustls(rustls::Error),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "reading '{path}': {source}"),
            Self::NoCertsFound(p) => write!(f, "no certificates found in '{p}'"),
            Self::NoPrivateKey(p) => write!(f, "no private key found in '{p}'"),
            Self::EmptyCaStore(p) => write!(f, "CA file '{p}' yielded no usable roots"),
            Self::Verifier(e) => write!(f, "building client-cert verifier: {e}"),
            Self::Rustls(e) => write!(f, "rustls: {e}"),
        }
    }
}

impl std::error::Error for TlsConfigError {}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let p = path.display().to_string();
    let file = File::open(path).map_err(|source| TlsConfigError::Io {
        path: p.clone(),
        source,
    })?;
    let mut rd = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut rd)
        .collect::<Result<_, _>>()
        .map_err(|source| TlsConfigError::Io {
            path: p.clone(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsConfigError::NoCertsFound(p));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let p = path.display().to_string();
    let file = File::open(path).map_err(|source| TlsConfigError::Io {
        path: p.clone(),
        source,
    })?;
    let mut rd = BufReader::new(file);
    rustls_pemfile::private_key(&mut rd)
        .map_err(|source| TlsConfigError::Io {
            path: p.clone(),
            source,
        })?
        .ok_or(TlsConfigError::NoPrivateKey(p))
}

/// Build a rustls `ServerConfig` that requires mutual TLS.
///
/// The server presents `cert_path` / `key_path` to the client, and the client
/// MUST present a certificate signed by the CA in `client_ca_path`. The
/// aws-lc-rs crypto provider is used explicitly, matching the workspace
/// convention.
pub fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: &Path,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let mut roots = RootCertStore::empty();
    for ca in load_certs(client_ca_path)? {
        roots
            .add(ca)
            .map_err(|e| TlsConfigError::Verifier(e.to_string()))?;
    }
    if roots.is_empty() {
        return Err(TlsConfigError::EmptyCaStore(
            client_ca_path.display().to_string(),
        ));
    }

    // `builder_with_provider` requires the client cert verifier to use the same
    // provider as the server config.  Both calls below pass the aws-lc-rs
    // provider explicitly; neither relies on any global default.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    // `WebPkiClientVerifier::builder_with_provider(...).build()` produces a
    // verifier with the default (Deny) anonymous policy, meaning a client
    // certificate is REQUIRED — anonymous clients are rejected.
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| TlsConfigError::Verifier(e.to_string()))?;

    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsConfigError::Rustls)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(TlsConfigError::Rustls)?;

    Ok(Arc::new(config))
}

/// A `ServerConfig` that can be atomically hot-reloaded (e.g. on SIGHUP) by
/// re-reading the cert/key/CA files and storing the new `Arc<ServerConfig>`
/// into the inner `ArcSwap`.
///
/// The TCP accept loop calls `current()` at each accept; in-flight TLS
/// handshakes that already took their `Arc<ServerConfig>` reference are
/// unaffected by a concurrent `reload()`.
#[derive(Clone)]
pub struct ReloadableServerConfig {
    inner: Arc<ArcSwap<ServerConfig>>,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    client_ca_path: std::path::PathBuf,
}

impl ReloadableServerConfig {
    /// Load the initial config from the given paths.
    pub fn load(
        cert_path: std::path::PathBuf,
        key_path: std::path::PathBuf,
        client_ca_path: std::path::PathBuf,
    ) -> Result<Self, TlsConfigError> {
        let cfg = build_server_config(&cert_path, &key_path, &client_ca_path)?;
        Ok(Self {
            inner: Arc::new(ArcSwap::from(cfg)),
            cert_path,
            key_path,
            client_ca_path,
        })
    }

    /// Return the current `Arc<ServerConfig>`.  Cheap — no allocation.
    pub fn current(&self) -> Arc<ServerConfig> {
        self.inner.load_full()
    }

    /// Atomically swap in a freshly-built `ServerConfig` from the same paths.
    ///
    /// Fails without touching the running config if cert/key/CA cannot be
    /// re-parsed (fail-safe: the old config continues to be used).
    pub fn reload(&self) -> Result<(), TlsConfigError> {
        let cfg = build_server_config(&self.cert_path, &self.key_path, &self.client_ca_path)?;
        self.inner.store(cfg);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gen_ca_and_leaf() -> (String, String, String) {
        let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let leaf_params = rcgen::CertificateParams::new(vec!["weir-test".to_string()]).unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        (ca_cert.pem(), leaf.pem(), leaf_key.serialize_pem())
    }

    fn write_tmp(contents: &str, suffix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let name = format!(
            "weir-tls-test-{:x}-{suffix}",
            crc32fast::hash(contents.as_bytes())
        );
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn builds_with_valid_cert_key_ca() {
        let (ca, cert, key) = gen_ca_and_leaf();
        let ca_p = write_tmp(&ca, "ca.pem");
        let cert_p = write_tmp(&cert, "cert.pem");
        let key_p = write_tmp(&key, "key.pem");
        assert!(build_server_config(&cert_p, &key_p, &ca_p).is_ok());
    }

    #[test]
    fn rejects_missing_ca_file() {
        let (_ca, cert, key) = gen_ca_and_leaf();
        let cert_p = write_tmp(&cert, "cert2.pem");
        let key_p = write_tmp(&key, "key2.pem");
        let missing = std::path::Path::new("/nonexistent/ca.pem");
        assert!(matches!(
            build_server_config(&cert_p, &key_p, missing),
            Err(TlsConfigError::Io { .. })
        ));
    }

    #[test]
    fn rejects_key_cert_mismatch() {
        let (ca, cert, _key) = gen_ca_and_leaf();
        let (_ca2, _cert2, key2) = gen_ca_and_leaf();
        let ca_p = write_tmp(&ca, "ca3.pem");
        let cert_p = write_tmp(&cert, "cert3.pem");
        let key_p = write_tmp(&key2, "key3.pem");
        // rustls 0.23: with_single_cert calls CertifiedKey::from_der which
        // validates SubjectPublicKeyInfo matches.  A mismatched key/cert pair
        // is caught at build time and surfaces as TlsConfigError::Rustls.
        assert!(matches!(
            build_server_config(&cert_p, &key_p, &ca_p),
            Err(TlsConfigError::Rustls(_))
        ));
    }

    #[test]
    fn rejects_empty_ca() {
        let (_ca, cert, key) = gen_ca_and_leaf();
        let cert_p = write_tmp(&cert, "cert4.pem");
        let key_p = write_tmp(&key, "key4.pem");
        let empty_ca = write_tmp("# no certs here\n", "ca4.pem");
        assert!(matches!(
            build_server_config(&cert_p, &key_p, &empty_ca),
            Err(TlsConfigError::NoCertsFound(_))
        ));
    }

    #[test]
    fn reload_swaps_config() {
        let (ca, cert, key) = gen_ca_and_leaf();
        let ca_p = write_tmp(&ca, "ca5.pem");
        let cert_p = write_tmp(&cert, "cert5.pem");
        let key_p = write_tmp(&key, "key5.pem");
        let r = ReloadableServerConfig::load(cert_p, key_p, ca_p).unwrap();
        let before = r.current();
        assert!(r.reload().is_ok());
        let after = r.current();
        assert!(!Arc::ptr_eq(&before, &after));
    }
}
