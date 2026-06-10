//! TLS test fixtures: generate a throwaway CA, a server cert, and client certs
//! (valid, wrong-CA, expired) for the TCP+mTLS integration tests.

use std::{io::Write, path::PathBuf};

/// A bundle of PEM material written to temp files, with paths the daemon and
/// client can read. Files live under a process-temp subdir keyed by `tag`.
pub struct TlsFixture {
    pub ca_cert_path: PathBuf,
    pub server_cert_path: PathBuf,
    pub server_key_path: PathBuf,
    pub client_cert_path: PathBuf,
    pub client_key_path: PathBuf,
    pub wrong_ca_client_cert_path: PathBuf,
    pub wrong_ca_client_key_path: PathBuf,
    pub expired_client_cert_path: PathBuf,
    pub expired_client_key_path: PathBuf,
}

fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> PathBuf {
    let p = dir.join(name);
    let mut f = std::fs::File::create(&p).unwrap();
    f.write_all(pem.as_bytes()).unwrap();
    p
}

struct Ca {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

fn make_ca() -> Ca {
    let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Ca { cert, key }
}

fn leaf(ca: &Ca, cn: &str, expired: bool) -> (String, String) {
    let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
    if expired {
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = rcgen::date_time_ymd(2000, 1, 2);
    }
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
    (cert.pem(), key.serialize_pem())
}

impl TlsFixture {
    /// `tag` keys the temp subdir so parallel tests don't collide.
    pub fn generate(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("weir-tls-fixture-{tag}"));
        std::fs::create_dir_all(&dir).unwrap();

        let ca = make_ca();
        let other_ca = make_ca();

        let (server_cert, server_key) = leaf(&ca, "weir-server", false);
        let (client_cert, client_key) = leaf(&ca, "weir-client", false);
        let (wrong_cert, wrong_key) = leaf(&other_ca, "evil-client", false);
        let (exp_cert, exp_key) = leaf(&ca, "stale-client", true);

        Self {
            ca_cert_path: write_pem(&dir, "ca.pem", &ca.cert.pem()),
            server_cert_path: write_pem(&dir, "server.pem", &server_cert),
            server_key_path: write_pem(&dir, "server.key", &server_key),
            client_cert_path: write_pem(&dir, "client.pem", &client_cert),
            client_key_path: write_pem(&dir, "client.key", &client_key),
            wrong_ca_client_cert_path: write_pem(&dir, "wrong.pem", &wrong_cert),
            wrong_ca_client_key_path: write_pem(&dir, "wrong.key", &wrong_key),
            expired_client_cert_path: write_pem(&dir, "expired.pem", &exp_cert),
            expired_client_key_path: write_pem(&dir, "expired.key", &exp_key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_generates_all_pem_files() {
        let fx = TlsFixture::generate("smoke");
        for p in [
            &fx.ca_cert_path,
            &fx.server_cert_path,
            &fx.server_key_path,
            &fx.client_cert_path,
            &fx.client_key_path,
            &fx.wrong_ca_client_cert_path,
            &fx.wrong_ca_client_key_path,
            &fx.expired_client_cert_path,
            &fx.expired_client_key_path,
        ] {
            assert!(p.exists(), "missing {}", p.display());
        }
        assert!(std::fs::metadata(&fx.ca_cert_path).unwrap().len() > 0);
    }
}
