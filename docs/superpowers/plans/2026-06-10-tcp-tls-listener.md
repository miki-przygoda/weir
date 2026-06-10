# TCP + mTLS Listener Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a TCP listener to `weir-server` that accepts remote producers over mutual TLS (CA-signed client certs), running concurrently with the existing Unix socket, plus a TLS client connector and per-connection durability default in `weir-client`.

**Architecture:** One shared, transport-generic connection handler (`handle_connection<S>`) is fed by two accept loops — the existing Unix loop and a new TCP+TLS loop — both pushing to the same work queue. TLS is rustls 0.23 with the **aws-lc-rs** provider (already the workspace crypto stack); the server `ServerConfig` lives behind an `ArcSwap` so SIGHUP can hot-reload certs without dropping connections. All new TCP/TLS code is gated behind a default-off `tls` cargo feature, so the default build stays Unix-only and unchanged.

**Tech Stack:** Rust 2024, tokio, rustls 0.23 (+ tokio-rustls, rustls-pemfile, rustls-pki-types, aws-lc-rs provider), arc-swap, x509-parser (CN extraction), rcgen (test cert generation), prometheus-client.

**Reference spec:** `docs/superpowers/specs/2026-06-10-tcp-tls-listener-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `crates/weir-server/src/socket/connection.rs` | Modify | Make `handle_connection` + `send_ack`/`send_nack` + internal helpers generic over the stream type |
| `crates/weir-server/src/socket/tls.rs` | Create | Build & hot-reload the rustls `ServerConfig` (server identity + CA client verifier) |
| `crates/weir-server/src/socket/tcp.rs` | Create | TCP accept loop: bind, TLS handshake under timeout, hand `TlsStream` to the shared handler |
| `crates/weir-server/src/socket/mod.rs` | Modify | Export `tls`/`tcp` (feature-gated); Unix loop unchanged |
| `crates/weir-server/src/config/mod.rs` | Modify | New config fields (`tcp_bind`, `tls_*`) + validation |
| `crates/weir-server/src/config/{cli,env,file}.rs` | Modify | Parse the new keys from CLI/env/TOML |
| `crates/weir-server/src/metrics/mod.rs` | Modify | `weir_tls_handshake_failures_total`, `weir_tls_config_reloads_total` |
| `crates/weir-server/src/main.rs` | Modify | Spawn the TCP loop + SIGHUP reload task alongside the Unix loop (feature-gated) |
| `crates/weir-server/Cargo.toml` | Modify | `tls` feature + gated deps + `rcgen` dev-dep |
| `crates/weir-testkit/src/tls.rs` | Create | Test cert fixtures (rcgen) + `weir_server_tls()` harness helper |
| `crates/weir-testkit/Cargo.toml` | Modify | `rcgen` dep (testkit is a dev/test crate) |
| `crates/weir-client/src/lib.rs` | Modify | Generic `WeirClient<S>`, re-export TLS connector |
| `crates/weir-client/src/unix.rs` | Modify | Generalize struct + methods over `Read + Write`; add `default_durability` / `push_default` |
| `crates/weir-client/src/tls.rs` | Create | `connect_tls` + `ClientTlsConfig` (blocking rustls client) |
| `crates/weir-client/Cargo.toml` | Modify | `tls` feature + gated deps |
| `crates/weir-client/examples/push_tls.rs` | Create | Worked mTLS push example |
| `docs/operations/configuration.md` | Modify | Document the new keys |
| `docs/operations/tcp-mtls.md` | Create | Operator guide: cert setup, rotation, SIGHUP |
| `docs/security/threat-model.md` | Modify | Note the network/TLS path |
| `CHANGELOG.md` | Modify | Unreleased entry |

**Implementation order (each task = working, testable software):**
1. Generic handler refactor (no behaviour change; existing tests are the guard)
2. `tls` feature + deps
3. Metrics
4. `tls.rs` config builder + reload wrapper
5. Config surface
6. testkit TLS harness
7. `tcp.rs` accept loop + integration tests
8. SIGHUP reload
9. Wire into `main.rs`
10. Client TLS connector + durability default + example
11. Docs + CHANGELOG

---

## Task 1: Generic stream handler refactor

Make the connection handler transport-agnostic. **No behavioural change** — the existing connection tests (which use `UnixStream::pair()`) are the regression guard, and we add one new test proving the handler works over a non-Unix in-memory stream.

**Files:**
- Modify: `crates/weir-server/src/socket/connection.rs`
- Test: `crates/weir-server/src/socket/connection.rs` (existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Read the current signatures**

Run: `grep -n "UnixStream" crates/weir-server/src/socket/connection.rs`
Expected: occurrences in `use` (line ~5), `handle_connection` (line ~76), an internal helper (line ~266), `send_nack` (line ~393), `send_ack` (line ~432), plus the `#[cfg(test)] mod tests`. The test-module occurrences stay as-is (they construct concrete `UnixStream::pair()`).

- [ ] **Step 2: Write the failing test (generic transport proof)**

Add to the `#[cfg(test)] mod tests` block in `connection.rs`. This drives a full Push→Ack over an in-memory `tokio::io::duplex` pipe (NOT a `UnixStream`), proving the handler is transport-generic.

```rust
#[tokio::test]
async fn handle_connection_works_over_non_unix_stream() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use weir_core::{Durability, Envelope, Header, MessageType, HEADER_LEN};

    // In-memory duplex stands in for any AsyncRead+AsyncWrite transport
    // (e.g. a TlsStream). 64 KiB buffer is ample for one small frame.
    let (mut client, server) = tokio::io::duplex(64 * 1024);

    let (queue_tx, queue_rx) = crate::queue::bounded(16, 1);
    let metrics = std::sync::Arc::new(crate::metrics::Metrics::new());
    let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let cfg = ConnectionConfig {
        max_payload_bytes: 1024,
        read_timeout: std::time::Duration::from_secs(5),
        ack_timeout: std::time::Duration::from_millis(500),
        shard_id: 0,
    };

    // Drain the queue so the push gets acked.
    std::thread::spawn(move || {
        if let Ok(unit) = queue_rx.recv() {
            unit.ack(crate::models::AckOutcome::Durable);
        }
    });

    let handle = tokio::spawn(handle_connection(server, queue_tx, cfg, metrics, sd_rx));

    let header = Header::new(MessageType::Push, Durability::Buffered, 0, 5);
    let frame = Envelope::new(header, b"hello".to_vec()).encode();
    client.write_all(&frame).await.unwrap();

    // Read the Ack header back.
    let mut resp = [0u8; HEADER_LEN];
    client.read_exact(&mut resp).await.unwrap();
    let resp_header = Header::decode(&resp).unwrap();
    assert_eq!(resp_header.message_type, MessageType::Ack);

    drop(client); // EOF → handler returns
    handle.await.unwrap().unwrap();
}
```

> **NOTE for the executor:** the exact constructors (`crate::queue::bounded`, `unit.ack(...)`, `AckOutcome`, `Metrics::new`) must match the real APIs. Before writing the test, confirm them: `grep -n "pub fn bounded\|pub fn ack\|pub enum AckOutcome\|impl Metrics" crates/weir-server/src/queue.rs crates/weir-server/src/models.rs crates/weir-server/src/metrics/mod.rs`. Adapt the helper calls in the test to the real signatures (the existing tests in this module already show the correct idiom — mirror them).

- [ ] **Step 3: Run the test to verify it fails to compile**

Run: `cargo test -p weir-server --lib socket::connection::tests::handle_connection_works_over_non_unix_stream`
Expected: COMPILE ERROR — `handle_connection` expects `UnixStream`, got `tokio::io::DuplexStream`.

- [ ] **Step 4: Generalize the handler signatures**

In `connection.rs`:

1. Remove `UnixStream` from the top-level `use tokio::net::UnixStream;` (the test module re-imports its own).
2. Add the trait imports near the top: `use tokio::io::{AsyncRead, AsyncWrite};` (keep existing `AsyncReadExt`/`AsyncWriteExt` usage).
3. Change `handle_connection`:
   ```rust
   pub async fn handle_connection<S>(
       stream: S,
       queue_tx: QueueSender<WorkUnit>,
       config: ConnectionConfig,
       metrics: Arc<Metrics>,
       mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
   ) -> io::Result<()>
   where
       S: AsyncRead + AsyncWrite + Unpin + Send,
   {
   ```
   The body is unchanged: `tokio::io::BufReader::new(stream)` requires `S: AsyncRead`; writes go through `stream.get_mut()` which yields `&mut S` (needs `S: AsyncWrite`).
4. Change `send_ack` and `send_nack` and the internal helper at ~line 266 from `stream: &mut UnixStream` to a generic `&mut S`:
   ```rust
   async fn send_ack<S: AsyncWrite + Unpin>(stream: &mut S) -> io::Result<()> { /* unchanged body */ }
   async fn send_nack<S: AsyncWrite + Unpin>(stream: &mut S, reason: WireNack, extra: &[u8]) -> io::Result<()> { /* unchanged body */ }
   ```
   For the ~line 266 helper: apply the same `<S: AsyncRead + AsyncWrite + Unpin>` bound matching how it uses the stream (read and/or write).

- [ ] **Step 5: Run the new test + the full existing connection suite**

Run: `cargo test -p weir-server --lib socket::connection`
Expected: PASS — the new generic test passes AND every pre-existing `socket::connection::tests::*` still passes unchanged (the regression guard).

- [ ] **Step 6: Confirm the accept-loop callsite still compiles**

The Unix loop in `socket/mod.rs:184` calls `handle_connection(stream, ...)` with a concrete `UnixStream` — type inference picks `S = UnixStream`, no change needed.
Run: `cargo build -p weir-server`
Expected: clean build.

- [ ] **Step 7: Commit**

```bash
git add crates/weir-server/src/socket/connection.rs
git commit -m "refactor(socket): make handle_connection generic over the stream type"
```

---

## Task 2: `tls` feature flag + dependencies

**Files:**
- Modify: `crates/weir-server/Cargo.toml`

- [ ] **Step 1: Add the feature and gated deps**

In `crates/weir-server/Cargo.toml`, add a `[features]` table (none exists yet) and the optional deps. `rustls` is already a direct dep — reuse it.

```toml
[features]
default = []
# TCP listener with mutual TLS. Off by default: weir-server stays Unix-only
# unless explicitly built with --features tls.
tls = ["dep:tokio-rustls", "dep:rustls-pemfile", "dep:rustls-pki-types", "dep:arc-swap", "dep:x509-parser"]
```

Add to `[dependencies]` (the `rustls = "0.23"` line already exists and stays):

```toml
# TLS listener (feature = "tls"). tokio-rustls 0.26 tracks rustls 0.23.
# Pinned to the aws-lc-rs crypto provider already used across the workspace.
tokio-rustls = { version = "0.26", default-features = false, features = ["aws-lc-rs", "tls12"], optional = true }
rustls-pemfile = { version = "2", optional = true }
rustls-pki-types = { version = "1", optional = true }
# Hot-reloadable ServerConfig (SIGHUP cert rotation without dropping conns).
arc-swap = { version = "1", optional = true }
# Client-cert CN extraction for tracing spans.
x509-parser = { version = "0.16", optional = true }
```

Add to `[dev-dependencies]`:

```toml
# Generates throwaway CA / server / client certs for the TLS integration tests.
rcgen = "0.13"
```

- [ ] **Step 2: Verify the default build is unchanged**

Run: `cargo build -p weir-server`
Expected: builds; none of the new deps are compiled (they're `optional` and `tls` is off).

- [ ] **Step 3: Verify the feature resolves**

Run: `cargo build -p weir-server --features tls`
Expected: builds (the new crates compile; no code uses them yet, so no warnings beyond unused — acceptable at this step since modules land in later tasks).

> **NOTE:** if step 3 emits `unused crate dependency` warnings under `-D warnings`, that's expected until Tasks 4/5 use them. Do not add `#[allow]`; the warnings clear once the modules exist. If CI runs `-D warnings` on `--features tls`, land Tasks 4–5 before pushing.

- [ ] **Step 4: Commit**

```bash
git add crates/weir-server/Cargo.toml
git commit -m "build(server): add default-off tls feature + gated deps"
```

---

## Task 3: TLS metrics

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs`
- Test: `crates/weir-server/src/metrics/mod.rs` (existing test module — there is a test asserting the exposed metric set; update it)

- [ ] **Step 1: Inspect the metrics struct + label pattern**

Run: `grep -n "Family\|struct Metrics\|register\|EncodeLabelSet\|fn new" crates/weir-server/src/metrics/mod.rs | head -40`
Confirm the existing pattern: counters are `prometheus_client::metrics::family::Family<Label, Counter>` registered in `Metrics::new`, with `#[derive(EncodeLabelSet)]` label structs (the `NackLabel`/`TierValue` types already in the codebase are the template).

- [ ] **Step 2: Add the label enums + fields**

Add near the other label types:

```rust
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TlsHandshakeFailureLabel {
    pub reason: TlsHandshakeFailureReason,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
#[allow(non_camel_case_types)]
pub enum TlsHandshakeFailureReason {
    no_client_cert,
    bad_cert,
    timeout,
    other,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TlsReloadLabel {
    pub outcome: TlsReloadOutcome,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
#[allow(non_camel_case_types)]
pub enum TlsReloadOutcome { ok, failed }
```

Add fields to `struct Metrics`:

```rust
pub tls_handshake_failures: Family<TlsHandshakeFailureLabel, Counter>,
pub tls_config_reloads: Family<TlsReloadLabel, Counter>,
```

Register them in `Metrics::new` (mirror the existing `records_nack` registration), with help text:
- `weir_tls_handshake_failures_total` — "TLS handshakes rejected, by reason"
- `weir_tls_config_reloads_total` — "SIGHUP TLS config reloads, by outcome"

> These are always compiled (not `tls`-gated) so the metric surface is stable across build variants — they simply stay at zero without the feature. This matches the project's "metric surface is the public contract" stance.

- [ ] **Step 3: Update the metric-set test**

Find the test that pins the exposed metric names (search `grep -n "weir_records_accepted\|metric.*names\|expected_metrics" crates/weir-server/src/metrics/mod.rs`). Add `weir_tls_handshake_failures_total` and `weir_tls_config_reloads_total` to its expected set.

- [ ] **Step 4: Run metrics tests**

Run: `cargo test -p weir-server --lib metrics`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/metrics/mod.rs
git commit -m "feat(metrics): add tls handshake-failure + config-reload counters"
```

---

## Task 4: TLS server config builder + reload wrapper (`tls.rs`)

**Files:**
- Create: `crates/weir-server/src/socket/tls.rs`
- Modify: `crates/weir-server/src/socket/mod.rs` (add `#[cfg(feature = "tls")] pub mod tls;`)

- [ ] **Step 1: Register the module (feature-gated)**

In `socket/mod.rs`, near the existing `mod connection; mod peer;`:

```rust
#[cfg(feature = "tls")]
pub mod tls;
```

- [ ] **Step 2: Write `tls.rs` with the builder + reload wrapper**

```rust
//! TLS server config for the TCP listener (feature = "tls").
//!
//! Builds a rustls `ServerConfig` that REQUIRES a client certificate signed by
//! the configured CA (mutual TLS). The config is held behind an `ArcSwap` so a
//! SIGHUP can hot-swap freshly-rotated certs without dropping connections — the
//! TCP accept loop reads the current `Arc<ServerConfig>` at each accept.
//!
//! Crypto provider: aws-lc-rs, explicitly, to match the rest of the workspace
//! (reqwest / mysql_async / tokio-postgres-rustls all use it). Building configs
//! with an explicit provider avoids the "could not determine CryptoProvider"
//! ambiguity that bit the postgres sink.

use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Errors building the TLS server config. All are fatal at startup; on SIGHUP
/// reload they are non-fatal (the previous config is retained).
#[derive(Debug)]
pub enum TlsConfigError {
    Io { path: String, source: std::io::Error },
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
    let file = File::open(path).map_err(|source| TlsConfigError::Io { path: p.clone(), source })?;
    let mut rd = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut rd)
        .collect::<Result<_, _>>()
        .map_err(|source| TlsConfigError::Io { path: p.clone(), source })?;
    if certs.is_empty() {
        return Err(TlsConfigError::NoCertsFound(p));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let p = path.display().to_string();
    let file = File::open(path).map_err(|source| TlsConfigError::Io { path: p.clone(), source })?;
    let mut rd = BufReader::new(file);
    rustls_pemfile::private_key(&mut rd)
        .map_err(|source| TlsConfigError::Io { path: p.clone(), source })?
        .ok_or(TlsConfigError::NoPrivateKey(p))
}

/// Builds the mTLS `ServerConfig` from the three PEM files. Requires a
/// CA-signed client certificate on every handshake.
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

    let verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .map_err(|e| TlsConfigError::Verifier(e.to_string()))?;

    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(TlsConfigError::Rustls)?
    .with_client_cert_verifier(verifier)
    .with_single_cert(certs, key)
    .map_err(TlsConfigError::Rustls)?;

    Ok(Arc::new(config))
}

/// Hot-reloadable handle around the `ServerConfig`. Cloneable (`Arc` inside);
/// the accept loop holds one clone, the SIGHUP task another.
#[derive(Clone)]
pub struct ReloadableServerConfig {
    inner: Arc<ArcSwap<ServerConfig>>,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    client_ca_path: std::path::PathBuf,
}

impl ReloadableServerConfig {
    /// Initial load. Fatal error propagates (daemon refuses to start with a
    /// broken TLS config).
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

    /// Current config — read once per accepted connection.
    pub fn current(&self) -> Arc<ServerConfig> {
        self.inner.load_full()
    }

    /// Re-read the same paths and atomically swap on success. On failure the
    /// previous config is retained and the error is returned (caller logs +
    /// increments the failed-reload metric). Never panics, never tears down.
    pub fn reload(&self) -> Result<(), TlsConfigError> {
        let cfg = build_server_config(&self.cert_path, &self.key_path, &self.client_ca_path)?;
        self.inner.store(cfg);
        Ok(())
    }
}
```

- [ ] **Step 3: Write the unit tests (rcgen fixtures)**

Append a `#[cfg(test)] mod tests` to `tls.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Generates a self-signed CA + a leaf cert signed by it. Returns
    // (ca_pem, leaf_cert_pem, leaf_key_pem).
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
        // Unique-ish name without Math.random: use the contents hash + suffix.
        let name = format!("weir-tls-test-{:x}-{suffix}", crc32fast::hash(contents.as_bytes()));
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
        let (_ca2, _cert2, key2) = gen_ca_and_leaf(); // unrelated key
        let ca_p = write_tmp(&ca, "ca3.pem");
        let cert_p = write_tmp(&cert, "cert3.pem");
        let key_p = write_tmp(&key2, "key3.pem");
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
        // New Arc instance after reload (store replaced the pointer).
        assert!(!Arc::ptr_eq(&before, &after));
    }
}
```

> **NOTE:** `crc32fast` is already a dep of weir-server, so `write_tmp` can use it. Confirm the rcgen 0.13 API names with `cargo doc -p rcgen --open` if any call fails — the 0.13 surface is `CertificateParams::new`, `KeyPair::generate`, `params.self_signed(&key)`, `params.signed_by(&key, &issuer_cert, &issuer_key)`, `cert.pem()`, `key.serialize_pem()`.

- [ ] **Step 4: Run the tls.rs tests**

Run: `cargo test -p weir-server --features tls --lib socket::tls`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/socket/tls.rs crates/weir-server/src/socket/mod.rs
git commit -m "feat(tls): mTLS ServerConfig builder + hot-reloadable handle"
```

---

## Task 5: Config surface

**Files:**
- Modify: `crates/weir-server/src/config/mod.rs`
- Modify: `crates/weir-server/src/config/cli.rs`
- Modify: `crates/weir-server/src/config/env.rs`
- Modify: `crates/weir-server/src/config/file.rs`
- Test: `crates/weir-server/src/config/mod.rs` (existing test module)

- [ ] **Step 1: Add fields to `PartialConfig`**

In `config/mod.rs`, in `PartialConfig` (after `peer_uid_check`):

```rust
    pub tcp_bind: Option<String>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_client_ca_path: Option<PathBuf>,
    pub tls_handshake_timeout_secs: Option<u64>,
```

- [ ] **Step 2: Add fields to the final `Config`**

Add `use std::net::SocketAddr;` if not present. In `struct Config` (after `peer_uid_check`):

```rust
    /// TCP listen address for the mTLS listener (e.g. `0.0.0.0:7100`).
    /// `None` ⇒ no TCP listener; Unix-only. When `Some`, the three `tls_*`
    /// paths are required and the binary must be built with `--features tls`.
    pub tcp_bind: Option<SocketAddr>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_client_ca_path: Option<PathBuf>,
    pub tls_handshake_timeout_secs: u64,
```

- [ ] **Step 3: Merge + validate in `from_layers`**

Find the `merge!` macro usage block and the validation section in `from_layers` (the function `Self::from_layers`). Add, mirroring the existing `merge!` + `validate_path_format` idioms:

```rust
    // ── TCP + TLS ────────────────────────────────────────────────────────────
    let tcp_bind_str = merge!(tcp_bind);
    let tls_cert_path = merge!(tls_cert_path);
    let tls_key_path = merge!(tls_key_path);
    let tls_client_ca_path = merge!(tls_client_ca_path);
    let tls_handshake_timeout_secs = merge!(tls_handshake_timeout_secs).unwrap_or(10);

    let tcp_bind = match tcp_bind_str {
        None => None,
        Some(s) => {
            let addr = s.parse::<SocketAddr>().map_err(|_| ConfigError::Invalid {
                field: "tcp_bind",
                message: format!(
                    "'{s}' is not a valid socket address (expected e.g. '0.0.0.0:7100' or '[::]:7100')"
                ),
            })?;
            // TLS is mandatory on the TCP path — never plaintext.
            if !cfg!(feature = "tls") {
                return Err(ConfigError::Invalid {
                    field: "tcp_bind",
                    message:
                        "tcp_bind is set but this binary was built without the 'tls' feature; \
                         rebuild with --features tls (plaintext TCP is never exposed)"
                            .to_string(),
                });
            }
            for (name, opt) in [
                ("tls_cert_path", &tls_cert_path),
                ("tls_key_path", &tls_key_path),
                ("tls_client_ca_path", &tls_client_ca_path),
            ] {
                let path = opt.as_ref().ok_or(ConfigError::Invalid {
                    field: name,
                    message: format!("{name} is required when tcp_bind is set"),
                })?;
                if !path.exists() {
                    return Err(ConfigError::Invalid {
                        field: name,
                        message: format!("{name} '{}' does not exist", path.display()),
                    });
                }
            }
            Some(addr)
        }
    };
```

Then add the five fields to the `Config { ... }` constructor near the end of `from_layers`:

```rust
            tcp_bind,
            tls_cert_path,
            tls_key_path,
            tls_client_ca_path,
            tls_handshake_timeout_secs,
```

> **NOTE:** match the exact `ConfigError` variant + field shape already in this file. `grep -n "enum ConfigError\|Invalid {" crates/weir-server/src/config/mod.rs` first and adapt (the file uses `field: &'static str` per the earlier `metrics_bind` example).

- [ ] **Step 4: Write failing validation tests**

Add to the config test module:

```rust
#[test]
fn tcp_bind_without_tls_paths_is_rejected() {
    let mut cli = PartialConfig::empty();
    cli.tcp_bind = Some("127.0.0.1:7100".to_string());
    let err = Config::from_layers(cli, PartialConfig::empty(), PartialConfig::empty())
        .expect_err("tcp_bind without certs must fail");
    assert!(err.to_string().contains("tls_cert_path"), "{err}");
}

#[test]
fn tcp_bind_invalid_addr_is_rejected() {
    let mut cli = PartialConfig::empty();
    cli.tcp_bind = Some("not-an-addr".to_string());
    let err = Config::from_layers(cli, PartialConfig::empty(), PartialConfig::empty())
        .expect_err("bad addr must fail");
    assert!(err.to_string().contains("tcp_bind"), "{err}");
}

#[test]
fn no_tcp_bind_defaults_to_unix_only() {
    let cfg = Config::from_layers(
        PartialConfig::empty(),
        PartialConfig::empty(),
        minimal_valid_partial(), // existing test helper that supplies required fields
    )
    .unwrap();
    assert!(cfg.tcp_bind.is_none());
    assert_eq!(cfg.tls_handshake_timeout_secs, 10);
}
```

> **NOTE:** `minimal_valid_partial()` — reuse whatever the existing config tests use to satisfy required fields (`grep -n "fn .*Partial\|PartialConfig {" crates/weir-server/src/config/mod.rs` in the test module). The first two tests should run even with the `tls` feature OFF (the addr-parse + required-path checks fire before the feature check for the invalid-addr case; the no-tls-paths case under `--features tls` hits the path check). Gate `tcp_bind_without_tls_paths_is_rejected` with `#[cfg(feature = "tls")]` since without the feature the earlier feature-check path triggers a different message — assert on the feature-off message in a separate `#[cfg(not(feature = "tls"))]` test.

- [ ] **Step 5: Add CLI flags (`cli.rs`)**

In `config/cli.rs`, mirror an existing `pico-args` optional flag (e.g. how `--socket-path` / `--metrics-bind` are parsed). Add:

```rust
    tcp_bind: args.opt_value_from_str("--tcp-bind")?,
    tls_cert_path: args.opt_value_from_str("--tls-cert")?,
    tls_key_path: args.opt_value_from_str("--tls-key")?,
    tls_client_ca_path: args.opt_value_from_str("--tls-client-ca")?,
    tls_handshake_timeout_secs: args.opt_value_from_str("--tls-handshake-timeout-secs")?,
```

Add the five flags to the `--help` text block in this file.

- [ ] **Step 6: Add env vars (`env.rs`)**

In `config/env.rs`, mirror the existing `WEIR_*` reads:

```rust
    tcp_bind: var("WEIR_TCP_BIND"),
    tls_cert_path: var("WEIR_TLS_CERT").map(PathBuf::from),
    tls_key_path: var("WEIR_TLS_KEY").map(PathBuf::from),
    tls_client_ca_path: var("WEIR_TLS_CLIENT_CA").map(PathBuf::from),
    tls_handshake_timeout_secs: var("WEIR_TLS_HANDSHAKE_TIMEOUT_SECS").and_then(|s| s.parse().ok()),
```

> Match the exact helper (`var`/parse) shape already in `env.rs`.

- [ ] **Step 7: Add TOML keys (`file.rs`)**

In `config/file.rs`, add the five fields to the serde struct that maps the TOML file (mirror existing `socket_path`/`metrics_bind` fields), with `#[serde(default)]` so they're optional.

- [ ] **Step 8: Run config tests + full build**

Run: `cargo test -p weir-server --lib config` then `cargo test -p weir-server --features tls --lib config`
Expected: PASS in both. Also `cargo build -p weir-server` (feature off) stays green.

- [ ] **Step 9: Commit**

```bash
git add crates/weir-server/src/config/
git commit -m "feat(config): tcp_bind + tls_* keys with mandatory-TLS validation"
```

---

## Task 6: testkit TLS harness

**Files:**
- Create: `crates/weir-testkit/src/tls.rs`
- Modify: `crates/weir-testkit/src/lib.rs` (export it)
- Modify: `crates/weir-testkit/Cargo.toml` (add `rcgen`)

- [ ] **Step 1: Add rcgen to testkit**

In `crates/weir-testkit/Cargo.toml` `[dependencies]` add `rcgen = "0.13"` and `rustls-pki-types = "1"`.

- [ ] **Step 2: Inspect the existing harness**

Run: `grep -n "pub fn weir_server\|pub struct\|TempDir\|socket_path" crates/weir-testkit/src/*.rs`
Confirm how `weir_server(...)` spins up a daemon (it returns a handle with a socket path + temp WAB dir). The TLS helper extends this shape with a TCP address + generated cert paths.

- [ ] **Step 3: Write `tls.rs` (cert-generation fixtures)**

```rust
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
    /// A client cert signed by a DIFFERENT CA (for the rejection test).
    pub wrong_ca_client_cert_path: PathBuf,
    pub wrong_ca_client_key_path: PathBuf,
    /// An already-expired client cert signed by the right CA.
    pub expired_client_cert_path: PathBuf,
    pub expired_client_key_path: PathBuf,
}

fn write(dir: &std::path::Path, name: &str, pem: &str) -> PathBuf {
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
        // not_after in the past → handshake-time validity check rejects it.
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
            ca_cert_path: write(&dir, "ca.pem", &ca.cert.pem()),
            server_cert_path: write(&dir, "server.pem", &server_cert),
            server_key_path: write(&dir, "server.key", &server_key),
            client_cert_path: write(&dir, "client.pem", &client_cert),
            client_key_path: write(&dir, "client.key", &client_key),
            wrong_ca_client_cert_path: write(&dir, "wrong.pem", &wrong_cert),
            wrong_ca_client_key_path: write(&dir, "wrong.key", &wrong_key),
            expired_client_cert_path: write(&dir, "expired.pem", &exp_cert),
            expired_client_key_path: write(&dir, "expired.key", &exp_key),
        }
    }
}
```

In `crates/weir-testkit/src/lib.rs` add `pub mod tls;`.

> **NOTE:** confirm rcgen 0.13 exposes `date_time_ymd` (it does, re-exported from the `time` integration). If not, set `params.not_after` via `time::OffsetDateTime`. The executor verifies at compile time.

- [ ] **Step 4: Build testkit**

Run: `cargo build -p weir-testkit`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-testkit/
git commit -m "test(testkit): TLS cert fixtures (ca/server/client/wrong/expired)"
```

---

## Task 7: TCP accept loop (`tcp.rs`) + integration tests

**Files:**
- Create: `crates/weir-server/src/socket/tcp.rs`
- Modify: `crates/weir-server/src/socket/mod.rs` (`#[cfg(feature = "tls")] pub mod tcp;` + share helpers)
- Test: `crates/weir-server/tests/tls_listener.rs` (new integration test file)

- [ ] **Step 1: Register the module**

In `socket/mod.rs`: `#[cfg(feature = "tls")] pub mod tcp;`. Also make `ConnectionConfig`, the drain helper, and `handle_connection` reachable from `tcp.rs` (they're already `pub`/`pub(crate)` in this module — confirm and widen visibility only if needed).

- [ ] **Step 2: Write `tcp.rs`**

```rust
//! TCP accept loop with mutual TLS (feature = "tls").
//!
//! Mirrors the Unix accept loop in `mod.rs`: shared connection-limit semaphore,
//! round-robin shard assignment, graceful-shutdown watch channel. The ONLY
//! differences are (a) it binds a TCP socket, and (b) every connection must
//! complete a mutual-TLS handshake (CA-signed client cert) — under a handshake
//! timeout — before the shared `handle_connection` ever sees a byte.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{
    metrics::{Metrics, TlsHandshakeFailureLabel, TlsHandshakeFailureReason},
    models::WorkUnit,
    queue::QueueSender,
    socket::{
        connection::{handle_connection, ConnectionConfig},
        tls::ReloadableServerConfig,
    },
};

pub struct TcpConfig {
    pub bind_addr: std::net::SocketAddr,
    pub max_connections: usize,
    pub max_payload_bytes: usize,
    pub shard_count: usize,
    pub shutdown_timeout_secs: u64,
    pub connection_read_timeout_secs: u64,
    pub handshake_timeout_secs: u64,
}

/// Binds the TCP socket and accepts mutual-TLS connections until shutdown.
/// Shares `sem` with the Unix loop so the global connection cap is honoured
/// across both transports.
pub async fn run(
    config: TcpConfig,
    tls: ReloadableServerConfig,
    queue_tx: QueueSender<WorkUnit>,
    sem: Arc<Semaphore>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    handler_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "TCP+mTLS listener bound");

    let effective_cap = config
        .max_payload_bytes
        .min(weir_core::MAX_PAYLOAD_HARD_CAP);
    let read_timeout = Duration::from_secs(config.connection_read_timeout_secs);
    let handshake_timeout = Duration::from_secs(config.handshake_timeout_secs);
    let shard_count = config.shard_count.max(1) as u64;
    let conn_counter = AtomicU64::new(0);
    let mut join_set: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                info!("TCP manager: shutdown signal received, stopping accept loop");
                break;
            }
            res = listener.accept() => {
                let (tcp_stream, peer) = match res {
                    Ok(v) => v,
                    Err(e) => { error!(error = %e, "tcp accept error"); continue; }
                };
                let _ = tcp_stream.set_nodelay(true);

                let Ok(permit) = sem.clone().try_acquire_owned() else {
                    warn!("connection limit reached; dropping TCP connection");
                    drop(tcp_stream);
                    continue;
                };

                let acceptor = tokio_rustls::TlsAcceptor::from(tls.current());
                let tx = queue_tx.clone();
                let m = Arc::clone(&metrics);
                let handler_shutdown = handler_shutdown_rx.clone();
                let n = conn_counter.fetch_add(1, Ordering::Relaxed);
                let cfg = ConnectionConfig {
                    max_payload_bytes: effective_cap,
                    read_timeout,
                    ack_timeout: crate::socket::connection::ACK_TIMEOUT,
                    shard_id: (n % shard_count) as u32,
                };

                join_set.spawn(async move {
                    let _permit = permit;
                    // ── TLS handshake under timeout ───────────────────────────
                    let tls_stream = match tokio::time::timeout(
                        handshake_timeout,
                        acceptor.accept(tcp_stream),
                    ).await {
                        Err(_) => {
                            m.tls_handshake_failures.get_or_create(&TlsHandshakeFailureLabel {
                                reason: TlsHandshakeFailureReason::timeout,
                            }).inc();
                            debug!(%peer, "tls handshake timed out");
                            return;
                        }
                        Ok(Err(e)) => {
                            let reason = classify_handshake_error(&e);
                            m.tls_handshake_failures.get_or_create(&TlsHandshakeFailureLabel { reason }).inc();
                            debug!(%peer, error = %e, "tls handshake failed");
                            return;
                        }
                        Ok(Ok(s)) => s,
                    };

                    // Best-effort: log the client cert CN for audit/tracing.
                    if let Some(cn) = client_cert_cn(&tls_stream) {
                        debug!(%peer, client_cn = %cn, "tls connection established");
                    }

                    if let Err(e) = handle_connection(tls_stream, tx, cfg, m, handler_shutdown).await {
                        use std::io::ErrorKind::*;
                        if !matches!(e.kind(), UnexpectedEof | ConnectionReset | BrokenPipe) {
                            warn!(error = %e, "tls connection closed with error");
                        }
                    }
                });
            }
        }
    }

    // Graceful drain, same policy as the Unix loop.
    let timeout = Duration::from_secs(config.shutdown_timeout_secs);
    if tokio::time::timeout(timeout, async { while join_set.join_next().await.is_some() {} })
        .await
        .is_err()
    {
        error!(remaining = join_set.len(), "TCP manager: shutdown timeout; aborting connections");
        metrics.connections_aborted_at_shutdown.inc_by(join_set.len() as u64);
        join_set.abort_all();
    }
    Ok(())
}

/// Maps a rustls handshake error to a metric reason. rustls surfaces
/// "no client cert" and "bad cert" as `AlertReceived`/`InvalidCertificate`
/// variants; we collapse to the coarse buckets the metric exposes.
fn classify_handshake_error(e: &std::io::Error) -> TlsHandshakeFailureReason {
    let msg = e.to_string();
    if msg.contains("CertificateRequired") || msg.contains("certificate required") {
        TlsHandshakeFailureReason::no_client_cert
    } else if msg.contains("InvalidCertificate")
        || msg.contains("UnknownIssuer")
        || msg.contains("Expired")
        || msg.contains("BadEncoding")
    {
        TlsHandshakeFailureReason::bad_cert
    } else {
        TlsHandshakeFailureReason::other
    }
}

/// Extracts the client certificate Common Name from a completed TLS stream.
fn client_cert_cn<IO>(stream: &tokio_rustls::server::TlsStream<IO>) -> Option<String> {
    let (_io, conn) = stream.get_ref();
    let cert = conn.peer_certificates()?.first()?;
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    parsed
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string())
}
```

> **NOTE:** confirm `ACK_TIMEOUT` is `pub(crate)` in `connection.rs` (the existing Unix loop references `crate::socket::connection::ACK_TIMEOUT`, so it is). Confirm `weir_core::MAX_PAYLOAD_HARD_CAP` and `connections_aborted_at_shutdown` names. `classify_handshake_error` is string-based because rustls collapses many cases into `io::Error`; the integration tests assert the *coarse* reason, so exact substring matching is validated there — adjust substrings if a test shows a mismatch.

- [ ] **Step 3: Write the integration tests**

Create `crates/weir-server/tests/tls_listener.rs`:

```rust
//! TCP + mutual-TLS integration tests. Run with: cargo test -p weir-server --features tls --test tls_listener
#![cfg(feature = "tls")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use weir_core::{Durability, Envelope, Header, MessageType, HEADER_LEN};
use weir_testkit::tls::TlsFixture;

// Spins up a daemon with BOTH a Unix socket and a TCP+mTLS listener, returns
// the bound TCP address + the fixture. (Implemented via the testkit harness;
// see NOTE — the helper name/shape must match Task 6's harness.)
fn start_tls_daemon(tag: &str) -> (std::net::SocketAddr, TlsFixture, weir_testkit::ServerHandle) {
    let fx = TlsFixture::generate(tag);
    let handle = weir_testkit::weir_server_tls(&fx); // see NOTE below
    (handle.tcp_addr(), fx, handle)
}

// Build a blocking rustls client connection using the given client cert/key.
fn tls_connect(
    addr: std::net::SocketAddr,
    ca: &std::path::Path,
    client_cert: &std::path::Path,
    client_key: &std::path::Path,
) -> std::io::Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    use rustls_pki_types::ServerName;
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(ca) {
        roots.add(c).unwrap();
    }
    let certs = load_certs(client_cert);
    let key = load_key(client_key);
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_client_auth_cert(certs, key)
    .unwrap();
    let server_name = ServerName::try_from("weir-server").unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let sock = TcpStream::connect(addr)?;
    Ok(rustls::StreamOwned::new(conn, sock))
}

fn load_certs(p: &std::path::Path) -> Vec<rustls_pki_types::CertificateDer<'static>> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(p).unwrap());
    rustls_pemfile::certs(&mut rd).map(|r| r.unwrap()).collect()
}
fn load_key(p: &std::path::Path) -> rustls_pki_types::PrivateKeyDer<'static> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(p).unwrap());
    rustls_pemfile::private_key(&mut rd).unwrap().unwrap()
}

fn push_one(stream: &mut impl ReadWrite, payload: &[u8]) -> MessageType {
    let header = Header::new(MessageType::Push, Durability::Sync, 0, payload.len() as u32);
    let frame = Envelope::new(header, payload.to_vec()).encode();
    stream.write_all(&frame).unwrap();
    let mut resp = [0u8; HEADER_LEN];
    stream.read_exact(&mut resp).unwrap();
    Header::decode(&resp).unwrap().message_type
}
trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

#[test]
fn valid_client_cert_pushes_succeed() {
    let (addr, fx, _h) = start_tls_daemon("valid");
    let mut s = tls_connect(addr, &fx.ca_cert_path, &fx.client_cert_path, &fx.client_key_path).unwrap();
    for _ in 0..1000 {
        assert_eq!(push_one(&mut s, b"hello"), MessageType::Ack);
    }
}

#[test]
fn no_client_cert_is_rejected() {
    let (addr, fx, _h) = start_tls_daemon("nocert");
    // Build a client config WITHOUT a client cert.
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(&fx.ca_cert_path) { roots.add(c).unwrap(); }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions().unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let sn = rustls_pki_types::ServerName::try_from("weir-server").unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(config), sn).unwrap();
    let sock = TcpStream::connect(addr).unwrap();
    let mut s = rustls::StreamOwned::new(conn, sock);
    // Server demands a client cert → handshake/IO fails when we try to use it.
    let header = Header::new(MessageType::Push, Durability::Sync, 0, 5);
    let frame = Envelope::new(header, b"hello".to_vec()).encode();
    let _ = s.write_all(&frame);
    let mut resp = [0u8; HEADER_LEN];
    assert!(s.read_exact(&mut resp).is_err(), "no-cert client must not get an Ack");
}

#[test]
fn wrong_ca_client_cert_is_rejected() {
    let (addr, fx, _h) = start_tls_daemon("wrongca");
    let r = tls_connect(addr, &fx.ca_cert_path, &fx.wrong_ca_client_cert_path, &fx.wrong_ca_client_key_path);
    // Either the handshake errors immediately, or the first frame I/O fails.
    let failed = match r {
        Err(_) => true,
        Ok(mut s) => {
            let header = Header::new(MessageType::Push, Durability::Sync, 0, 5);
            let frame = Envelope::new(header, b"hello".to_vec()).encode();
            s.write_all(&frame).is_err() || {
                let mut resp = [0u8; HEADER_LEN];
                s.read_exact(&mut resp).is_err()
            }
        }
    };
    assert!(failed, "wrong-CA client cert must be rejected");
}

#[test]
fn expired_client_cert_is_rejected() {
    let (addr, fx, _h) = start_tls_daemon("expired");
    let r = tls_connect(addr, &fx.ca_cert_path, &fx.expired_client_cert_path, &fx.expired_client_key_path);
    let failed = match r {
        Err(_) => true,
        Ok(mut s) => {
            let header = Header::new(MessageType::Push, Durability::Sync, 0, 5);
            let frame = Envelope::new(header, b"hi".to_vec()).encode();
            s.write_all(&frame).is_err() || {
                let mut resp = [0u8; HEADER_LEN];
                s.read_exact(&mut resp).is_err()
            }
        }
    };
    assert!(failed, "expired client cert must be rejected");
}
```

> **NOTE — the harness helper.** `weir_testkit::weir_server_tls(&fx)` and `ServerHandle::tcp_addr()` must be provided by Task 6's harness. If Task 6 implemented only the cert fixtures, extend the testkit now: add a `weir_server_tls(fixture)` that constructs a `Config` with `tcp_bind = Some(127.0.0.1:0)` (port 0 = OS-assigned) + the fixture's server cert/key/ca paths, starts the daemon via the same internal entrypoint `weir_server(...)` uses, and exposes the actually-bound port. Binding port 0 requires reading back the bound address — have the daemon entrypoint surface it, or bind the `TcpListener` in the harness and hand the daemon the listener. Simplest: add a test-only `run_with_listener` path, or bind to `127.0.0.1:0` and have `tcp.rs::run` report the local addr through a `oneshot`. Pick the approach that fits the existing harness; keep it test-only.

- [ ] **Step 4: Run the integration tests**

Run: `cargo test -p weir-server --features tls --test tls_listener`
Expected: 4 tests PASS (valid → 1000 acks; no-cert / wrong-CA / expired → rejected).

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/socket/tcp.rs crates/weir-server/src/socket/mod.rs crates/weir-server/tests/tls_listener.rs crates/weir-testkit/
git commit -m "feat(socket): TCP + mutual-TLS accept loop with integration tests"
```

---

## Task 8: SIGHUP hot cert reload

**Files:**
- Modify: `crates/weir-server/src/main.rs` (or a small `socket/tls.rs` helper) — spawn the SIGHUP task
- Test: `crates/weir-server/tests/tls_listener.rs` (add reload tests)

- [ ] **Step 1: Add the SIGHUP reload task**

In `main.rs`, where the daemon's tokio tasks are spawned (after the TLS config is loaded — see Task 9 wiring), add a task that reloads on SIGHUP. Provide it the `ReloadableServerConfig` and `Arc<Metrics>`:

```rust
#[cfg(feature = "tls")]
fn spawn_tls_reload_task(
    tls: crate::socket::tls::ReloadableServerConfig,
    metrics: std::sync::Arc<crate::metrics::Metrics>,
) {
    use crate::metrics::{TlsReloadLabel, TlsReloadOutcome};
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGHUP handler; cert reload disabled");
                return;
            }
        };
        while hup.recv().await.is_some() {
            match tls.reload() {
                Ok(()) => {
                    metrics.tls_config_reloads
                        .get_or_create(&TlsReloadLabel { outcome: TlsReloadOutcome::ok }).inc();
                    tracing::info!("SIGHUP: TLS certificates reloaded");
                }
                Err(e) => {
                    metrics.tls_config_reloads
                        .get_or_create(&TlsReloadLabel { outcome: TlsReloadOutcome::failed }).inc();
                    tracing::error!(error = %e, "SIGHUP: TLS reload failed; keeping previous certs");
                }
            }
        }
    });
}
```

Call `spawn_tls_reload_task(tls.clone(), metrics.clone())` only when `tcp_bind` is configured (next to the TCP-loop spawn in Task 9).

- [ ] **Step 2: Write the reload tests (unit-level on the wrapper)**

The end-to-end SIGHUP path (signal delivery) is awkward to test in-process; assert the **reload mechanism** directly via `ReloadableServerConfig`, which the SIGHUP task calls. Add to `crates/weir-server/tests/tls_listener.rs`:

```rust
#[test]
fn reload_failure_keeps_serving() {
    // Stand up a reloadable config, then point one path at garbage and reload.
    use weir_testkit::tls::TlsFixture;
    let fx = TlsFixture::generate("reloadfail");
    let r = weir_server::socket::tls::ReloadableServerConfig::load(
        fx.server_cert_path.clone(),
        fx.server_key_path.clone(),
        fx.ca_cert_path.clone(),
    ).unwrap();
    let before = r.current();
    // Overwrite the cert file with junk, then reload → must fail, keep `before`.
    std::fs::write(&fx.server_cert_path, b"not a cert").unwrap();
    assert!(r.reload().is_err());
    assert!(std::sync::Arc::ptr_eq(&before, &r.current()), "failed reload must retain old config");
}
```

Plus a success-path reload assertion (already covered by `reload_swaps_config` in `tls.rs` unit tests — reference it; no need to duplicate).

> **NOTE:** this requires `weir_server::socket::tls` to be reachable from the integration test. Ensure `socket` and `tls` are `pub` in the lib (the lib already exposes `socket`; confirm `pub mod tls` not `pub(crate)`). If the project keeps `socket` internal, move this assertion into the `tls.rs` `#[cfg(test)]` unit module instead (where it has crate-internal access) — that's the cleaner home and avoids widening visibility.

- [ ] **Step 3: Run + build**

Run: `cargo test -p weir-server --features tls` (whole feature suite) then `cargo build -p weir-server --features tls`
Expected: PASS / clean.

- [ ] **Step 4: Commit**

```bash
git add crates/weir-server/src/main.rs crates/weir-server/tests/tls_listener.rs crates/weir-server/src/socket/tls.rs
git commit -m "feat(tls): SIGHUP hot cert reload (fail-safe, metric-counted)"
```

---

## Task 9: Wire the TCP loop into `main.rs`

**Files:**
- Modify: `crates/weir-server/src/main.rs`

- [ ] **Step 1: Inspect the current Unix-loop spawn + shared semaphore**

Run: `grep -n "socket::run\|Semaphore\|SocketConfig\|tokio::spawn\|shutdown" crates/weir-server/src/main.rs`
Confirm how `socket::run(...)` is launched and how shutdown is signalled. The TCP loop must share the **same** `Arc<Semaphore>` and the same `handler_shutdown` watch receiver. Currently the Unix loop creates the semaphore internally (`socket/mod.rs`) — refactor so the semaphore + handler-shutdown watch are created in `main.rs` (or a shared setup fn) and passed into both `socket::run` and `tcp::run`.

- [ ] **Step 2: Refactor the shared connection-limit semaphore**

Change `socket::run` to accept `sem: Arc<Semaphore>` and the `handler_shutdown` pair as parameters instead of creating them internally (move the `Semaphore::new(max_connections)` and the `watch::channel(false)` up into `main.rs`). Update the Unix-loop body to use the passed-in `sem`/watch. This keeps the global cap shared across both transports (spec §7).

> Keep this refactor minimal and behaviour-preserving for the Unix-only path: with no `tcp_bind`, only `socket::run` is spawned and behaves exactly as before.

- [ ] **Step 3: Spawn the TCP loop + reload task when configured**

In `main.rs`, after building `Config` and the shared `sem`:

```rust
#[cfg(feature = "tls")]
let tcp_handle = if let Some(bind_addr) = config.tcp_bind {
    let tls = crate::socket::tls::ReloadableServerConfig::load(
        config.tls_cert_path.clone().expect("validated present"),
        config.tls_key_path.clone().expect("validated present"),
        config.tls_client_ca_path.clone().expect("validated present"),
    )
    .map_err(|e| { tracing::error!(error = %e, "failed to load TLS config"); e })?;

    spawn_tls_reload_task(tls.clone(), metrics.clone());

    let (tcp_shutdown_tx, tcp_shutdown_rx) = tokio::sync::oneshot::channel();
    // Register tcp_shutdown_tx with the daemon's shutdown fan-out so SIGTERM
    // stops BOTH listeners (mirror how the Unix shutdown oneshot is wired).
    let tcp_cfg = crate::socket::tcp::TcpConfig {
        bind_addr,
        max_connections: config.max_connections,
        max_payload_bytes: config.max_payload_bytes,
        shard_count: config.shard_count,
        shutdown_timeout_secs: config.shutdown_timeout_secs,
        connection_read_timeout_secs: config.connection_read_timeout_secs,
        handshake_timeout_secs: config.tls_handshake_timeout_secs,
    };
    Some((tokio::spawn(crate::socket::tcp::run(
        tcp_cfg, tls, queue_tx.clone(), sem.clone(), tcp_shutdown_rx,
        handler_shutdown_rx.clone(), metrics.clone(),
    )), tcp_shutdown_tx))
} else {
    None
};
```

Wire `tcp_shutdown_tx` into the existing shutdown path (the same place SIGTERM fires the Unix loop's shutdown), and `.await` the `tcp_handle` join during graceful shutdown alongside the Unix one.

> **NOTE:** match the real local variable names in `main.rs` (`metrics`, `queue_tx`, the shutdown plumbing). The `error` map on `load` makes a broken TLS config a hard startup failure (spec: never a plaintext downgrade). The `TLS map_err`/`?` requires `main` to return a `Result` — it already does (it calls `Config::load()?`).

- [ ] **Step 4: Manual smoke test (real daemon, both listeners)**

Generate a quick cert set and run the daemon. Use the testkit fixture generator via a throwaway binary, OR `openssl`/`rcgen`-cli. Then:

```bash
cargo build -p weir-server --features tls
# (generate ca.pem/server.pem/server.key/client.pem/client.key — see docs/operations/tcp-mtls.md once written)
./target/debug/weir-server \
  --wab-dir /tmp/weir/wab --socket-path /tmp/weir/run/weir.sock \
  --tcp-bind 127.0.0.1:7100 --tls-cert server.pem --tls-key server.key --tls-client-ca ca.pem &
```
Expected: log lines for BOTH "socket listening" (Unix) and "TCP+mTLS listener bound" (127.0.0.1:7100). Kill with SIGTERM; expect clean drain of both.

> If you cannot generate certs by hand here, this manual step can be skipped in favour of the Task 7 integration tests, which exercise the same path in-process. Note in the commit if skipped.

- [ ] **Step 5: Run the full server test suite (both feature states)**

Run: `cargo test -p weir-server` then `cargo test -p weir-server --features tls`
Expected: PASS in both.

- [ ] **Step 6: Commit**

```bash
git add crates/weir-server/src/main.rs crates/weir-server/src/socket/mod.rs
git commit -m "feat(server): run Unix + TCP/mTLS listeners concurrently on one queue"
```

---

## Task 10: Client TLS connector + per-connection durability default

**Files:**
- Modify: `crates/weir-client/Cargo.toml`
- Modify: `crates/weir-client/src/unix.rs` (generalize struct + methods; add durability default)
- Modify: `crates/weir-client/src/lib.rs` (generic re-exports + `tls` module)
- Create: `crates/weir-client/src/tls.rs`
- Create: `crates/weir-client/examples/push_tls.rs`

- [ ] **Step 1: Add the `tls` feature + deps to the client**

In `crates/weir-client/Cargo.toml`:

```toml
[features]
default = []
tls = ["dep:rustls", "dep:rustls-pemfile", "dep:rustls-pki-types"]

[dependencies]
weir-core = { path = "../weir-core" }
crc32fast = "1"
rustls = { version = "0.23", optional = true }
rustls-pemfile = { version = "2", optional = true }
rustls-pki-types = { version = "1", optional = true }
```

- [ ] **Step 2: Generalize `WeirClient` over the stream + add durability default**

In `unix.rs`, change the struct + impl split. Keep `UnixStream` as the default type param so existing `WeirClient` usages compile unchanged.

```rust
// struct: generic with a Unix default + a connection-level durability default.
pub struct WeirClient<S = UnixStream> {
    stream: S,
    default_durability: Option<Durability>,
}

// Shared methods over any Read+Write transport (Unix OR TLS).
impl<S: Read + Write> WeirClient<S> {
    pub fn set_default_durability(&mut self, durability: Durability) {
        self.default_durability = Some(durability);
    }

    pub fn push(&mut self, payload: impl AsRef<[u8]>, durability: Durability) -> Result<(), ClientError> {
        // (unchanged body — already only uses self.stream as Read+Write)
        # ...
    }

    /// Pushes at the connection's default durability tier (set at connect time
    /// or via `set_default_durability`). Errors if no default was set.
    pub fn push_default(&mut self, payload: impl AsRef<[u8]>) -> Result<(), ClientError> {
        let d = self.default_durability.ok_or(ClientError::NoDefaultDurability)?;
        self.push(payload, d)
    }

    pub fn health_check(&mut self) -> Result<(), ClientError> { /* unchanged */ }
    fn read_response(&mut self) -> Result<Envelope, ClientError> { /* unchanged */ }
}

// Unix-specific constructors.
impl WeirClient<UnixStream> {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path.as_ref())?;
        Ok(Self { stream, default_durability: None })
    }
    pub fn connect_with_default(path: impl AsRef<Path>, durability: Durability) -> Result<Self, ClientError> {
        let mut c = Self::connect(path)?;
        c.default_durability = Some(durability);
        Ok(c)
    }
    pub fn from_stream(stream: UnixStream) -> Self {
        Self { stream, default_durability: None }
    }
}
```

Add the error variant to `enum ClientError`:

```rust
    /// `push_default` was called but no default durability was set.
    NoDefaultDurability,
```
and its `Display` arm:
```rust
    Self::NoDefaultDurability => write!(f, "push_default called but no default durability was set"),
```

- [ ] **Step 3: Add the failing durability-default test**

In `unix.rs` tests (or a new `#[cfg(test)] mod`):

```rust
#[test]
fn push_default_without_default_errors() {
    let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
    let mut c = WeirClient::from_stream(a);
    let err = c.push_default(b"x").unwrap_err();
    assert!(matches!(err, ClientError::NoDefaultDurability));
}

#[test]
fn set_default_durability_is_used_by_push_default() {
    // Server side echoes an Ack for one Push; assert push_default sends with
    // the configured tier. Use UnixStream::pair + a hand-rolled responder, or
    // assert the encoded header's durability byte via from_stream + a reader.
    let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
    let mut c = WeirClient::from_stream(client_end);
    c.set_default_durability(Durability::Batched);

    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut hdr = [0u8; weir_core::HEADER_LEN];
        server_end.read_exact(&mut hdr).unwrap();
        let h = weir_core::Header::decode(&hdr).unwrap();
        // drain payload + crc so the client's write completes
        let mut rest = vec![0u8; h.payload_len as usize + 4];
        server_end.read_exact(&mut rest).unwrap();
        // reply Ack
        use std::io::Write;
        let ack = weir_core::Envelope::new(
            weir_core::Header::new(weir_core::MessageType::Ack, weir_core::Durability::Sync, 0, 0),
            vec![],
        ).encode();
        server_end.write_all(&ack).unwrap();
        h.durability
    });

    c.push_default(b"hello").unwrap();
    assert_eq!(reader.join().unwrap(), Durability::Batched);
}
```

Run: `cargo test -p weir-client` → Expected: the two new tests + all existing tests PASS.

- [ ] **Step 4: Write the TLS connector (`tls.rs`)**

```rust
//! Blocking mutual-TLS client connector (feature = "tls").

use std::{io::BufReader, net::TcpStream, path::Path, sync::Arc};

use rustls_pki_types::ServerName;
use weir_core::Durability;

use crate::{ClientError, WeirClient};

/// Materials + parameters for an mTLS connection.
pub struct ClientTlsConfig<'a> {
    pub client_cert: &'a Path,
    pub client_key: &'a Path,
    pub ca_cert: &'a Path,
    /// DNS name the server cert must present (matched against its SAN/CN).
    pub server_name: &'a str,
    /// Optional connection-level default durability.
    pub default_durability: Option<Durability>,
}

pub type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

impl WeirClient<TlsStream> {
    /// Connects to a weir daemon's TCP+mTLS listener at `addr`.
    pub fn connect_tls(
        addr: impl std::net::ToSocketAddrs,
        cfg: ClientTlsConfig<'_>,
    ) -> Result<Self, ClientError> {
        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(cfg.ca_cert)? {
            roots.add(c).map_err(|e| ClientError::Protocol(format!("ca: {e}")))?;
        }
        let certs = load_certs(cfg.client_cert)?;
        let key = load_key(cfg.client_key)?;

        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClientError::Protocol(format!("tls: {e}")))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| ClientError::Protocol(format!("client auth: {e}")))?;

        let server_name = ServerName::try_from(cfg.server_name.to_string())
            .map_err(|e| ClientError::Protocol(format!("server name: {e}")))?;
        let conn = rustls::ClientConnection::new(Arc::new(tls_config), server_name)
            .map_err(|e| ClientError::Protocol(format!("tls connect: {e}")))?;
        let sock = TcpStream::connect(addr)?;
        let stream = rustls::StreamOwned::new(conn, sock);

        Ok(Self::from_tls_stream(stream, cfg.default_durability))
    }
}

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
```

Add a small constructor in `unix.rs`'s generic impl (so `tls.rs` can build the struct without touching private fields from another module — or make the fields `pub(crate)`):

```rust
impl<S: Read + Write> WeirClient<S> {
    pub(crate) fn from_tls_stream(stream: S, default_durability: Option<Durability>) -> Self {
        Self { stream, default_durability }
    }
}
```

In `lib.rs`:

```rust
#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub use tls::{ClientTlsConfig, TlsStream};

// WeirClient/ClientError already re-exported; ensure they're visible without cfg(unix)
// gating for the tls path. If the crate is unix-only today, keep the struct available
// on all targets (the Unix constructors stay #[cfg(unix)], the generic methods do not).
```

> **NOTE:** today `lib.rs` gates the whole `unix` module on `#[cfg(unix)]`. The generic `WeirClient<S>` + methods should be available whenever needed by `tls`. Restructure: move the generic struct/methods/`ClientError` into a transport-neutral module (e.g. `client.rs`), keep the `connect`/`from_stream` Unix constructors `#[cfg(unix)]`, and put `connect_tls` in `tls.rs`. Adjust re-exports so `WeirClient` and `ClientError` are exported unconditionally.

- [ ] **Step 5: Write the `push_tls` example**

`crates/weir-client/examples/push_tls.rs`:

```rust
//! mTLS push example. Run: cargo run -p weir-client --features tls --example push_tls
//! Requires a running weir daemon with --tcp-bind and matching client certs.
#[cfg(feature = "tls")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::path::Path;
    use weir_client::{ClientTlsConfig, WeirClient};
    use weir_core::Durability;

    let mut client = WeirClient::connect_tls(
        "127.0.0.1:7100",
        ClientTlsConfig {
            client_cert: Path::new("client.pem"),
            client_key: Path::new("client.key"),
            ca_cert: Path::new("ca.pem"),
            server_name: "weir-server",
            default_durability: Some(Durability::Batched),
        },
    )?;
    client.push_default(b"hello over mTLS")?;
    println!("pushed one record over mTLS");
    Ok(())
}

#[cfg(not(feature = "tls"))]
fn main() {
    eprintln!("build with --features tls to run this example");
}
```

- [ ] **Step 6: Build + test the client both ways**

Run:
```bash
cargo test -p weir-client
cargo build -p weir-client --features tls
cargo build -p weir-client --features tls --example push_tls
```
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add crates/weir-client/
git commit -m "feat(client): mTLS connect_tls + per-connection durability default"
```

---

## Task 11: Docs + CHANGELOG

**Files:**
- Modify: `docs/operations/configuration.md`
- Create: `docs/operations/tcp-mtls.md`
- Modify: `docs/security/threat-model.md`
- Modify: `docs/SUMMARY.md` (book TOC — add the new page)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Document the config keys**

In `docs/operations/configuration.md`, add a "TCP + mutual TLS" section documenting each key (`tcp_bind`, `tls_cert_path`, `tls_key_path`, `tls_client_ca_path`, `tls_handshake_timeout_secs`) with type, default, CLI flag, env var, TOML key, and the "TLS mandatory / never plaintext" + "requires --features tls" notes. Match the table style already in that file.

- [ ] **Step 2: Write the operator guide**

Create `docs/operations/tcp-mtls.md`: threat model (hostile net), how to stand up a CA + issue server/client certs (openssl or rcgen recipe), the four config keys, running with both listeners, **cert rotation via SIGHUP** (`kill -HUP <pid>` reloads certs with no dropped connections; failed reloads keep the old certs and bump `weir_tls_config_reloads_total{outcome="failed"}`), and the metrics to watch (`weir_tls_handshake_failures_total`). State what's out of scope: CN allowlist, CRL/OCSP, plaintext TCP.

- [ ] **Step 3: Threat-model note**

In `docs/security/threat-model.md`, add a row/section: the TCP path's trust is mutual TLS with CA-signed client certs; revocation is by CA rotation (CRL/OCSP out of scope, v2); handshake-slowloris is bounded by `tls_handshake_timeout_secs`; the connection cap is shared with the Unix listener.

- [ ] **Step 4: Add to the book TOC**

In `docs/SUMMARY.md`, add `- [TCP + mutual TLS](operations/tcp-mtls.md)` under the Operations section.

- [ ] **Step 5: CHANGELOG**

Under `## [Unreleased]`, add an `### Added` entry summarizing: TCP listener with mutual TLS (CA-signed client certs, rustls/aws-lc-rs), concurrent with the Unix socket; SIGHUP hot cert reload; `weir-client` `connect_tls` + per-connection durability default; new `tls` feature (default off) on both crates; new config keys + metrics. Note the wire protocol is unchanged (TLS wraps the existing frame protocol).

- [ ] **Step 6: Docs build check**

Run: `mdbook build` (if installed) — or at minimum confirm no broken relative links by eye.
Expected: builds / links resolve.

- [ ] **Step 7: Commit**

```bash
git add docs/ CHANGELOG.md
git commit -m "docs: TCP + mutual TLS operator guide, config keys, threat-model note [skip ci]"
```

---

## Final verification (before opening the PR)

- [ ] **Default build is Unix-only and unchanged:** `cargo build` and `cargo test` (no features) green.
- [ ] **TLS feature builds + tests:** `cargo build --features tls -p weir-server` and `cargo test -p weir-server --features tls` green; `cargo test -p weir-client` and `cargo build -p weir-client --features tls` green.
- [ ] **Lint (matches CI, all targets/features):** `cargo fmt --all -- --check` and `cargo clippy --all-targets --features tls -- -D warnings` and `cargo clippy --all-targets -- -D warnings` all clean. *(Per the project's CI/PR discipline: run the full lint locally AND wait for ALL CI checks incl. Windows before merging. The `tls` code is Unix-oriented but must still compile in the cross-platform build — keep any libc/Unix-only bits `#[cfg(unix)]`.)*
- [ ] **Spec acceptance criteria** (spec §14) each map to a passing test or a manual check above.
- [ ] **Open PR** from `feat/tcp-tls` → `main` once CI is green.

---

## Self-review notes (author)

- **Spec coverage:** §3 architecture → Tasks 1,7,9; §4 generic handler → Task 1; §5 tls.rs → Task 4; §6 tcp.rs → Task 7; §6.1 SIGHUP reload → Tasks 4(reload wrapper)+8; §7 hardening → Tasks 5(validation),7(timeout/cap); §8 config → Task 5; §9 client + §9.1 durability default → Task 10; §10 observability → Task 3 (+CN in Task 7); §11 feature gating → Tasks 2,10; §12 testing → Tasks 4,6,7,8,10; §13 out-of-scope → not built (correct); §14 acceptance → Final verification.
- **Known executor judgement points (flagged inline):** exact queue/ack/metrics API names (Task 1), `ConfigError` variant shape (Task 5), the testkit `weir_server_tls` harness + port-0 readback (Tasks 6/7), client `#[cfg(unix)]` module restructure for the TLS path (Task 10), and rustls handshake-error substring matching (Task 7). Each has a NOTE with how to resolve against the real code.
- **Deviation from spec §10:** client identity on the tracing span is the cert **CN via x509-parser** (as specced) — implemented in `tcp.rs::client_cert_cn`. No fingerprint substitution.
