# TCP + mutual TLS

> **Default-off feature.** The TCP listener and all TLS code lives behind the
> `tls` Cargo feature on `weir-server` (and `weir-client`). A standard
> `cargo build` produces a Unix-socket-only binary. To enable the TCP path:
>
> ```bash
> cargo build --features tls
> ```

## Threat model

Weir's Unix socket is designed for local producers: OS file permissions are
the trust boundary and there is no over-the-wire authentication. That is the
right model for co-located producers — cheap, auditable, zero extra attack
surface.

Remote producers across an untrusted network need a different model. The TCP
listener addresses this with **mandatory mutual TLS**:

- Every TCP client must present a certificate signed by the configured CA.
  Anonymous clients (no cert presented) and clients with certs signed by an
  unknown CA are rejected at the TLS handshake before any application data
  is exchanged.
- **Plaintext TCP is never exposed.** Setting `tcp_bind` without a complete
  TLS configuration is a fatal startup error. The daemon does not fall back
  to cleartext under any circumstances.
- **Trust model: CA issuance.** Issuing a client certificate from the
  configured CA is the act of authorising a producer. There is no per-CN/SAN
  allowlist — possession of a valid CA-signed cert is sufficient authority.
  To revoke a client, rotate the CA (see [Out of scope](#out-of-scope) for
  why CRL/OCSP are not implemented).
- **TLS implementation: rustls with the ring crypto provider.** No
  system OpenSSL dependency; the crypto provider is selected explicitly so
  the binary's code-signing surface stays at one provider.

The existing per-frame hardening applies over TLS without modification:
CRC-before-alloc, `max_payload_bytes` cap, and `connection_read_timeout_secs`
all operate on the decrypted byte stream exactly as they do on the Unix path.

## Setting up a CA and issuing certificates

The recipe below uses `openssl` to produce a minimal PKI. Real deployments
should use a CA management tool (CFSSL, Vault PKI, step-ca, etc.) — the
openssl recipe is a concise illustration of the certificate relationships, not
a production recommendation.

### 1. Create a CA

```bash
# CA key (keep this secret)
openssl genrsa -out ca.key 4096

# Self-signed CA certificate (10 years; adjust -days as needed)
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 \
  -subj "/CN=weir-producer-ca"
```

### 2. Create the server certificate

The server cert must have a Subject Alternative Name (SAN) that matches the
hostname or IP address clients will connect to. The `server_name` field in
`WeirClient::connect_tls` must match one of the SANs.

```bash
# Server key
openssl genrsa -out server.key 4096

# CSR
openssl req -new -key server.key -out server.csr \
  -subj "/CN=weir-server"

# Sign with the CA, adding the SAN
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365 \
  -extfile <(printf "subjectAltName=DNS:weir.internal.example.com,IP:10.0.0.5")
```

Place `server.crt` and `server.key` where the daemon can read them (mode
`0o400` for the key, owned by the daemon user).

### 3. Issue a client certificate

Each producer that needs to connect to the TCP listener gets its own client
cert, signed by the CA from step 1.

```bash
# Client key
openssl genrsa -out client.key 4096

# CSR
openssl req -new -key client.key -out client.csr \
  -subj "/CN=producer-host-1"

# Sign with the CA
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days 365
```

Repeat for every producer. The `ca.crt` from step 1 is the
`tls_client_ca_path` you give to the daemon — it is the **only** CA the
daemon trusts for client authentication.

## Running with both listeners

With certs in place, start the daemon with `--tcp-bind` and the three cert
paths:

```bash
weir-server \
  --wab-dir     /var/lib/weir/wab \
  --tcp-bind    0.0.0.0:7100 \
  --tls-cert    /etc/weir/tls/server.crt \
  --tls-key     /etc/weir/tls/server.key \
  --tls-client-ca /etc/weir/tls/ca.crt
```

Or equivalently via env vars (preferred in container environments):

```bash
WEIR_WAB_DIR=/var/lib/weir/wab \
WEIR_TCP_BIND=0.0.0.0:7100 \
WEIR_TLS_CERT=/etc/weir/tls/server.crt \
WEIR_TLS_KEY=/etc/weir/tls/server.key \
WEIR_TLS_CLIENT_CA=/etc/weir/tls/ca.crt \
weir-server
```

The Unix socket (`--socket-path`, default `/run/weir/weir.sock`) starts as
usual. Both listeners feed the same internal pipeline. The total connection
cap is shared: `max_connections` bounds the combined Unix + TCP concurrent
connections.

### TOML file example

```toml
[server]
wab_dir  = "/var/lib/weir/wab"
tcp_bind = "0.0.0.0:7100"

tls_cert_path      = "/etc/weir/tls/server.crt"
tls_key_path       = "/etc/weir/tls/server.key"
tls_client_ca_path = "/etc/weir/tls/ca.crt"

# Handshake timeout (default 10s; lower on well-connected networks)
tls_handshake_timeout_secs = 10

# Shared connection cap across Unix + TCP listeners
max_connections = 256
```

## Config reference summary

| TOML key | CLI flag | Env var | Default | Notes |
|----------|----------|---------|---------|-------|
| `tcp_bind` | `--tcp-bind` | `WEIR_TCP_BIND` | none | Socket addr, e.g. `0.0.0.0:7100`; TCP disabled when absent |
| `tls_cert_path` | `--tls-cert` | `WEIR_TLS_CERT` | none | Required when `tcp_bind` set |
| `tls_key_path` | `--tls-key` | `WEIR_TLS_KEY` | none | Required when `tcp_bind` set |
| `tls_client_ca_path` | `--tls-client-ca` | `WEIR_TLS_CLIENT_CA` | none | Required when `tcp_bind` set |
| `tls_handshake_timeout_secs` | `--tls-handshake-timeout-secs` | `WEIR_TLS_HANDSHAKE_TIMEOUT_SECS` | `10` | Bounds TLS-handshake slowloris; permit held across handshake |

All five keys follow the standard weir config merge order: CLI flag >
environment variable > TOML file > built-in default.

## Cert rotation via SIGHUP

TLS material can be rotated **without dropping active connections**:

1. Replace the cert/key/CA files at the configured paths.
2. Send SIGHUP to the daemon:
   ```bash
   kill -HUP $(pidof weir-server)
   # or with a PID file:
   kill -HUP $(cat /run/weir/weir.pid)
   ```

The daemon re-reads the three TLS files and installs the new configuration
for all subsequent TLS handshakes. Connections that are already established
continue on the old TLS session until they close naturally.

**Fail-safe reload**: if the new files are missing, unreadable, or contain
invalid PEM, the daemon:

- Keeps serving with the **previous** TLS configuration.
- Logs an `error!`-level message with the failure reason.
- Increments `weir_tls_config_reloads_total{outcome="failed"}`.

A successful reload increments `weir_tls_config_reloads_total{outcome="ok"}`.

**Important**: SIGHUP reloads **TLS material only**. All other configuration
(socket paths, WAB dir, connection caps, sink settings) is read once at
startup and never reloaded. Changing those settings requires a daemon restart.

### Rotation checklist

1. Generate new cert/key (using the existing CA, or a new CA for full
   rotation).
2. Write new files to the configured paths (or new paths, updating config
   first).
3. Verify the files are readable by the daemon user.
4. `kill -HUP <pid>`
5. Check `weir_tls_config_reloads_total{outcome="ok"}` incremented (via the
   `/metrics` endpoint or your Prometheus instance).
6. Confirm `weir_tls_config_reloads_total{outcome="failed"}` did **not**
   increment.

## Monitoring

### Key metrics

| Metric | Labels | What to watch |
|--------|--------|----------------|
| `weir_tls_handshake_failures_total` | `reason` ∈ {`no_client_cert`, `bad_cert`, `timeout`, `other`} | Any non-zero rate warrants investigation |
| `weir_tls_config_reloads_total` | `outcome` ∈ {`ok`, `failed`} | Alert on `outcome="failed"` after a rotation attempt |

### Suggested alert rules

```promql
# Unauthorised connection attempts or misconfigured clients
rate(weir_tls_handshake_failures_total{reason=~"no_client_cert|bad_cert"}[5m]) > 0

# Cert rotation failed — daemon is still serving old certs
increase(weir_tls_config_reloads_total{outcome="failed"}[5m]) > 0

# Handshake slowloris activity
rate(weir_tls_handshake_failures_total{reason="timeout"}[5m]) > 0.1
```

The existing `weir_connection_idle_timeout_total` and
`weir_accept_latency_seconds` metrics apply to both the Unix and TCP
listeners, so a combined connection-rate dashboard needs no changes.

## Client usage

`weir-client` gains a TLS connector behind its own `tls` feature. Build with:

```bash
cargo add weir-client --features tls
```

### `WeirClient::connect_tls`

```rust
use std::path::Path;
use weir_client::{ClientTlsConfig, WeirClient};
use weir_core::Durability;

// The client API is synchronous — no async runtime required.
let mut client = WeirClient::connect_tls(
    "weir.internal.example.com:7100",
    ClientTlsConfig {
        // Paths are borrowed (&Path); the config borrows, it does not own.
        client_cert: Path::new("/path/to/client.crt"),
        client_key:  Path::new("/path/to/client.key"),
        ca_cert:     Path::new("/path/to/ca.crt"),
        // Must match a SAN in the server cert.
        server_name: "weir.internal.example.com",
        // Tier used by push_default(); None requires a tier per push.
        default_durability: Some(Durability::Sync),
    },
)?;

client.push(b"hello", Durability::Sync)?;
```

The client validates the server certificate against `ca_cert`. The
`server_name` must match a DNS SAN (or IP SAN) in the server certificate.

### Per-connection durability default

`connect_tls` (and `connect`) accept a `default_durability` field in
`ClientTlsConfig` (and a new `connect_with_default` constructor on the
plain client). Once set, `client.push_default(payload)` uses that tier
without repeating the durability argument per-push. The default can be
updated at runtime with `client.set_default_durability(tier)`.

### Example

A complete runnable example is at
`crates/weir-client/examples/push_tls.rs`. Run it against a local daemon
with:

```bash
cargo run --example push_tls --features tls -- \
  --addr weir.internal.example.com:7100 \
  --cert client.crt \
  --key  client.key \
  --ca   ca.crt \
  --sni  weir.internal.example.com
```

## Out of scope

The following are explicitly **not implemented** in the current version. They
are v2 items and are documented here so operators know what to use instead.

| Not implemented | What to use instead |
|----------------|---------------------|
| **CN/SAN allowlist** | CA issuance is the gate. Only issue client certs to authorised producers; the CA is the access control list. |
| **CRL / OCSP revocation** | Rotate the CA: issue a new CA, re-issue server + client certs from the new CA, update `tls_client_ca_path`, send SIGHUP. Old certs become invalid immediately (the old CA is no longer trusted). |
| **General config hot-reload** | SIGHUP reloads TLS material only. All other config changes require a daemon restart. |
| **Plaintext TCP** | Not supported, by design. A `tcp_bind` without valid TLS config is a fatal startup error. |

## See also

- [Configuration reference](configuration.md) — full option descriptions
  for all five TLS config keys.
- [Threat model](../security/threat-model.md) — how the TCP path fits into
  the overall weir security model.
- [`crates/weir-client/examples/push_tls.rs`](https://github.com/miki-przygoda/weir/blob/main/crates/weir-client/examples/push_tls.rs)
  — runnable TLS client example.
