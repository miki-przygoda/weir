# TCP + mTLS Listener — Design Spec

- **Status:** Approved (design phase), pre-implementation
- **Date:** 2026-06-10
- **Author:** Mikolaj (with Claude Code)
- **Supersedes/closes:** the "TCP listener + TLS + auth" item deferred in `PLAN.md`
  ("No TCP in v1", "TCP+TLS+auth is a v0.2 bundle")

## 1. Goal & threat model

Add a TCP listener to `weir-server` so producers on **other hosts, across an
untrusted network** can push records. None of the trust the Unix socket relies
on (file mode `0o600`, `SO_PEERCRED`/`getpeereid` peer-uid check) exists over
TCP, so the TCP path derives its trust **entirely from mutual TLS**:

- The daemon authenticates every client via a **client certificate** that must
  be signed by an operator-run CA the daemon is configured to trust.
- The client authenticates the **server** certificate against the same (or a
  configured) CA — mutual.
- **Plain, unencrypted TCP is never exposed.** The TCP listener only comes into
  existence when TLS is configured; there is no plaintext-TCP code path.

Threat model: a hostile network where any party can open a TCP connection.
Defenses: mandatory mTLS (no anonymous access), a dedicated TLS-handshake
timeout (handshake-slowloris), a shared global connection cap, a uniform-drop
policy on rejected handshakes (no error oracle), and all existing per-frame
DoS hardening (CRC-before-alloc, payload caps, read timeout) carried over
unchanged.

## 2. Decisions (locked during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Driving use case | Remote producers, **hostile net** | Mandates mTLS, max hardening |
| Auth model | **Mutual TLS, client cert required** | Replaces filesystem trust |
| Trust model | **CA-signed**: trust a CA root, accept any client cert it signed | Scales to many producers; issuance = authorization; rotation via re-issue |
| Topology | **Unix + TCP concurrently** | Local/admin over Unix, remote over TCP+mTLS; both feed one queue |
| TLS library | **rustls** via `tokio-rustls` (server), blocking `rustls` (client) | Pure Rust, no OpenSSL; ecosystem norm |
| Handler strategy | **Approach A — generic over the stream** | Single audited frame parser; zero-cost monomorphization |
| Client connector | **In scope** for this bundle | A server with mTLS and no matching client is untestable/unusable |
| Hot cert reload | **In scope** (folded in) — SIGHUP reloads cert/key/CA | Short-lived certs rotate often on a hostile net; restart-on-rotation drain window is unacceptable there |
| Per-connection durability default | **In scope** (folded in) — set at connect time | Closes a standing PLAN open question; natural to add alongside the new client connector |

## 3. Architecture

Two accept loops, one shared hardened connection handler, one shared work queue.

```
weir-server/src/socket/
  mod.rs          # Unix accept loop (existing) + shared spawn/limit/shutdown machinery
  connection.rs   # handle_connection<S> — GENERIC over the stream (refactored, Approach A)
  peer.rs         # Unix peer-uid check (existing, unchanged)
  tcp.rs          # NEW — TCP accept loop: bind, TLS handshake, hand TlsStream to the handler
  tls.rs          # NEW — builds rustls ServerConfig (server identity + client-CA verifier)
```

Both loops:
- feed the same `QueueSender<WorkUnit>`,
- share the same connection-limit `Semaphore` (`max_connections`),
- share the same round-robin shard assignment (`counter % shard_count`),
- share the same graceful-shutdown `watch` channel.

The TCP loop is spawned **only** when `tcp_bind` is configured. Auth is an
accept-loop concern (as it already is for Unix): the Unix loop runs the
peer-uid check, the TCP loop runs the mTLS handshake, and **only an
already-authenticated stream reaches `handle_connection`.** The handler stays
identity-agnostic.

## 4. Component: generic handler refactor (`connection.rs`)

Change the four `UnixStream`-typed signatures to a generic stream:

- `handle_connection(stream: UnixStream, …)`
  → `handle_connection<S>(stream: S, …) where S: AsyncRead + AsyncWrite + Unpin + Send`
- the internal read loop (`stream: &mut UnixStream`), `send_ack`, `send_nack`
  take `&mut S` (or a generic helper bound) accordingly.

**No behavioural change.** Every existing DoS-hardening property is
transport-agnostic and carries over verbatim. The existing connection unit
tests use `tokio::net::UnixStream::pair()`, which satisfies the new bound, so
they run unchanged and act as the regression guard for the refactor. This is
the only edit to audited trust-boundary code and gets a focused review.

## 5. Component: TLS server config (`tls.rs`)

Builds a `rustls::ServerConfig`, held behind an **`ArcSwap<ServerConfig>`** so it
can be hot-swapped on SIGHUP (see §6.1). The TCP accept loop reads the current
`Arc<ServerConfig>` at each accept and wraps it in a per-accept `TlsAcceptor`
(an `Arc` clone — cheap), so every new handshake uses the latest config while
in-flight sessions keep theirs. The builder:

1. Load server identity: `tls_cert_path` (PEM cert chain) + `tls_key_path`
   (PEM private key).
2. Load `tls_client_ca_path` (CA root PEM) into a `RootCertStore`.
3. Build a `WebPkiClientVerifier` from that store that **requires** a client
   certificate (`.build()`, not `.allow_unauthenticated()`). A handshake with
   no client cert, or a client cert not chaining to the configured CA, fails at
   the TLS layer before any frame is read.
4. Use rustls safe defaults (TLS 1.2 + 1.3, strong cipher suites only); TLS 1.3
   preferred. The TLS-1.2 decision is documented, not force-disabled
   (compatibility; rustls's 1.2 suites are already hardened).

Errors (missing file, malformed PEM, key/cert mismatch, empty CA store) surface
as a fatal startup config error with a clear message.

## 6. Component: TCP accept loop (`tcp.rs`)

Per accepted connection:

1. `TcpListener::bind(tcp_bind)`, accept loop.
2. Acquire a `max_connections` semaphore permit (shared with Unix).
3. Set `TCP_NODELAY` (latency posture).
4. Run the TLS handshake via the shared `TlsAcceptor`, wrapped in a
   `tls_handshake_timeout_secs` timeout.
5. On success: extract the client cert CN (best-effort, for the tracing span),
   then call the shared `handle_connection` with the `TlsStream`.
6. On failure (no cert / wrong CA / expired / malformed / timeout): increment
   `weir_tls_handshake_failures_total{reason}`, drop the connection uniformly
   (no error detail returned to the peer), release the permit.

Participates in the graceful-shutdown `watch` channel exactly like the Unix loop.

## 6.1 Hot cert reload (SIGHUP)

mTLS certs on a hostile net are short-lived and rotate often. v1 reloads the
TLS material **without dropping connections** so rotation needs no restart:

- A `SIGHUP` handler (tokio `signal::unix`) re-runs the §5 builder against the
  **same configured paths** (`tls_cert_path` / `tls_key_path` /
  `tls_client_ca_path`) and atomically `store`s the new `Arc<ServerConfig>` into
  the `ArcSwap`.
- **Scope is TLS material only.** This is *not* general config reload — every
  other setting stays read-once-at-startup (consistent with the documented "no
  SIGHUP config reload" stance). The docs will be explicit that SIGHUP reloads
  TLS certs and nothing else.
- **Reload is fail-safe.** If the new material is missing/malformed/mismatched,
  the swap does **not** happen: the daemon keeps serving the previous config,
  logs the error, and increments `weir_tls_config_reloads_total{outcome="failed"}`.
  A successful reload increments `…{outcome="ok"}`. The listener is never torn
  down by a bad reload.
- **In-flight connections are unaffected** — their TLS session is already
  negotiated; only handshakes *after* the swap use the new material.
- No-op when `tcp_bind` is unset (no TLS configured) — SIGHUP is ignored with a
  debug log.

## 7. Security hardening (hostile-net specifics)

- **mTLS mandatory** — zero anonymous access on the TCP path.
- **Handshake timeout** (`tls_handshake_timeout_secs`, default `10`) — separate
  from the frame read timeout; caps handshake-slowloris that would otherwise
  hold a permit indefinitely.
- **Shared connection cap** — TCP and Unix draw from one `max_connections`
  semaphore; neither transport can exceed the global bound or fully starve the
  other beyond it.
- **No error oracle** — rejected handshakes get a uniform drop.
- **Explicit opt-in bind** — `tcp_bind` has no default; the bound address is
  logged loudly at startup.
- **No silent plaintext fallback** — `tcp_bind` set without valid TLS config, or
  without the `tls` feature compiled in, is a fatal error, never a downgrade.
- All existing per-frame hardening (header-CRC-before-alloc, payload caps,
  slowloris read timeout) applies identically over TLS.

## 8. Config surface

Merged with the existing precedence: **CLI > env > TOML > default.** New keys:

| Option | Type | Default | Notes |
|---|---|---|---|
| `tcp_bind` | `Option<SocketAddr>` | `None` | e.g. `0.0.0.0:7100`. `None` ⇒ no TCP listener. |
| `tls_cert_path` | `PathBuf` | — | server cert chain (PEM); required iff `tcp_bind` set |
| `tls_key_path` | `PathBuf` | — | server private key (PEM); required iff `tcp_bind` set |
| `tls_client_ca_path` | `PathBuf` | — | CA root for client-cert verification (PEM); required iff `tcp_bind` set |
| `tls_handshake_timeout_secs` | `u64` | `10` | TLS handshake deadline |

Startup validation: if `tcp_bind` is set, all three TLS paths must be present
and readable **and** the `tls` feature must be compiled in; otherwise fatal
config error. CLI flag + env var + TOML key for each, following the existing
naming conventions in `config/`.

## 9. Component: client-side TLS connector (`weir-client`)

- Generalize the client's inner stream over `Read + Write` (mirrors the server's
  Approach A). Existing `WeirClient::connect(path)` and `from_stream` keep
  working unchanged (no public break for current users).
- Add, behind a new default-off `tls` feature on `weir-client`:
  ```rust
  WeirClient::connect_tls(addr, ClientTlsConfig {
      client_cert, client_key, ca_cert, server_name,
  }) -> Result<Self, ClientError>
  ```
  using blocking `rustls::StreamOwned<ClientConnection, TcpStream>`.
- The client validates the **server** cert against `ca_cert` (mutual auth).
- Add a `push_tls` example alongside the existing `push_simple`.

### 9.1 Per-connection durability default

Closes the standing PLAN open question. Both constructors gain an optional
connect-time default tier:

- `WeirClient` stores an optional `default_durability: Option<Durability>` set at
  connect time (a builder arg / config field on both `connect` and
  `connect_tls`; `None` preserves today's behaviour).
- Add `push_default(payload)` — pushes at the connection's default tier; errors
  clearly if no default was set.
- Existing `push(payload, durability)` is unchanged — an explicit per-call tier
  always overrides the connection default. The wire envelope's per-record
  DURABILITY byte is unaffected; this is purely client-side ergonomics so a
  producer that always wants e.g. `Batched` doesn't repeat it on every call.

## 10. Observability

- `weir_tls_handshake_failures_total{reason}` — reason ∈ {`no_client_cert`,
  `bad_cert`, `timeout`, `other`}.
- `weir_tls_config_reloads_total{outcome}` — outcome ∈ {`ok`, `failed`} (SIGHUP
  reload accounting, §6.1).
- A `transport` label (`unix` | `tls`) on connection-accept accounting.
- Client cert CN attached to the per-connection tracing span (best-effort).

## 11. Feature gating & dependencies

- New `tls` cargo feature on **`weir-server`** (default **off**) and
  **`weir-client`** (default **off**).
- Server deps (under `tls`): `tokio-rustls` (pulls `rustls`),
  `rustls-pemfile`, `rustls-pki-types`, `arc-swap` (hot-reloadable
  `ServerConfig`, §6.1).
- Client deps (under `tls`): `rustls`, `rustls-pemfile`, `rustls-pki-types`.
- Dev-dependency: `rcgen` — generates throwaway CA/server/client certs in tests.
- Feature off ⇒ `tcp.rs`/`tls.rs` are not compiled; `weir-server` is Unix-only
  exactly as today. CI gains a `--features tls` build + test pass (Linux, and
  the cross-platform build check; the TCP integration tests run on Linux).

## 12. Testing

- **Unit (`tls.rs`):** config builder loads valid certs; rejects missing/garbage
  CA, key/cert mismatch, empty CA store.
- **Integration** (throwaway CA + server + client certs via `rcgen`):
  1. valid client cert → 1000 pushes all ACKed (parity with the Unix load test);
  2. **no** client cert → handshake rejected, no frames parsed;
  3. client cert from a **different** CA → rejected;
  4. **expired** client cert → rejected;
  5. handshake-timeout slowloris → permit released, `…handshake_failures_total{reason="timeout"}` incremented;
  6. both listeners up → a Unix push and a TLS push land in the same queue and both ACK.
  7. **SIGHUP reload, success** → swap to a freshly-issued server cert; a new
     handshake uses it while an in-flight connection is unaffected;
     `…config_reloads_total{outcome="ok"}` increments.
  8. **SIGHUP reload, failure** → point at a malformed/missing cert; the daemon
     keeps the old config, stays up, `…{outcome="failed"}` increments.
- **Client (`weir-client`):** `default_durability` set at connect time →
  `push_default` uses it; per-call `push(payload, tier)` overrides it;
  `push_default` with no default set errors clearly.
- Extend the `weir-testkit` harness with a TLS server variant (cert fixtures +
  a `weir_server_tls(...)` helper).

## 13. Out of scope (this bundle) — tracked for v2

- **CN/SAN authorization allowlist** — CA issuance is the authorization gate;
  CN allowlisting is a documented v2 extension.
- **CRL / OCSP revocation** — v1 revokes by rotating the CA / re-issuing.
  Online revocation is v2. *(Confirmed acceptable for the threat model.)*
- **General config hot-reload** — SIGHUP reloads TLS material **only** (§6.1);
  all other settings remain read-once-at-startup.
- **Plain (non-TLS) TCP** — never exposed, by design.

*(Folded into this bundle after the design review: hot cert reload via SIGHUP
(§6.1) and the per-connection durability default (§9.1) — both were originally
deferred.)*

## 14. Acceptance criteria

- `weir-server --features tls` builds; default build unchanged and still Unix-only.
- With `tcp_bind` + valid TLS config, a CA-signed client pushes records over TLS
  end-to-end and they drain through the normal pipeline.
- A client with no cert / a wrong-CA cert / an expired cert is rejected at the
  handshake; the daemon stays up and the failure is counted in metrics.
- Unix and TCP listeners run concurrently against one queue.
- `tcp_bind` set without valid TLS config (or without the `tls` feature) is a
  fatal startup error, never a plaintext downgrade.
- **SIGHUP** swaps in freshly-rotated certs with no connection drop; a bad
  reload leaves the daemon serving the previous certs and is counted.
- **Client** can set a default durability at connect time; `push_default` uses
  it and per-call `push` overrides it.
- All existing tests pass unchanged; new TLS tests pass under `--features tls`.
- Docs updated: configuration reference (new keys), a TCP+mTLS operations page,
  and a threat-model note for the network path.
