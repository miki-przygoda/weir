//! Synchronous blocking client for the weir daemon.
//!
//! Connects over a Unix domain socket (or TCP + mutual TLS behind the `tls`
//! feature) and exchanges frames using the weir wire protocol v1. Each method
//! issues one request and reads one response — no pipelining. For concurrent
//! producers, create one [`WeirClient`] per thread.
//!
//! # Platforms
//!
//! The Unix-socket transport is, unsurprisingly, Unix-only. **The TCP + mutual
//! TLS transport is not** — it builds wherever rustls does, Windows included,
//! as of 2.0.3. Before then it was gated on `unix` by accident: it lives in its
//! own module but imported two types that happened to sit in the Unix one.
//!
//! # Throughput: one record per round-trip
//!
//! [`push`](WeirClient::push) is synchronous: it sends one frame and blocks for
//! the ack before returning, so one connection carries one record at a time.
//! What that costs depends on the tier, and the two are bounded by different
//! things:
//!
//! - **`Buffered`** is round-trip bound. The daemon acks from memory, so the
//!   ceiling is roughly `1 / RTT`.
//! - **`Durable`** is fsync bound, not RTT bound. The ack waits on the daemon's
//!   fsync, which dominates the round trip by orders of magnitude — batching at
//!   the daemon amortises it across concurrent producers, which is why fanning
//!   out helps this tier *more* than it helps `Buffered`.
//!
//! Measured on one machine (Apple M3 Max, Unix socket, 256-byte records): a bare
//! AF_UNIX round trip is **~7 µs**, and a single client reaches **~32,000 rec/s
//! `Buffered`** and **~5,700 rec/s `Durable`**. On a 2-vCPU CI runner the same
//! figures are ~13,900 and ~3,700. Treat these as the shape of the problem, not
//! as a spec: they move with hardware, filesystem, and record size. An earlier
//! version of this paragraph quoted "~50 µs RTT" and "~20k rec/s", which matched
//! neither tier on any machine we have measured.
//!
//! To go faster, **fan out across connections**: create one `WeirClient` per
//! producer thread (they're independent and the daemon handles many
//! concurrently). Ordering is only guaranteed within a single connection's
//! sequential pushes, not across connections.
//!
//! **A pooled client needs no protocol change at all** — the wire protocol
//! already permits several frames in flight on one connection, and fanning out
//! across connections is measurable today: roughly 5× on `Buffered` at 16
//! connections and 9× on `Durable` at 48. A built-in batched-push *frame* is the
//! part that needs new wire machinery. An earlier version of this paragraph
//! attributed the protocol cost to both and so steered readers away from the one
//! option that already works.
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(unix)] {
//! use weir_client::{WeirClient, Durability}; // Durability is re-exported from weir-core
//!
//! let mut client = WeirClient::connect("/run/weir/weir.sock").unwrap();
//! client.push(b"hello world", Durability::Durable).unwrap();
//! # }
//! ```
//!
//! # Ack vs. delivery
//!
//! A successful [`push`](WeirClient::push) means the record is **durably buffered
//! at the requested [`Durability`] tier** — fsync'd to the write-ahead buffer for
//! [`Durable`](Durability::Durable), in memory for
//! [`Buffered`](Durability::Buffered). It does **not** mean the record has reached
//! your downstream sink yet: the daemon drains buffered records to the sink in
//! batches, only once a WAB segment seals (its size threshold, or daemon
//! shutdown). For a small smoke test the sink may not be touched at all — watch
//! `weir_records_ack_total` (acceptance), not the sink-commit metric, to confirm
//! the daemon took your records.
//!
//! # Running the daemon
//!
//! This crate is the producer side; the daemon is the `weir-server` binary:
//!
//! ```text
//! mkdir -p /run/weir/wab            # the daemon does not create its directories
//! weir-server --wab-dir /run/weir/wab --socket-path /run/weir/weir.sock
//! ```
//!
//! `--wab-dir` must already exist. On macOS, do not place the socket directly in
//! `/tmp` (it is a symlink the hardened bind rejects) — use a dedicated `0700`
//! directory. Run `weir-server --help` for the full option list.
//!
//! # Observability
//!
//! The daemon serves Prometheus metrics at `127.0.0.1:9185/metrics` by default.
//! The counters a producer cares about are labelled by tier/reason, e.g.
//! `weir_records_accepted_total{tier="durable"}`,
//! `weir_records_ack_total{tier="durable"}`, and
//! `weir_records_nack_total{tier="durable",reason="empty_payload"}`.
#![deny(missing_docs)]

/// Transport-agnostic client core. Split out of `unix` so that `tls` — which
/// needs a `TcpStream` and rustls, and nothing Unix-specific — is gated on the
/// feature alone rather than on the platform.
mod client;

pub use client::{ClientError, DefaultTransport, WeirClient};

#[cfg(unix)]
mod unix;

/// Re-export of [`weir_core::Durability`] so the common producer path needs a
/// single crate import (`weir_client::Durability`).
pub use weir_core::Durability;

/// Re-export of [`weir_core::NackReason`] — the payload of [`ClientError::Nack`].
/// Re-exported so consumers can match on the reason (e.g. to distinguish the
/// connection-closing Nacks) without taking a direct dependency on `weir-core`.
pub use weir_core::NackReason;

#[cfg(feature = "tls")]
mod tls;

#[cfg(feature = "tls")]
pub use tls::{ClientTlsConfig, TlsStream};
