//! Synchronous blocking client for the weir daemon.
//!
//! Connects over a Unix domain socket (or TCP + mutual TLS behind the `tls`
//! feature) and exchanges frames using the weir wire protocol v1. Each method
//! issues one request and reads one response — no pipelining. For concurrent
//! producers, create one [`WeirClient`] per thread.
//!
//! # Throughput: one record per round-trip
//!
//! [`push`](WeirClient::push) is synchronous: it sends one frame and blocks for
//! the ack before returning. So a **single** client's throughput is bounded by
//! the round-trip time — roughly `1 / RTT` records/sec on one connection
//! (e.g. a ~50 µs Unix-socket RTT caps a single client near ~20k rec/s; a
//! `Sync`-tier push also waits on the daemon's fsync). To go faster, **fan out
//! across connections**: create one `WeirClient` per producer thread (they're
//! independent and the daemon handles many concurrently). Ordering is only
//! guaranteed within a single connection's sequential pushes, not across
//! connections. A built-in batched-push frame and a pooled client are a possible
//! future (2.0) wire-protocol addition; today, parallel connections are the way
//! to scale a producer.
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(unix)] {
//! use weir_client::{WeirClient, Durability}; // Durability is re-exported from weir-core
//!
//! let mut client = WeirClient::connect("/run/weir/weir.sock").unwrap();
//! client.push(b"hello world", Durability::Batched).unwrap();
//! # }
//! ```
//!
//! # Ack vs. delivery
//!
//! A successful [`push`](WeirClient::push) means the record is **durably buffered
//! at the requested [`Durability`] tier** — fsync'd to the write-ahead buffer for
//! [`Sync`](Durability::Sync)/[`Batched`](Durability::Batched), in memory for
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
//! `weir_records_accepted_total{tier="sync"}`,
//! `weir_records_ack_total{tier="sync"}`, and
//! `weir_records_nack_total{tier="sync",reason="empty_payload"}`.
#![deny(missing_docs)]

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub use unix::{ClientError, WeirClient};

/// Re-export of [`weir_core::Durability`] so the common producer path needs a
/// single crate import (`weir_client::Durability`).
pub use weir_core::Durability;

/// Re-export of [`weir_core::NackReason`] — the payload of [`ClientError::Nack`].
/// Re-exported so consumers can match on the reason (e.g. to distinguish the
/// connection-closing Nacks) without taking a direct dependency on `weir-core`.
pub use weir_core::NackReason;

#[cfg(all(unix, feature = "tls"))]
mod tls;

#[cfg(all(unix, feature = "tls"))]
pub use tls::{ClientTlsConfig, TlsStream};
