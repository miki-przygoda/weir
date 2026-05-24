//! Synchronous blocking client for the weir daemon.
//!
//! Connects over a Unix domain socket and exchanges frames using the weir wire
//! protocol v1. Each method issues one request and reads one response — no
//! pipelining. For concurrent producers, create one `WeirClient` per thread.
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(unix)] {
//! use weir_client::WeirClient;
//! use weir_core::Durability;
//!
//! let mut client = WeirClient::connect("/run/weir/weir.sock").unwrap();
//! client.push(b"hello world", Durability::Batched).unwrap();
//! # }
//! ```

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub use unix::{ClientError, WeirClient};
