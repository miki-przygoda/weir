//! S3-API object-storage sink for the [weir] daemon.
//!
//! [weir]: https://github.com/miki-przygoda/weir
#![deny(missing_docs)]

pub(crate) mod client;
pub(crate) mod creds;
pub(crate) mod framing;
pub(crate) mod key;
pub(crate) mod redact;
pub(crate) mod sigv4;
pub(crate) mod time;
