//! Tamper-evident hash chain over sealed weir WAB segments.
//!
//! # What this proves, and what it does not
//!
//! A CRC32 is an error-detection code, not a MAC: anyone who edits a record can
//! recompute the CRC in microseconds and the file reads back clean. This crate
//! chains sealed segments so that editing one is *detectable* — provided the
//! chain head was observed somewhere the editor does not control.
//!
//! It does **not** defend against a process running as the daemon UID (already
//! out of scope in the threat model — such a process can rewrite the segment,
//! the chain, and the log line), it is **not** non-repudiation (a chain is not a
//! signature), and it **detects rather than prevents**.
//!
//! The honest one-line description is: *prove a sealed buffer was not edited
//! after the fact.*
//!
//! Nothing here runs on the ack path. Chains are computed over segments that
//! are already sealed, already fsynced, and already acked.

#![deny(missing_docs)]

pub mod chain;

pub use chain::{ChainHead, DOMAIN_SEP};

/// Why an attest operation failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AttestError {
    /// A hex string was not 64 characters.
    BadHexLength {
        /// The length seen.
        len: usize,
    },
    /// A hex string contained a non-hex character.
    BadHexDigit,
    /// Underlying I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for AttestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadHexLength { len } => {
                write!(f, "chain head must be 64 hex characters, got {len}")
            }
            Self::BadHexDigit => write!(f, "chain head contains a non-hex character"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for AttestError {}

impl From<std::io::Error> for AttestError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
