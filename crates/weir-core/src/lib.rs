//! Core wire-protocol types shared by the weir daemon, client, and sink SDK.
//!
//! `weir-core` is the dependency-light foundation of the weir workspace: it
//! defines the v1 binary frame format and nothing else (no I/O, no async, no
//! daemon logic). Everything that crosses the socket is built from these types.
//!
//! # What's here
//!
//! - [`Envelope`] / [`Header`] — a complete wire frame and its 16-byte header,
//!   with `encode`/`decode` that own the CRC and validation order.
//! - [`MessageType`] / [`Durability`] / [`NackReason`] — the fixed-repr enums
//!   carried in the header and Nack payloads, each with a `TryFrom<u8>` whose
//!   error is [`UnknownMessageType`] / [`UnknownDurability`] / [`UnknownNackReason`]
//!   (re-exported here alongside the enums).
//! - [`RecordCoordinate`] — where an accepted record landed in the WAB, carried
//!   back by an [`AckTracked`](MessageType::AckTracked) frame.
//! - [`Payload`] — opaque, ref-counted payload bytes (O(1) clone).
//! - [`DecodeError`] / [`WeirError`] — the decode failure taxonomy.
//! - [`WIRE_VERSION`] / [`MAX_PAYLOAD_HARD_CAP`] / [`HEADER_LEN`] /
//!   [`MIN_FRAME_LEN`] — the protocol constants.
//!
//! The wire format itself is specified in `docs/wire_protocol.md`; this crate
//! is its executable reference (see `tests/reference_frames.rs`).
#![deny(missing_docs)]

pub mod coordinate;
pub mod durability;
pub mod envelope;
pub mod error;
pub mod nack;
pub mod payload;
pub mod version;

pub use coordinate::{
    COORDINATE_FIXED_LEN, COORDINATE_VERSION, CoordinateError, MAX_SEGMENT_NAME_LEN,
    MAX_TRACKED_ACK_PAYLOAD_LEN, RecordCoordinate,
};
pub use durability::{Durability, UnknownDurability};
pub use envelope::{Envelope, HEADER_LEN, Header, MIN_FRAME_LEN, MessageType, UnknownMessageType};
pub use error::{DecodeError, WeirError};
pub use nack::{NackReason, UnknownNackReason};
pub use payload::Payload;
pub use version::{MAX_PAYLOAD_HARD_CAP, WIRE_VERSION};
