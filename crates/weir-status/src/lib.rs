//! The weir status model (`weir.status/v1`, spec §5.1, §9).
//!
//! One pure evaluator turns `Evidence` into `Evaluation`; every consumer
//! (`/status`, `/readyz`, `/livez`, the admin `status` verb, the
//! `weir_condition_active` gauge, `weir-ctl status`, the readiness script)
//! calls it, so their answers cannot drift. Missing evidence makes a condition
//! *unevaluated*, never "false". Nothing here performs I/O or reads a clock.
#![forbid(unsafe_code)]

pub mod codes;
pub mod config;
pub mod evidence;
pub mod segment;

pub use codes::{
    ALL, Affects, CATALOGUE, CatalogueEntry, ConditionCode, Gate, OFFLINE_ONLY, Severity, entry,
};
pub use config::{ConfigValue, PublicConfig};
pub use evidence::*;
pub use segment::SegmentId;
