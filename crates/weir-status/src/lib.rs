//! The weir status model (`weir.status/v1`, spec §5.1, §9).
//!
//! One pure evaluator turns `Evidence` into `Evaluation`; every consumer
//! (`/status`, `/readyz`, `/livez`, the admin `status` verb, the
//! `weir_condition_active` gauge, `weir-ctl status`, the readiness script)
//! calls it, so their answers cannot drift. Missing evidence makes a condition
//! *unevaluated*, never "false". Nothing here performs I/O or reads a clock.
#![forbid(unsafe_code)]

pub mod admin;
pub mod codes;
pub mod condition;
pub mod config;
pub mod doc;
pub mod evidence;
pub mod exposition;
mod probe;
mod public;
mod rules;
pub mod segment;
mod summary;
#[doc(hidden)]
pub mod testing;

pub use codes::{
    ALL, Affects, CATALOGUE, CatalogueEntry, ConditionCode, Gate, OFFLINE_ONLY, Severity, entry,
};
pub use condition::*;
pub use config::{ConfigValue, PublicConfig};
pub use doc::*;
pub use evidence::*;
pub use exposition::{active_conditions_from_metrics, evidence_from_metrics};
pub use probe::*;
pub use public::{
    MAX_PUBLIC_CONDITIONS, MAX_PUBLIC_SHARDS, PublicAdminSurface, PublicCondition, PublicDrainView,
    PublicEvidenceValue, PublicReloadItem, PublicReloadLast, PublicReloadView, PublicRequeueView,
    PublicSinkFailure, PublicSinkView, PublicStatusDoc, public,
};
pub use rules::evaluate;
pub use segment::SegmentId;
