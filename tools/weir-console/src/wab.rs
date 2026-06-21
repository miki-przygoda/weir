//! Read-only core for the WAB Explorer. Pure functions over a wab directory;
//! no HTTP, no mutation. The axum handlers (server.rs) are thin wrappers.
use std::path::{Path, PathBuf};
use serde::Serialize;
use weir_wab::format::{
    self, ConfirmedMeta, SegmentFooterMeta, SegmentHeaderMeta, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN,
    EXT_ACTIVE, EXT_SEALED, EXT_CONFIRMED,
};
use weir_wab::{list_segment_files, verify_sealed_segment, SegmentReader, SegmentState, SegmentVerifyError};

/// Errors surfaced to HTTP as 4xx/5xx with a JSON `{error}` body.
#[derive(Debug)]
pub enum WabError {
    /// The request path escaped the wab dir (`..`, absolute) — 400.
    BadPath(String),
    /// Underlying I/O (missing dir, unreadable file) — 500.
    Io(std::io::Error),
}
impl std::fmt::Display for WabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WabError::BadPath(p) => write!(f, "invalid path: {p}"),
            WabError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}
impl From<std::io::Error> for WabError { fn from(e: std::io::Error) -> Self { WabError::Io(e) } }

const PREVIEW_BYTES: usize = 256;

#[derive(Serialize)]
pub struct HeaderJson { pub format_version: u8, pub shard_id: u16, pub created_at: i64 }
impl From<&SegmentHeaderMeta> for HeaderJson {
    fn from(h: &SegmentHeaderMeta) -> Self {
        HeaderJson { format_version: h.format_version, shard_id: h.shard_id, created_at: h.created_at }
    }
}
#[derive(Serialize)]
pub struct FooterJson { pub record_count: u64, pub data_bytes: u64, pub file_crc32: String, pub sealed_at: i64 }
impl From<&SegmentFooterMeta> for FooterJson {
    fn from(ft: &SegmentFooterMeta) -> Self {
        FooterJson { record_count: ft.record_count, data_bytes: ft.data_bytes,
            file_crc32: format!("{:#010x}", ft.file_crc32), sealed_at: ft.sealed_at }
    }
}
#[derive(Serialize)]
pub struct ConfirmedJson { pub sealed_at: i64, pub record_count: u64, pub drained_at: i64 }
impl From<&ConfirmedMeta> for ConfirmedJson {
    fn from(c: &ConfirmedMeta) -> Self {
        ConfirmedJson { sealed_at: c.sealed_at, record_count: c.record_count, drained_at: c.drained_at }
    }
}
/// `{ok: true}` or `{ok: false, kind, expected?, computed?, detail?}`.
#[derive(Serialize)]
pub struct IntegrityJson {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")] pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub computed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub detail: Option<String>,
}
impl IntegrityJson {
    pub fn ok() -> Self { IntegrityJson { ok: true, kind: None, expected: None, computed: None, detail: None } }
    pub fn from_err(e: &SegmentVerifyError) -> Self {
        let (kind, expected, computed, detail) = match e {
            SegmentVerifyError::Io(io) => ("Io", None, None, Some(io.to_string())),
            SegmentVerifyError::Header(h) => ("Header", None, None, Some(h.to_string())),
            SegmentVerifyError::TooShort => ("TooShort", None, None, None),
            SegmentVerifyError::BadRecord(io) => ("BadRecord", None, None, Some(io.to_string())),
            SegmentVerifyError::NoSentinel => ("NoSentinel", None, None, None),
            SegmentVerifyError::MissingFooter => ("MissingFooter", None, None, None),
            SegmentVerifyError::CrcMismatch { expected, computed } =>
                ("CrcMismatch", Some(format!("{expected:#010x}")), Some(format!("{computed:#010x}")), None),
            SegmentVerifyError::TrailingBytes => ("TrailingBytes", None, None, None),
            _ => ("Unknown", None, None, None), // SegmentVerifyError is #[non_exhaustive]
        };
        IntegrityJson { ok: false, kind: Some(kind.to_string()), expected, computed, detail }
    }
}

/// Resolve a request-relative path against the wab dir, rejecting any escape.
pub fn safe_join(wab_dir: &Path, rel: &str) -> Result<PathBuf, WabError> {
    let candidate = Path::new(rel);
    if candidate.is_absolute() || candidate.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
        return Err(WabError::BadPath(rel.to_string()));
    }
    Ok(wab_dir.join(candidate))
}

/// Classify a SegmentState into the wire string.
pub fn state_str(s: &SegmentState) -> &'static str {
    match s { SegmentState::Active => "active", SegmentState::Sealed => "sealed", SegmentState::Confirmed => "confirmed" }
}

// (inventory / records / dead_letter / verify added in later tasks)
