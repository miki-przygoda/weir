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

#[derive(Serialize)]
pub struct SegmentJson {
    pub shard: Option<u16>,
    pub file: String,           // path relative to wab_dir
    pub state: String,          // active|sealed|confirmed
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")] pub header: Option<HeaderJson>,
    #[serde(skip_serializing_if = "Option::is_none")] pub footer: Option<FooterJson>,
    #[serde(skip_serializing_if = "Option::is_none")] pub integrity: Option<IntegrityJson>,
    #[serde(skip_serializing_if = "Option::is_none")] pub confirmed: Option<ConfirmedJson>,
}

#[derive(Serialize, Default)]
pub struct Totals { pub segments: usize, pub sealed: usize, pub active: usize, pub confirmed: usize, pub dead_letter: usize, pub total_bytes: u64 }

#[derive(Serialize)]
pub struct SegmentsResponse { pub wab_dir: String, pub totals: Totals, pub segments: Vec<SegmentJson> }

fn shard_of(rel: &Path) -> Option<u16> {
    // shard_NN/<file> -> NN
    rel.components().next().and_then(|c| c.as_os_str().to_str())
        .and_then(|s| s.strip_prefix("shard_")).and_then(|n| n.parse().ok())
}

/// Read header meta best-effort (None if the file can't be opened/parsed).
fn header_of(path: &Path) -> Option<HeaderJson> {
    SegmentReader::open(path).ok().map(|r| HeaderJson::from(r.header()))
}

/// Collect every segment file under a wab dir. `list_segment_files` is
/// single-level (it does not recurse), but a real weir lays segments out under
/// per-shard subdirectories (`shard_NN/seg_*.wab*`). So scan the wab dir itself
/// (segments at the root, if any) plus each immediate `shard_*` subdirectory.
fn list_all_segment_files(wab_dir: &Path) -> Result<Vec<(PathBuf, SegmentState)>, WabError> {
    let mut all = list_segment_files(wab_dir)?;
    for entry in std::fs::read_dir(wab_dir)? {
        let path = entry?.path();
        let is_shard = path.is_dir()
            && path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("shard_"));
        if is_shard {
            all.extend(list_segment_files(&path)?);
        }
    }
    Ok(all)
}

pub fn inventory(wab_dir: &Path) -> Result<SegmentsResponse, WabError> {
    let mut segments = Vec::new();
    let mut totals = Totals::default();
    // dead-letter count (segments under dead_letter/, listed via its own endpoint)
    let dl = wab_dir.join("dead_letter");
    if dl.is_dir() {
        totals.dead_letter = list_segment_files(&dl).unwrap_or_default()
            .into_iter().filter(|(_, st)| matches!(st, SegmentState::Sealed | SegmentState::Active)).count();
    }
    for (path, state) in list_all_segment_files(wab_dir)? {
        let rel = path.strip_prefix(wab_dir).unwrap_or(&path).to_path_buf();
        let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut seg = SegmentJson {
            shard: shard_of(&rel), file: rel.to_string_lossy().into_owned(),
            state: state_str(&state).to_string(), size_bytes,
            header: None, footer: None, integrity: None, confirmed: None,
        };
        match state {
            SegmentState::Active => { totals.active += 1; seg.header = header_of(&path); }
            SegmentState::Sealed => {
                totals.sealed += 1;
                match verify_sealed_segment(&path) {
                    Ok(v) => { seg.header = Some(HeaderJson::from(&v.header)); seg.footer = Some(FooterJson::from(&v.footer)); seg.integrity = Some(IntegrityJson::ok()); }
                    Err(e) => { seg.header = header_of(&path); seg.integrity = Some(IntegrityJson::from_err(&e)); }
                }
            }
            SegmentState::Confirmed => {
                totals.confirmed += 1;
                if let Ok(buf) = std::fs::read(&path) {
                    if let Ok(c) = format::parse_confirmed(&buf) { seg.confirmed = Some(ConfirmedJson::from(&c)); }
                }
            }
        }
        segments.push(seg);
    }
    // Confirmed sidecars are metadata for their sealed segment, not standalone segments:
    // attach each confirmed sidecar's meta onto the matching sealed segment, then drop the
    // standalone confirmed entries from the segment count.
    let confirmed: Vec<(String, ConfirmedJson)> = segments.iter()
        .filter(|s| s.state == "confirmed")
        .filter_map(|s| s.confirmed.as_ref().map(|c| (s.file.replace(EXT_CONFIRMED, EXT_SEALED), ConfirmedJson { sealed_at: c.sealed_at, record_count: c.record_count, drained_at: c.drained_at })))
        .collect();
    for (sealed_file, c) in confirmed {
        if let Some(s) = segments.iter_mut().find(|s| s.file == sealed_file) { s.confirmed = Some(c); }
    }
    segments.retain(|s| s.state != "confirmed");
    totals.segments = segments.len();
    totals.total_bytes = segments.iter().map(|s| s.size_bytes).sum();
    Ok(SegmentsResponse { wab_dir: wab_dir.to_string_lossy().into_owned(), totals, segments })
}

#[derive(Serialize)]
pub struct RecordJson {
    pub index: usize,
    #[serde(skip_serializing_if = "Option::is_none")] pub len: Option<usize>,
    pub crc_ok: bool,                              // false when error is Some
    #[serde(skip_serializing_if = "Option::is_none")] pub hex_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub utf8_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub error: Option<String>,
}
#[derive(Serialize)]
pub struct RecordsResponse {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub header: Option<HeaderJson>,
    pub records: Vec<RecordJson>,
    pub terminated_cleanly: Option<bool>,
}

fn hex_preview(bytes: &[u8]) -> String {
    bytes.iter().take(PREVIEW_BYTES).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ")
}

pub fn records(wab_dir: &Path, rel: &str, offset: usize, limit: usize) -> Result<RecordsResponse, WabError> {
    let path = safe_join(wab_dir, rel)?;
    let limit = limit.clamp(1, 1000);
    let mut reader = SegmentReader::open(&path)?;
    let header = Some(HeaderJson::from(reader.header()));
    let mut records = Vec::new();
    let mut index = 0usize;
    while let Some(item) = reader.next() {
        if index < offset { index += 1; continue; }
        if records.len() >= limit { break; }
        match item {
            Ok(payload) => {
                let bytes: &[u8] = &payload;
                records.push(RecordJson {
                    index, len: Some(bytes.len()), crc_ok: true,
                    hex_preview: Some(hex_preview(bytes)),
                    utf8_preview: Some(String::from_utf8_lossy(&bytes[..bytes.len().min(PREVIEW_BYTES)]).into_owned()),
                    error: None,
                });
            }
            Err(e) => {
                records.push(RecordJson { index, len: None, crc_ok: false, hex_preview: None, utf8_preview: None, error: Some(e.to_string()) });
                index += 1;
                break; // reader is fused after an Err; stop
            }
        }
        index += 1;
    }
    Ok(RecordsResponse { file: rel.to_string(), header, records, terminated_cleanly: reader.terminated_cleanly() })
}

#[derive(Serialize)]
pub struct DeadLetterSegmentJson { pub file: String, pub records: Vec<RecordJson>, pub terminated_cleanly: Option<bool> }
#[derive(Serialize)]
pub struct DeadLetterResponse { pub dead_letter_dir: String, pub segments: Vec<DeadLetterSegmentJson> }

pub fn dead_letter(wab_dir: &Path) -> Result<DeadLetterResponse, WabError> {
    let dl_dir = wab_dir.join("dead_letter");
    let mut segments = Vec::new();
    if dl_dir.is_dir() {
        for (path, _state) in list_segment_files(&dl_dir)? {
            let rel = format!("dead_letter/{}", path.file_name().unwrap().to_string_lossy());
            let r = records(wab_dir, &rel, 0, 1000)?;
            segments.push(DeadLetterSegmentJson { file: rel, records: r.records, terminated_cleanly: r.terminated_cleanly });
        }
    }
    Ok(DeadLetterResponse { dead_letter_dir: dl_dir.to_string_lossy().into_owned(), segments })
}

pub fn verify(wab_dir: &Path, rel: &str) -> Result<IntegrityJson, WabError> {
    let path = safe_join(wab_dir, rel)?;
    Ok(match verify_sealed_segment(&path) { Ok(_) => IntegrityJson::ok(), Err(e) => IntegrityJson::from_err(&e) })
}
