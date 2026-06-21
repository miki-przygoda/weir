# weir-console — WAB Explorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the read-only WAB Explorer view of a new local tool `weir-console` — a web UI that inspects a weir wab directory (segments, records, integrity, dead-letter) by driving the 1.2.0 `weir-wab` public read API.

**Architecture:** A new crate `tools/weir-console/` **excluded from the root workspace**. An `axum` backend exposes read-only JSON over a small `wab` core module (pure functions over a wab dir, unit-testable without HTTP); a static, vanilla, hr2-styled frontend renders a shard→segment tree + a record viewer. Spec: `docs/superpowers/specs/2026-06-21-weir-console-wab-explorer-design.md`.

**Tech Stack:** Rust, `axum` + `tower-http` (HTTP/static), `tokio`, `serde`/`serde_json`, `clap` (args), `weir-wab` + `weir-core` (path deps); `tempfile` (dev, fixtures). Frontend: vanilla HTML/CSS/JS reusing `demo/weir.css`.

---

## Verified `weir-wab` API (use these exact signatures)

```rust
// crate root (weir_wab::)
pub use weir_core::Payload;                              // derefs to [u8]
pub struct SegmentReader { /* … */ }
impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self>;
    pub fn header(&self) -> &weir_wab::format::SegmentHeaderMeta;
    pub fn terminated_cleanly(&self) -> Option<bool>;    // Some(true)=sentinel, Some(false)=torn, None=mid-iter
}
impl Iterator for SegmentReader { type Item = std::io::Result<Payload>; }
pub enum SegmentState { Active, Sealed, Confirmed }
pub fn list_segment_files(dir: impl AsRef<Path>) -> std::io::Result<Vec<(PathBuf, SegmentState)>>;
pub struct SegmentVerification { pub header: format::SegmentHeaderMeta, pub footer: format::SegmentFooterMeta }
#[non_exhaustive] pub enum SegmentVerifyError {
    Io(std::io::Error), Header(format::SegmentHeaderParseError), TooShort,
    BadRecord(std::io::Error), NoSentinel, MissingFooter,
    CrcMismatch { expected: u32, computed: u32 }, TrailingBytes,
}
pub fn verify_sealed_segment(path: impl AsRef<Path>) -> Result<SegmentVerification, SegmentVerifyError>;

// weir_wab::format::
pub const SEGMENT_HEADER_LEN: usize = 24;
pub const SEGMENT_FOOTER_LEN: usize = 32;
pub const EXT_ACTIVE: &str = ".wab"; EXT_SEALED: ".wab.sealed"; EXT_CONFIRMED: ".wab.confirmed";
pub struct SegmentHeaderMeta { pub format_version: u8, pub shard_id: u16, pub created_at: i64 }
pub struct SegmentFooterMeta { pub record_count: u64, pub data_bytes: u64, pub file_crc32: u32, pub sealed_at: i64 }
pub struct ConfirmedMeta { pub sealed_at: i64, pub record_count: u64, pub drained_at: i64 }
pub fn parse_confirmed(buf: &[u8]) -> Result<ConfirmedMeta, ConfirmedParseError>;
pub fn crc32(bytes: &[u8]) -> u32;
// write-side builders (for test fixtures only):
pub fn build_segment_header(shard_id: u16) -> [u8; SEGMENT_HEADER_LEN];
pub fn build_sentinel() -> [u8; 4];
pub fn build_segment_footer(record_count: u64, data_bytes: u64, file_crc32: u32, sealed_at: i64) -> [u8; SEGMENT_FOOTER_LEN];
pub fn build_confirmed(sealed_at: i64, record_count: u64, drained_at: i64) -> [u8; CONFIRMED_LEN];
```
On-disk record framing (per record): `payload_len: u32 LE` · `crc32: u32 LE (over the payload bytes)` · `payload`. End sentinel = `payload_len == 0` (`build_sentinel()`). Confirmed sidecar = the `.wab.confirmed` file's whole bytes → `parse_confirmed`.

## File Structure

```
tools/weir-console/
  Cargo.toml                 # publish=false; deps above; [[bin]] name = "weir-console"
  README.md                  # what it is, how to run, read-only note
  src/
    main.rs                  # clap args, build router, bind+serve
    server.rs                # axum Router: /api/wab/* + ServeDir static + index fallback; AppState{wab_dir}
    wab.rs                   # PURE core: inventory/records/dead_letter/verify over a wab dir + safe_join + JSON types + WabError
  static/
    index.html               # Explorer page (nav: Explorer active, Ops/Live disabled)
    explorer.js              # fetch + render tree/detail/records/toggle/errors
    weir.css                 # committed copy of demo/weir.css (provenance noted in README)
  tests/
    fixtures.rs              # shared: build a fixtures wab dir (sealed/active/confirmed/corrupt/truncated/dead-letter)
    wab_api.rs               # integration tests over the wab core + a couple axum oneshot tests
Cargo.toml (root)            # add exclude = ["tools/weir-console"]
```

Root workspace, published crates, and main CI are **unaffected** (the tool is excluded).

---

### Task 1: Scaffold the crate, excluded from the root workspace

**Files:**
- Create: `tools/weir-console/Cargo.toml`, `tools/weir-console/src/main.rs`
- Modify: `Cargo.toml` (root, `[workspace]` block — add `exclude`)

- [ ] **Step 1: Add the exclude to the root workspace**

In root `Cargo.toml`, inside the existing `[workspace]` table (which has `members = [...]` and `resolver = "2"`), add:

```toml
exclude = ["tools/weir-console"]
```

- [ ] **Step 2: Create the tool crate manifest**

`tools/weir-console/Cargo.toml`:

```toml
[package]
name = "weir-console"
version = "0.1.0"
edition = "2024"
rust-version = "1.88"
publish = false
description = "Local tool: inspect a live/at-rest weir (WAB Explorer view)."

[[bin]]
name = "weir-console"
path = "src/main.rs"

[dependencies]
axum = "0.7"
tower-http = { version = "0.6", features = ["fs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
clap = { version = "4", features = ["derive"] }
weir-core = { path = "../../crates/weir-core" }
weir-wab = { path = "../../crates/weir-wab" }

[dev-dependencies]
tempfile = "3"
tower = { version = "0.5", features = ["util"] }   # ServiceExt::oneshot for axum tests
```

- [ ] **Step 3: Minimal `main.rs` that builds**

`tools/weir-console/src/main.rs`:

```rust
fn main() {
    println!("weir-console (scaffold)");
}
```

- [ ] **Step 4: Verify the tool builds and the root workspace is unaffected**

Run (from repo root): `cargo build --manifest-path tools/weir-console/Cargo.toml`
Expected: `Finished` (downloads axum/etc. into the tool's own lockfile).
Run: `git diff --exit-code Cargo.lock`
Expected: no change (the root lockfile is untouched — the tool has its own `tools/weir-console/Cargo.lock`).
Run: `cargo build --workspace`
Expected: `Finished`, weir-console NOT among the compiled crates.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml tools/weir-console/Cargo.toml tools/weir-console/src/main.rs tools/weir-console/Cargo.lock
git commit -m "feat(weir-console): scaffold the tool crate, excluded from the root workspace"
```

---

### Task 2: `wab` core module — JSON types, `safe_join`, and the fixtures helper

**Files:**
- Create: `tools/weir-console/src/wab.rs`
- Create: `tools/weir-console/tests/fixtures.rs`

- [ ] **Step 1: Write the JSON types + error + `safe_join` in `wab.rs`**

`tools/weir-console/src/wab.rs`:

```rust
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
```

Add `mod wab;` to `main.rs` (replace the scaffold body for now with `mod wab; fn main() {}` — the real `main` lands in Task 8).

- [ ] **Step 2: Write the fixtures helper**

`tools/weir-console/tests/fixtures.rs`:

```rust
//! Builds a deterministic fixtures wab dir using weir-wab's public byte builders,
//! so the wab-core integration tests have known good/corrupt/edge inputs.
use std::fs;
use std::path::{Path, PathBuf};
use weir_wab::format::{build_confirmed, build_segment_footer, build_segment_header, build_sentinel, crc32};

/// One on-disk record: payload_len(LE u32) + crc32(payload)(LE u32) + payload.
fn record_bytes(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + payload.len());
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    v.extend_from_slice(&crc32(payload).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

/// Header + records + (if `sealed`) sentinel + footer. Returns the raw bytes.
pub fn segment_bytes(shard_id: u16, records: &[&[u8]], sealed: bool) -> Vec<u8> {
    let mut buf = build_segment_header(shard_id).to_vec();
    let mut data_bytes: u64 = 0;
    for r in records { buf.extend_from_slice(&record_bytes(r)); data_bytes += r.len() as u64; }
    if sealed {
        let file_crc = crc32(&buf); // CRC over all bytes before the sentinel
        buf.extend_from_slice(&build_sentinel());
        buf.extend_from_slice(&build_segment_footer(records.len() as u64, data_bytes, file_crc, 1_700_000_000_000_000_000));
    }
    buf
}

/// Writes a complete fixtures wab dir under `root` and returns `root`.
/// Layout: shard_00/{sealed, active, confirmed sidecar, crc-corrupt sealed, truncated sealed}
/// and dead_letter/ with one sealed dead-letter segment.
pub fn build_fixtures(root: &Path) -> PathBuf {
    let shard = root.join("shard_00");
    fs::create_dir_all(&shard).unwrap();

    // A clean sealed segment: seg_00000001.wab.sealed (2 records)
    fs::write(shard.join("seg_00000001.wab.sealed"),
        segment_bytes(0, &[b"alpha", b"beta"], true)).unwrap();

    // An active (unsealed) segment: seg_00000002.wab (1 record, no sentinel/footer)
    fs::write(shard.join("seg_00000002.wab"),
        segment_bytes(0, &[b"gamma"], false)).unwrap();

    // A confirmed sidecar for segment 1: seg_00000001.wab.confirmed
    fs::write(shard.join("seg_00000001.wab.confirmed"),
        build_confirmed(1_700_000_000_000_000_000, 2, 1_700_000_001_000_000_000)).unwrap();

    // A CRC-corrupt sealed segment: flip a payload byte AFTER building (footer CRC now mismatches)
    let mut corrupt = segment_bytes(0, &[b"corruptme"], true);
    corrupt[weir_wab::format::SEGMENT_HEADER_LEN + 8] ^= 0xff; // first payload byte
    fs::write(shard.join("seg_00000003.wab.sealed"), corrupt).unwrap();

    // A truncated sealed segment: drop the trailing footer bytes -> MissingFooter/NoSentinel
    let mut truncated = segment_bytes(0, &[b"shortlived"], true);
    truncated.truncate(truncated.len() - 10);
    fs::write(shard.join("seg_00000004.wab.sealed"), truncated).unwrap();

    // Dead-letter store: dead_letter/dl_00000001.wab.sealed (2 rejected payloads)
    let dl = root.join("dead_letter");
    fs::create_dir_all(&dl).unwrap();
    fs::write(dl.join("dl_00000001.wab.sealed"),
        segment_bytes(0, &[b"rejected-1", b"rejected-2"], true)).unwrap();

    root.to_path_buf()
}
```

- [ ] **Step 3: Verify it builds**

Run: `cargo build --manifest-path tools/weir-console/Cargo.toml`
Expected: `Finished` (the fixtures file isn't compiled into the bin; it compiles when tests run in later tasks — verify with `cargo test --manifest-path tools/weir-console/Cargo.toml --no-run`).

- [ ] **Step 4: Commit**

```bash
git add tools/weir-console/src/wab.rs tools/weir-console/src/main.rs tools/weir-console/tests/fixtures.rs tools/weir-console/Cargo.lock
git commit -m "feat(weir-console): wab core types, safe_join, and the fixtures helper"
```

---

### Task 3: `inventory` — the `/api/wab/segments` data

**Files:**
- Modify: `tools/weir-console/src/wab.rs`
- Test: `tools/weir-console/tests/wab_api.rs`

- [ ] **Step 1: Write the failing test**

`tools/weir-console/tests/wab_api.rs`:

```rust
mod fixtures;
use weir_console::wab; // see Step 3 note on exposing the lib
use tempfile::tempdir;

#[test]
fn inventory_reports_state_meta_and_integrity() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let inv = wab::inventory(&root).unwrap();

    assert_eq!(inv.totals.segments, 5);          // 2 sealed + 1 active + 1 corrupt-sealed + 1 truncated-sealed (confirmed sidecars are not counted as segments)
    assert_eq!(inv.totals.active, 1);
    assert_eq!(inv.totals.dead_letter, 1);

    let sealed_ok = inv.segments.iter().find(|s| s.file.ends_with("seg_00000001.wab.sealed")).unwrap();
    assert_eq!(sealed_ok.state, "sealed");
    assert_eq!(sealed_ok.footer.as_ref().unwrap().record_count, 2);
    assert!(sealed_ok.integrity.as_ref().unwrap().ok);
    assert!(sealed_ok.confirmed.is_some()); // sidecar present -> confirmed meta attached

    let corrupt = inv.segments.iter().find(|s| s.file.ends_with("seg_00000003.wab.sealed")).unwrap();
    let integ = corrupt.integrity.as_ref().unwrap();
    assert!(!integ.ok);
    assert_eq!(integ.kind.as_deref(), Some("CrcMismatch"));

    let active = inv.segments.iter().find(|s| s.file.ends_with("seg_00000002.wab")).unwrap();
    assert_eq!(active.state, "active");
    assert!(active.footer.is_none()); // unsealed: no footer/integrity
    assert!(active.header.is_some());
}
```

> **Step 3 note:** to call `wab::` from the integration test, the crate needs a library target. Add `src/lib.rs` with `pub mod wab;` and set `Cargo.toml` `[lib] name = "weir_console"` + keep the `[[bin]]`. The bin's `main.rs` then does `use weir_console::wab;`. Do this in this task (it's the first cross-module test).

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api inventory_reports_state_meta_and_integrity`
Expected: FAIL to compile — `wab::inventory` / `SegmentsResponse` not found, and `weir_console` lib missing.

- [ ] **Step 3: Add the lib target + implement `inventory`**

Create `tools/weir-console/src/lib.rs`:
```rust
pub mod wab;
```
In `Cargo.toml` add:
```toml
[lib]
name = "weir_console"
path = "src/lib.rs"
```
Change `main.rs` to `use weir_console::wab;` instead of `mod wab;`.

Add to `wab.rs`:

```rust
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

pub fn inventory(wab_dir: &Path) -> Result<SegmentsResponse, WabError> {
    let mut segments = Vec::new();
    let mut totals = Totals::default();
    // dead-letter count (segments under dead_letter/, listed via its own endpoint)
    let dl = wab_dir.join("dead_letter");
    if dl.is_dir() {
        totals.dead_letter = list_segment_files(&dl).unwrap_or_default()
            .into_iter().filter(|(_, st)| matches!(st, SegmentState::Sealed | SegmentState::Active)).count();
    }
    for (path, state) in list_segment_files(wab_dir)? {
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api inventory_reports_state_meta_and_integrity`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/src/lib.rs tools/weir-console/src/main.rs tools/weir-console/src/wab.rs tools/weir-console/Cargo.toml tools/weir-console/tests/wab_api.rs tools/weir-console/Cargo.lock
git commit -m "feat(weir-console): wab inventory (segments + state + meta + integrity)"
```

---

### Task 4: `records` — the `/api/wab/segment` data

**Files:**
- Modify: `tools/weir-console/src/wab.rs`
- Test: `tools/weir-console/tests/wab_api.rs`

- [ ] **Step 1: Write the failing test**

Append to `tools/weir-console/tests/wab_api.rs`:

```rust
#[test]
fn records_reads_payloads_with_crc_and_termination() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());

    let ok = wab::records(&root, "shard_00/seg_00000001.wab.sealed", 0, 100).unwrap();
    assert_eq!(ok.records.len(), 2);
    assert!(ok.records[0].crc_ok && ok.records[1].crc_ok);
    assert_eq!(ok.records[0].utf8_preview.as_deref(), Some("alpha"));
    assert_eq!(ok.terminated_cleanly, Some(true)); // sentinel present

    // The CRC-corrupt segment yields an error row for the bad record.
    let bad = wab::records(&root, "shard_00/seg_00000003.wab.sealed", 0, 100).unwrap();
    assert!(bad.records.iter().any(|r| r.error.is_some()));

    // Path escape is rejected.
    assert!(matches!(wab::records(&root, "../etc/passwd", 0, 10), Err(wab::WabError::BadPath(_))));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api records_reads_payloads_with_crc_and_termination`
Expected: FAIL — `wab::records` not found.

- [ ] **Step 3: Implement `records`**

Add to `wab.rs`:

```rust
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api records_reads_payloads_with_crc_and_termination`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/src/wab.rs tools/weir-console/tests/wab_api.rs
git commit -m "feat(weir-console): wab record reader (previews, crc flag, termination, path-safety)"
```

---

### Task 5: `dead_letter` and `verify`

**Files:**
- Modify: `tools/weir-console/src/wab.rs`
- Test: `tools/weir-console/tests/wab_api.rs`

- [ ] **Step 1: Write the failing tests**

Append to `tools/weir-console/tests/wab_api.rs`:

```rust
#[test]
fn dead_letter_lists_payload_records() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let dl = wab::dead_letter(&root).unwrap();
    assert_eq!(dl.segments.len(), 1);
    assert_eq!(dl.segments[0].records.len(), 2);
    assert_eq!(dl.segments[0].records[0].utf8_preview.as_deref(), Some("rejected-1"));
}

#[test]
fn verify_returns_ok_and_structured_error() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    assert!(wab::verify(&root, "shard_00/seg_00000001.wab.sealed").unwrap().ok);
    let bad = wab::verify(&root, "shard_00/seg_00000003.wab.sealed").unwrap();
    assert!(!bad.ok && bad.kind.as_deref() == Some("CrcMismatch"));
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api dead_letter`
Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api verify_returns`
Expected: FAIL — `wab::dead_letter` / `wab::verify` not found.

- [ ] **Step 3: Implement both**

Add to `wab.rs`:

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api`
Expected: PASS (all wab_api tests).

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/src/wab.rs tools/weir-console/tests/wab_api.rs
git commit -m "feat(weir-console): wab dead-letter listing + per-segment verify"
```

---

### Task 6: Console shell — axum server, args, static serving

**Files:**
- Create: `tools/weir-console/src/server.rs`
- Modify: `tools/weir-console/src/lib.rs`, `tools/weir-console/src/main.rs`
- Test: `tools/weir-console/tests/wab_api.rs`

- [ ] **Step 1: Write the failing axum oneshot tests**

Append to `tools/weir-console/tests/wab_api.rs`:

```rust
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn http_segments_ok_and_bad_path_400() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let app = weir_console::server::router(root.clone());

    let resp = app.clone().oneshot(Request::get("/api/wab/segments").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.oneshot(Request::get("/api/wab/segment?path=../x&offset=0&limit=10").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api http_segments`
Expected: FAIL — `weir_console::server::router` not found.

- [ ] **Step 3: Implement `server.rs`**

`tools/weir-console/src/server.rs`:

```rust
use std::path::PathBuf;
use std::sync::Arc;
use axum::{extract::{Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router};
use serde::Deserialize;
use tower_http::services::ServeDir;
use crate::wab::{self, WabError};

#[derive(Clone)]
pub struct AppState { pub wab_dir: Arc<PathBuf>, pub static_dir: Arc<PathBuf> }

fn err_response(e: WabError) -> Response {
    let code = match e { WabError::BadPath(_) => StatusCode::BAD_REQUEST, WabError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR };
    (code, Json(serde_json::json!({ "error": e.to_string() }))).into_response()
}

#[derive(Deserialize)] struct SegQuery { path: String, #[serde(default)] offset: usize, #[serde(default = "def_limit")] limit: usize }
fn def_limit() -> usize { 100 }
#[derive(Deserialize)] struct PathQuery { path: String }

pub fn router(wab_dir: PathBuf) -> Router { router_with_static(wab_dir, default_static_dir()) }

pub fn router_with_static(wab_dir: PathBuf, static_dir: PathBuf) -> Router {
    let state = AppState { wab_dir: Arc::new(wab_dir), static_dir: Arc::new(static_dir.clone()) };
    Router::new()
        .route("/api/wab/segments", get(|State(s): State<AppState>| async move {
            match wab::inventory(&s.wab_dir) { Ok(r) => Json(r).into_response(), Err(e) => err_response(e) }
        }))
        .route("/api/wab/segment", get(|State(s): State<AppState>, Query(q): Query<SegQuery>| async move {
            match wab::records(&s.wab_dir, &q.path, q.offset, q.limit) { Ok(r) => Json(r).into_response(), Err(e) => err_response(e) }
        }))
        .route("/api/wab/dead-letter", get(|State(s): State<AppState>| async move {
            match wab::dead_letter(&s.wab_dir) { Ok(r) => Json(r).into_response(), Err(e) => err_response(e) }
        }))
        .route("/api/wab/verify", get(|State(s): State<AppState>, Query(q): Query<PathQuery>| async move {
            match wab::verify(&s.wab_dir, &q.path) { Ok(r) => Json(r).into_response(), Err(e) => err_response(e) }
        }))
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .with_state(state)
}

fn default_static_dir() -> PathBuf {
    // ../static relative to the crate (works from `cargo run` in the workspace).
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static")
}
```

Add `pub mod server;` to `lib.rs`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api http_segments`
Expected: PASS.

- [ ] **Step 5: Implement `main.rs` (args + serve)**

`tools/weir-console/src/main.rs`:

```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use clap::Parser;

#[derive(Parser)]
#[command(name = "weir-console", about = "Inspect a weir wab directory (WAB Explorer).")]
struct Args {
    /// The weir wab directory to inspect (read-only).
    #[arg(long)]
    wab_dir: PathBuf,
    /// Address to bind.
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.wab_dir.is_dir() {
        eprintln!("weir-console: --wab-dir {:?} is not a directory", args.wab_dir);
        std::process::exit(2);
    }
    let app = weir_console::server::router(args.wab_dir.clone());
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("weir-console: WAB Explorer for {:?} at http://{}", args.wab_dir, args.bind);
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 6: Verify it builds + commit**

Run: `cargo build --manifest-path tools/weir-console/Cargo.toml`
Expected: `Finished`.
```bash
git add tools/weir-console/src/server.rs tools/weir-console/src/lib.rs tools/weir-console/src/main.rs tools/weir-console/tests/wab_api.rs tools/weir-console/Cargo.lock
git commit -m "feat(weir-console): axum shell (routes, static serving, args, serve)"
```

---

### Task 7: Frontend — Explorer page + weir.css copy + explorer.js

**Files:**
- Create: `tools/weir-console/static/weir.css` (copy), `tools/weir-console/static/index.html`, `tools/weir-console/static/explorer.js`

- [ ] **Step 1: Copy the hr2 theme**

Run: `cp demo/weir.css tools/weir-console/static/weir.css`
(The README, Task 9, records that this is a copy of `demo/weir.css`.)

- [ ] **Step 2: Write `index.html`**

`tools/weir-console/static/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>weir-console · WAB Explorer</title>
<link rel="stylesheet" href="weir.css" />
<style>
  .wc-grid { display: grid; grid-template-columns: 320px 1fr; gap: 16px; }
  .wc-tree { border: 1px solid var(--n-border); padding: 8px; overflow:auto; max-height: 75vh; }
  .wc-seg { padding: 4px 6px; cursor: pointer; font-size: 12px; }
  .wc-seg:hover { background: var(--n-bg-row); }
  .wc-chip { font-size: 10px; padding: 1px 6px; border: 1px solid var(--n-border); margin-left: 6px; }
  .wc-ok { color: var(--n-green); } .wc-bad { color: var(--n-rose); }
  .wc-rec { font-family: 'JetBrains Mono', monospace; font-size: 11px; border-bottom: 1px solid var(--n-border-lt); padding: 4px 0; white-space: pre-wrap; word-break: break-all; }
</style>
</head>
<body>
<div class="statusbar top">
  <span>weir-console · WAB Explorer · <span data-weir-version></span></span>
  <span class="sb-spacer"></span>
  <span class="sb-dim">read-only</span>
</div>
<nav class="nav">
  <a href="index.html" class="active">Explorer</a>
  <a href="#" class="wc-disabled" title="coming soon">Ops</a>
  <a href="#" class="wc-disabled" title="coming soon">Live</a>
</nav>
<div class="wrap">
  <div id="topbar"></div>
  <div class="wc-grid">
    <div class="wc-tree" id="tree">loading…</div>
    <div id="detail" class="panel"><div class="panel-body">select a segment</div></div>
  </div>
</div>
<script src="explorer.js"></script>
</body>
</html>
```

(There is no `version.js` here — the `<span data-weir-version>` is filled by `explorer.js` from a constant, since this tool isn't part of the demo bundle's version pipeline.)

- [ ] **Step 3: Write `explorer.js`**

`tools/weir-console/static/explorer.js`:

```js
const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach(e => e.textContent = "1.2.0");

async function getJSON(url) {
  const r = await fetch(url);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

function chip(text, cls) { return `<span class="wc-chip ${cls || ""}">${text}</span>`; }

function renderTopbar(inv) {
  const t = inv.totals;
  $("#topbar").innerHTML = `
    <div class="sec-head"><span class="label">WAB</span><div class="rule"></div>
      <span class="sb-dim">${inv.wab_dir}</span></div>
    <p class="sb-dim">${t.segments} segments · ${t.sealed} sealed · ${t.active} active ·
      ${t.confirmed} confirmed · ${t.dead_letter} dead-letter · ${t.total_bytes} bytes</p>`;
}

function integrityChip(seg) {
  if (!seg.integrity) return "";
  return seg.integrity.ok ? chip("✓", "wc-ok") : chip("✗ " + seg.integrity.kind, "wc-bad");
}

function renderTree(inv) {
  const byShard = {};
  for (const s of inv.segments) (byShard[s.shard ?? "?"] ??= []).push(s);
  let html = "";
  for (const shard of Object.keys(byShard).sort()) {
    html += `<div class="label">shard ${shard}</div>`;
    for (const s of byShard[shard]) {
      const name = s.file.split("/").pop();
      html += `<div class="wc-seg" data-path="${s.file}">${name}${chip(s.state)}${integrityChip(s)}</div>`;
    }
  }
  html += `<div class="label">dead-letter</div><div class="wc-seg" data-deadletter="1">dead_letter/</div>`;
  $("#tree").innerHTML = html;
  $("#tree").querySelectorAll(".wc-seg[data-path]").forEach(el =>
    el.onclick = () => showSegment(el.dataset.path, inv));
  const dl = $("#tree").querySelector(".wc-seg[data-deadletter]");
  if (dl) dl.onclick = () => showDeadLetter();
}

let hexMode = false;
function recordRow(r) {
  if (r.error) return `<div class="wc-rec wc-bad">#${r.index} ERROR: ${r.error}</div>`;
  const preview = hexMode ? r.hex_preview : r.utf8_preview;
  return `<div class="wc-rec">#${r.index} · ${r.len}B · ${r.crc_ok ? '<span class="wc-ok">crc✓</span>' : '<span class="wc-bad">crc✗</span>'} · ${preview}</div>`;
}

function metaBlock(seg) {
  let h = "";
  if (seg.header) h += `<p class="sb-dim">header: shard ${seg.header.shard_id} · created_at ${seg.header.created_at} · v${seg.header.format_version}</p>`;
  if (seg.footer) h += `<p class="sb-dim">footer: ${seg.footer.record_count} records · ${seg.footer.data_bytes}B · crc ${seg.footer.file_crc32}</p>`;
  if (seg.confirmed) h += `<p class="sb-dim">confirmed: drained ${seg.confirmed.record_count} @ ${seg.confirmed.drained_at}</p>`;
  if (seg.integrity && !seg.integrity.ok) {
    const i = seg.integrity;
    h += `<p class="wc-bad">integrity: ${i.kind}${i.expected ? ` (expected ${i.expected}, computed ${i.computed})` : ""}${i.detail ? " — " + i.detail : ""}</p>`;
  }
  return h;
}

async function showSegment(path, inv) {
  const seg = inv.segments.find(s => s.file === path) || {};
  try {
    const data = await getJSON(`/api/wab/segment?path=${encodeURIComponent(path)}&offset=0&limit=200`);
    const term = data.terminated_cleanly === true ? "clean end (sentinel)" :
                 data.terminated_cleanly === false ? "torn tail (no sentinel)" : "—";
    $("#detail").innerHTML = `<div class="panel-title">${path.split("/").pop()}
        <button id="toggle" class="pt-right">${hexMode ? "utf8" : "hex"}</button></div>
      <div class="panel-body">${metaBlock(seg)}
        ${data.records.map(recordRow).join("")}
        <p class="sb-dim">— ${term}</p></div>`;
    $("#toggle").onclick = () => { hexMode = !hexMode; showSegment(path, inv); };
  } catch (e) {
    $("#detail").innerHTML = `<div class="panel-body wc-bad">error: ${e.message}</div>`;
  }
}

async function showDeadLetter() {
  try {
    const data = await getJSON("/api/wab/dead-letter");
    const segs = data.segments.map(s =>
      `<p class="label">${s.file}</p>${s.records.map(recordRow).join("")}`).join("");
    $("#detail").innerHTML = `<div class="panel-title">dead-letter</div>
      <div class="panel-body">${segs || "<p class='sb-dim'>empty</p>"}</div>`;
  } catch (e) { $("#detail").innerHTML = `<div class="panel-body wc-bad">error: ${e.message}</div>`; }
}

async function main() {
  try {
    const inv = await getJSON("/api/wab/segments");
    renderTopbar(inv); renderTree(inv);
  } catch (e) {
    $("#tree").innerHTML = `<span class="wc-bad">error: ${e.message}</span>`;
  }
}
main();
```

- [ ] **Step 4: Manual smoke (documented; real run)**

Run: `cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <a real or fixtures wab dir>`
Then open `http://127.0.0.1:8787` — confirm the tree lists segments with state + integrity chips and clicking one shows records. (For a quick fixtures dir, point `--wab-dir` at a temp dir populated by a short throwaway program or a real daemon's wab dir.)

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/static/
git commit -m "feat(weir-console): Explorer frontend (tree, record viewer, hex/utf8 toggle, corruption display)"
```

---

### Task 8: Frontend smoke test (node, no backend)

**Files:**
- Create: `tools/weir-console/static/explorer.test.mjs`

- [ ] **Step 1: Write a DOM-stubbed render test**

`tools/weir-console/static/explorer.test.mjs` — a minimal node test that stubs `document`/`fetch`, loads the render helpers, and asserts a tree row + a record row render from mock JSON, and the hex/utf8 toggle flips. (Mirror the existing demo bundle's headless harness pattern: stub `document.querySelector`/`querySelectorAll`, inject mock `/api/wab/segments` + `/api/wab/segment` responses, call `main()`, assert the produced HTML strings contain the segment file name, a `crc✓`, and that toggling re-renders with hex.) Keep it dependency-free (`node --test`).

- [ ] **Step 2: Run it**

Run: `node --test tools/weir-console/static/explorer.test.mjs`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tools/weir-console/static/explorer.test.mjs
git commit -m "test(weir-console): frontend render smoke test (mock JSON, no backend)"
```

---

### Task 9: README + final polish

**Files:**
- Create: `tools/weir-console/README.md`

- [ ] **Step 1: Write the README**

`tools/weir-console/README.md`: what it is (the WAB Explorer view of weir-console; Ops/Live to come), how to run (`cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <dir>`, open the printed URL), the **read-only** guarantee, that it works against a stopped daemon's dir (post-mortem forensics), the four endpoints, and the note that `static/weir.css` is a **copy of `demo/weir.css`** (hr2 theme).

- [ ] **Step 2: fmt + clippy the tool**

Run: `cargo fmt --manifest-path tools/weir-console/Cargo.toml`
Run: `cargo clippy --manifest-path tools/weir-console/Cargo.toml --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Full tool test + workspace-untouched check**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml`
Expected: all pass.
Run: `cargo build --workspace && git status --short Cargo.lock`
Expected: workspace builds; root `Cargo.lock` unchanged.

- [ ] **Step 4: Commit**

```bash
git add tools/weir-console/README.md tools/weir-console/src tools/weir-console/Cargo.lock
git commit -m "docs(weir-console): README + fmt/clippy polish for the WAB Explorer"
```

---

## Self-review

**Spec coverage:** `wab` endpoints — Task 3 (segments), 4 (records), 5 (dead-letter + verify); console shell — Task 6; frontend Explorer — Task 7; testing — Tasks 3–6 (backend integration over fixtures) + Task 8 (frontend smoke); placement/exclude/deps — Task 1; README + read-only + provenance — Task 9; corruption-as-first-class — IntegrityJson (Task 2/3) + record error rows (Task 4) + metaBlock (Task 7); path-safety — `safe_join` (Task 2), tested Task 4 + Task 6. Out-of-scope (Ops/Live/mutations/deployed) — not implemented (nav placeholders only). ✓ all covered.

**Placeholder scan:** no TBD/TODO; the one prose-only step is Task 8's frontend test (acceptable — it points at the existing headless-harness pattern with explicit assertions to make; everything correctness-critical has full code).

**Type consistency:** `inventory`/`records`/`dead_letter`/`verify` signatures + `SegmentsResponse`/`RecordsResponse`/`DeadLetterResponse`/`IntegrityJson`/`SegmentJson`/`RecordJson` names used identically across tasks and the frontend's field reads (`s.file`, `s.state`, `s.integrity.ok/kind/expected/computed`, `r.crc_ok/error/hex_preview/utf8_preview`, `data.terminated_cleanly`); `weir-wab` signatures match the verified-API block.
</content>
