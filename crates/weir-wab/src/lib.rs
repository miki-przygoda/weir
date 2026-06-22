//! On-disk WAB segment format and reader for weir.
//!
//! This crate is the single source of truth for the byte-level layout of a
//! weir write-ahead-buffer (WAB) segment file and the reader that streams
//! records back out of one. It is shared by two consumers:
//!
//! - **`weir-server`** (the daemon) — writes segments, drains them, and replays
//!   unconfirmed segments on crash recovery.
//! - **`weir-ctl`** (the operator CLI) — reads dead-letter segments to requeue
//!   them through the daemon's socket (`weir-ctl dl requeue`).
//!
//! Keeping the format + reader here means there is exactly one parser for the
//! on-disk format; the daemon and the CLI can never drift out of sync.
//!
//! The crate is deliberately tiny: it depends only on [`weir_core`] (for
//! [`Payload`] and the payload size cap) and `crc32fast`. It does no async I/O,
//! pulls in no runtime, and is safe for a slim CLI to depend on.
//!
//! # Trust model
//!
//! [`SegmentReader`] validates structural integrity only — magic, format version,
//! and a CRC32 per record. CRC32 detects *accidental* corruption, **not** a
//! deliberately forged segment with a valid CRC; see the Security model in
//! [`mod@format`] for the full statement. The reader does **not** check file
//! ownership or permissions: the access boundary is the filesystem (`0700` WAB
//! directory, `0600` segment files), and both consumers are assumed to run as the
//! daemon's UID. This is already the case for `weir-ctl dl requeue` — it must run
//! as that UID both to read the `0700` dead-letter directory *and* to pass the
//! daemon's default-on socket `peer_uid_check`. A different-UID caller cannot
//! reach the segments at all, so `dl requeue` opens no new forged-segment vector
//! beyond the "compromised process running as the daemon UID" case, which the
//! threat model places out of scope. If the WAB lives on a shared/network
//! filesystem these guarantees do not hold (also an explicit `format` assumption).
//!
//! See [`mod@format`] for the on-disk byte layout.

#![deny(missing_docs)]

pub mod format;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use weir_core::MAX_PAYLOAD_HARD_CAP;

use format::{
    EXT_ACTIVE, EXT_CONFIRMED, EXT_SEALED, FORMAT_VERSION, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN,
    SEGMENT_MAGIC, SegmentFooterMeta, SegmentHeaderMeta, SegmentHeaderParseError,
    parse_segment_footer, parse_segment_header,
};

/// Re-export of [`weir_core::Payload`] — the item type of the [`SegmentReader`]
/// iterator ([`io::Result<Payload>`]). Re-exported so a consumer depending on
/// only `weir-wab` can name the iterator item type (`weir_wab::Payload`)
/// without also taking a direct dependency on `weir-core`.
pub use weir_core::Payload;

/// An iterator over records in a WAB segment file.
///
/// Streams records without materialising the whole segment. Applies
/// [`MAX_PAYLOAD_HARD_CAP`] before every heap allocation to bound memory usage
/// while reading.
///
/// # Iteration contract
///
/// The iterator yields every good record up to the first problem, then stops.
/// Exactly how it stops depends on *where* the problem is:
///
/// - **End-of-records sentinel** (a `payload_len` of `0`): iteration ends
///   cleanly — the next call returns `None`. This is the normal end of a sealed
///   segment.
/// - **Torn trailing write at EOF**: a write interrupted partway through the
///   4-byte `payload_len` field at the very end of an un-sealed *active* segment
///   hits [`io::ErrorKind::UnexpectedEof`] on the length read and also ends
///   cleanly — the next call returns `None`, never an `Err`. Sealed segments
///   always end in a sentinel, so this only arises mid-write on an active one.
/// - **Mid-stream corruption**: a CRC32 mismatch (kind
///   [`io::ErrorKind::InvalidData`]), an oversized `payload_len` past
///   [`MAX_PAYLOAD_HARD_CAP`] (also `InvalidData`), or a truncation *after* a
///   valid length field — i.e. a short read of the CRC or payload bytes
///   (`UnexpectedEof`, wrapped with a "segment truncated mid-record" context but
///   keeping the `UnexpectedEof` kind) — yields a single `Some(Err(..))` and then
///   iteration stops (every subsequent call returns `None`).
///
/// In short: all good records up to the first corruption, then a single clean
/// `Err` — except a torn `payload_len` at EOF, which is indistinguishable from a
/// clean end and so returns `None` rather than `Err`.
#[derive(Debug)]
pub struct SegmentReader {
    reader: BufReader<File>,
    done: bool,
    header: SegmentHeaderMeta,
    /// Set in `next`'s terminal branches: `Some(true)` once a `payload_len == 0`
    /// sentinel is consumed, `Some(false)` if a torn `payload_len` EOF ended
    /// iteration, `None` while iteration has not yet ended. See
    /// [`SegmentReader::terminated_cleanly`].
    terminated_cleanly: Option<bool>,
}

impl SegmentReader {
    /// Opens a segment file and validates its header (magic + format version)
    /// before any records are read. Fails with [`io::ErrorKind::InvalidData`]
    /// for a bad magic or an unknown format version.
    ///
    /// The parsed header is retained and exposed via [`SegmentReader::header`].
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header = [0u8; SEGMENT_HEADER_LEN];
        if let Err(e) = reader.read_exact(&mut header) {
            // A file shorter than the fixed header can't carry one. Give the
            // bare "failed to fill whole buffer" a human-readable context (like
            // bad-magic/bad-version do); other read errors propagate as-is.
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment file too short: need a {SEGMENT_HEADER_LEN}-byte header, file is truncated/empty: {}",
                        path.display()
                    ),
                ));
            }
            return Err(e);
        }

        if header[0..4] != SEGMENT_MAGIC {
            let found = &header[0..4];
            // Lead with the ASCII rendering (each non-printable byte shown as
            // '.'), keeping the raw bytes for debugging. SEGMENT_MAGIC is the
            // wire/segment magic b"WEIR".
            let ascii: String = found
                .iter()
                .map(|&b| if b.is_ascii_graphic() { b as char } else { '.' })
                .collect();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad segment magic: found {ascii:?} ({found:?}), expected b\"WEIR\""),
            ));
        }
        if header[4] != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown segment format version: {}", header[4]),
            ));
        }

        // Magic and version are already validated above (with richer, path-aware
        // messages). Re-parse the same bytes to capture shard_id + created_at;
        // the structural checks cannot fail here, but map any error rather than
        // unwrap, so a future format change can never panic.
        let header = parse_segment_header(&header).map_err(|e| match e {
            SegmentHeaderParseError::WrongLength { .. } => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment header is not {SEGMENT_HEADER_LEN} bytes"),
            ),
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        })?;

        Ok(SegmentReader {
            reader,
            done: false,
            header,
            terminated_cleanly: None,
        })
    }

    /// The parsed segment header (shard id, format version, creation time),
    /// captured at [`open`](SegmentReader::open).
    pub fn header(&self) -> &SegmentHeaderMeta {
        &self.header
    }

    /// Consumes the reader and returns the underlying [`BufReader<File>`],
    /// positioned wherever iteration left it. Resume-after-error escape hatch:
    /// a forensics tool that hits an [`Err`] item can take ownership of the
    /// reader to seek past the damage and continue inspecting the file.
    pub fn into_inner(self) -> BufReader<File> {
        self.reader
    }

    /// Borrows the underlying [`BufReader<File>`] without consuming the reader.
    /// Like [`into_inner`](SegmentReader::into_inner) but non-destructive — for
    /// peeking at the current file position after an error.
    pub fn get_ref(&self) -> &BufReader<File> {
        &self.reader
    }

    /// How iteration ended, or `None` if it has not yet ended.
    ///
    /// - `None` — iteration is still in progress (no terminal branch reached).
    /// - `Some(true)` — a `payload_len == 0` end-of-records sentinel was
    ///   consumed: a clean termination, the normal end of a sealed segment.
    /// - `Some(false)` — a torn `payload_len` field hit EOF: an active segment
    ///   that ended mid-write with no sentinel. Iteration still ended cleanly
    ///   (with `None`, never `Err`), but the absence of the sentinel is the
    ///   forensic signal that the file was never sealed.
    ///
    /// Note this distinguishes only the two clean terminal branches. A mid-stream
    /// error (CRC mismatch, oversized length, truncation after a valid length)
    /// surfaces as an `Err` *item* and leaves this `None`; the caller already
    /// observes that failure directly.
    pub fn terminated_cleanly(&self) -> Option<bool> {
        self.terminated_cleanly
    }
}

/// Adds a "segment truncated mid-record" context to a short-read error hit
/// *after* a valid `payload_len` field, so the mid-stream truncation case reads
/// like the contextful open-time messages instead of the bare stdlib "failed to
/// fill whole buffer". Only an [`io::ErrorKind::UnexpectedEof`] (a genuine short
/// read of the CRC or payload bytes) is wrapped — and the wrapped error
/// *preserves* `UnexpectedEof`, because the documented iteration contract (and
/// callers, e.g. the recovery path's truncation test) switch on that kind. Any
/// other read error (e.g. a real I/O failure) propagates unchanged.
fn truncate_context(e: io::Error, payload_len: usize, what: &str) -> io::Error {
    if e.kind() != io::ErrorKind::UnexpectedEof {
        return e;
    }
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!(
            "segment truncated mid-record: short read of the {what} for a record \
             declaring {payload_len} payload bytes ({e})"
        ),
    )
}

impl Iterator for SegmentReader {
    type Item = io::Result<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Torn trailing write at EOF (or a clean end with no sentinel):
                // ends iteration cleanly. Set `done` like every other terminal
                // branch so the fused guarantee holds without relying on
                // BufReader<File> happening to keep returning EOF.
                self.done = true;
                self.terminated_cleanly = Some(false);
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            self.done = true;
            self.terminated_cleanly = Some(true);
            return None; // sentinel
        }

        // Cap check before allocation — MAX_PAYLOAD_HARD_CAP from weir-core.
        if payload_len > MAX_PAYLOAD_HARD_CAP {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record payload_len {payload_len} exceeds MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"
                ),
            )));
        }

        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            self.done = true;
            return Some(Err(truncate_context(e, payload_len, "CRC field")));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut payload_buf = vec![0u8; payload_len];
        if let Err(e) = self.reader.read_exact(&mut payload_buf) {
            self.done = true;
            return Some(Err(truncate_context(e, payload_len, "payload bytes")));
        }

        let computed_crc = crc32fast::hash(&payload_buf);
        if expected_crc != computed_crc {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record CRC mismatch: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
                ),
            )));
        }

        // Freeze: O(1) ownership transfer from Vec allocation to Bytes.
        Some(Ok(Payload::from(payload_buf)))
    }
}

// Every terminal branch of `next` sets `self.done = true` before returning
// (the EOF-on-`payload_len`, sentinel, oversized-len, CRC-mismatch, short-read,
// and other-read-error branches), and `done` short-circuits to `None` on entry.
// Once the iterator yields `None` it therefore yields `None` forever, so the
// fused guarantee is exact — make it explicit (and free) for callers.
impl std::iter::FusedIterator for SegmentReader {}

/// A sealed segment that passed [`verify_sealed_segment`]: its header parsed, its
/// record framing walked to the sentinel, its footer present, and the footer's
/// `file_crc32` matched a fresh CRC32 over every pre-sentinel byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentVerification {
    /// The parsed segment header.
    pub header: SegmentHeaderMeta,
    /// The parsed segment footer (record count, data bytes, CRC, seal time).
    pub footer: SegmentFooterMeta,
}

/// Why [`verify_sealed_segment`] rejected a file. Structured like the format
/// module's parse errors (`Display` + [`std::error::Error`], with a
/// [`From<io::Error>`](From) so the streaming reads can use `?`).
#[derive(Debug)]
#[non_exhaustive]
pub enum SegmentVerifyError {
    /// An underlying I/O error while reading the file.
    Io(io::Error),
    /// The fixed-size header failed to parse.
    Header(SegmentHeaderParseError),
    /// The file is too short to hold even a header.
    TooShort,
    /// A record's `payload_len` past [`MAX_PAYLOAD_HARD_CAP`], or a CRC mismatch,
    /// or any other structural fault hit while walking record framing.
    BadRecord(io::Error),
    /// Reached EOF while walking records without ever seeing the end-of-records
    /// sentinel — the file is not a cleanly sealed segment.
    NoSentinel,
    /// The sentinel was found but the trailing [`SEGMENT_FOOTER_LEN`]-byte footer
    /// is absent or truncated.
    MissingFooter,
    /// The footer's `file_crc32` did not match a fresh CRC32 over all bytes
    /// before the sentinel — the canonical accidental-corruption signal.
    CrcMismatch {
        /// The `file_crc32` recorded in the footer.
        expected: u32,
        /// The CRC32 freshly computed over every pre-sentinel byte.
        computed: u32,
    },
    /// The header, records, sentinel, and footer all parsed, but there are
    /// extra bytes after the footer — a sealed segment ends at the footer, so a
    /// trailing byte signals appended garbage or a concatenated second segment.
    TrailingBytes,
    /// The footer's `record_count` / `data_bytes` did not match the records
    /// actually walked. These two fields sit AFTER the sentinel, so `file_crc32`
    /// (which covers only pre-sentinel bytes) does not protect them — a corrupt
    /// value here would otherwise be handed to a forensics/ctl consumer as truth.
    FooterMismatch {
        /// `"record_count"` or `"data_bytes"`.
        field: &'static str,
        /// The value recorded in the footer.
        expected: u64,
        /// The value computed by walking the records.
        computed: u64,
    },
}

impl std::fmt::Display for SegmentVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error verifying segment: {e}"),
            Self::Header(e) => write!(f, "segment header invalid: {e}"),
            Self::TooShort => write!(f, "file too short to contain a segment header"),
            Self::BadRecord(e) => write!(f, "corrupt record framing: {e}"),
            Self::NoSentinel => write!(
                f,
                "reached EOF without an end-of-records sentinel: not a sealed segment"
            ),
            Self::MissingFooter => write!(
                f,
                "end-of-records sentinel found but the {SEGMENT_FOOTER_LEN}-byte footer is missing or truncated"
            ),
            Self::CrcMismatch { expected, computed } => write!(
                f,
                "whole-file CRC mismatch: footer recorded {expected:#010x}, computed {computed:#010x}"
            ),
            Self::TrailingBytes => write!(
                f,
                "extra bytes after the footer: a sealed segment must end at its footer"
            ),
            Self::FooterMismatch {
                field,
                expected,
                computed,
            } => write!(
                f,
                "footer {field} mismatch: footer recorded {expected}, records walked give {computed}"
            ),
        }
    }
}

impl std::error::Error for SegmentVerifyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) | Self::BadRecord(e) => Some(e),
            Self::Header(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for SegmentVerifyError {
    fn from(e: io::Error) -> Self {
        SegmentVerifyError::Io(e)
    }
}

/// Whole-file integrity check for a *sealed* segment — the forensics primitive.
///
/// Validates the header, walks the record framing (`len` / `crc` / payload) up to
/// the end-of-records sentinel, reads the trailing footer, then recomputes a
/// CRC32 over **every byte from offset 0 up to but not including the 4-byte
/// sentinel** and compares it to the footer's `file_crc32`.
///
/// The walk is *streamed* with a [`crc32fast::Hasher`] and fixed-size reads — it
/// never slurps the whole file, so it is safe on a full-size segment (up to
/// [`format::SEGMENT_MAX_BYTES`], 256 MiB).
///
/// Returns the parsed header + footer on success, or a structured
/// [`SegmentVerifyError`] describing the first fault found. An active/unsealed
/// segment has no sentinel + footer, so it surfaces as
/// [`SegmentVerifyError::NoSentinel`].
pub fn verify_sealed_segment(
    path: impl AsRef<Path>,
) -> Result<SegmentVerification, SegmentVerifyError> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut hasher = crc32fast::Hasher::new();

    // --- Header: read, hash, parse. ---
    let mut header_buf = [0u8; SEGMENT_HEADER_LEN];
    match reader.read_exact(&mut header_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(SegmentVerifyError::TooShort);
        }
        Err(e) => return Err(SegmentVerifyError::Io(e)),
    }
    hasher.update(&header_buf);
    let header = parse_segment_header(&header_buf).map_err(SegmentVerifyError::Header)?;

    // --- Records: walk framing to the sentinel, hashing every pre-sentinel byte. ---
    let mut walked_records: u64 = 0;
    let mut walked_data_bytes: u64 = 0;
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // EOF where a length (or sentinel) was expected: never sealed.
                return Err(SegmentVerifyError::NoSentinel);
            }
            Err(e) => return Err(SegmentVerifyError::Io(e)),
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            // Sentinel reached. Do NOT hash these 4 bytes — the footer CRC
            // covers only the pre-sentinel bytes. Break to read the footer.
            break;
        }
        walked_records += 1;
        walked_data_bytes += payload_len as u64;
        // A real record header → hash the length field.
        hasher.update(&len_buf);

        if payload_len > MAX_PAYLOAD_HARD_CAP {
            return Err(SegmentVerifyError::BadRecord(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record payload_len {payload_len} exceeds MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"
                ),
            )));
        }

        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf).map_err(|e| {
            SegmentVerifyError::BadRecord(truncate_context(e, payload_len, "CRC field"))
        })?;
        hasher.update(&crc_buf);
        let expected_crc = u32::from_le_bytes(crc_buf);

        // Stream the payload through the file hasher *and* a per-record hasher in
        // bounded chunks, so a 256 MiB record never lands in one allocation.
        let mut record_hasher = crc32fast::Hasher::new();
        let mut remaining = payload_len;
        let mut chunk = [0u8; 64 * 1024];
        while remaining > 0 {
            let want = remaining.min(chunk.len());
            reader.read_exact(&mut chunk[..want]).map_err(|e| {
                SegmentVerifyError::BadRecord(truncate_context(e, payload_len, "payload bytes"))
            })?;
            hasher.update(&chunk[..want]);
            record_hasher.update(&chunk[..want]);
            remaining -= want;
        }
        let computed_crc = record_hasher.finalize();
        if expected_crc != computed_crc {
            return Err(SegmentVerifyError::BadRecord(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record CRC mismatch: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
                ),
            )));
        }
    }

    // --- Footer: read the trailing fixed-size block. ---
    let mut footer_buf = [0u8; SEGMENT_FOOTER_LEN];
    match reader.read_exact(&mut footer_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(SegmentVerifyError::MissingFooter);
        }
        Err(e) => return Err(SegmentVerifyError::Io(e)),
    }
    // parse_segment_footer's only failure is a wrong length, which a fixed-size
    // [u8; SEGMENT_FOOTER_LEN] cannot trigger — but map it rather than unwrap.
    let footer =
        parse_segment_footer(&footer_buf).map_err(|_| SegmentVerifyError::MissingFooter)?;

    let computed = hasher.finalize();
    if footer.file_crc32 != computed {
        return Err(SegmentVerifyError::CrcMismatch {
            expected: footer.file_crc32,
            computed,
        });
    }

    // The footer's record_count / data_bytes sit after the sentinel, so file_crc32
    // does not protect them. Cross-check against what we actually walked so a
    // corrupt count is never reported to a consumer as truth.
    if footer.record_count != walked_records {
        return Err(SegmentVerifyError::FooterMismatch {
            field: "record_count",
            expected: footer.record_count,
            computed: walked_records,
        });
    }
    if footer.data_bytes != walked_data_bytes {
        return Err(SegmentVerifyError::FooterMismatch {
            field: "data_bytes",
            expected: footer.data_bytes,
            computed: walked_data_bytes,
        });
    }

    // A sealed segment ends exactly at its footer. Any byte beyond it is
    // appended garbage or a concatenated second segment — reject rather than
    // silently accept a file whose tail we never validated.
    let mut extra = [0u8; 1];
    match reader.read(&mut extra) {
        Ok(0) => {} // clean EOF: nothing trails the footer.
        Ok(_) => return Err(SegmentVerifyError::TrailingBytes),
        Err(e) => return Err(SegmentVerifyError::Io(e)),
    }

    Ok(SegmentVerification { header, footer })
}

/// The on-disk lifecycle state of a segment file, inferred from its extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// `.wab` — an active, unsealed segment still being written.
    Active,
    /// `.wab.sealed` — a sealed segment awaiting drain.
    Sealed,
    /// `.wab.confirmed` — the drain-confirmation sidecar for a sealed segment.
    Confirmed,
}

/// Lists segment-related files in `dir`, each tagged with its [`SegmentState`].
///
/// Classifies by extension ([`EXT_CONFIRMED`](format::EXT_CONFIRMED),
/// [`EXT_SEALED`](format::EXT_SEALED), [`EXT_ACTIVE`](format::EXT_ACTIVE)),
/// checked most-specific-first so `.wab.sealed` / `.wab.confirmed` are not
/// mis-classified as `.wab`. Files matching none of the three are ignored. The
/// result is sorted deterministically by path.
pub fn list_segment_files(dir: impl AsRef<Path>) -> io::Result<Vec<(PathBuf, SegmentState)>> {
    let dir = dir.as_ref();
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        // Most-specific suffix first: `.wab.confirmed` and `.wab.sealed` both end
        // in something containing `.wab`, so the order of these checks matters.
        let state = if name.ends_with(EXT_CONFIRMED) {
            SegmentState::Confirmed
        } else if name.ends_with(EXT_SEALED) {
            SegmentState::Sealed
        } else if name.ends_with(EXT_ACTIVE) {
            SegmentState::Active
        } else {
            continue;
        };
        out.push((path, state));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use format::{SENTINEL, build_segment_header};
    use std::io::Write;
    use std::path::PathBuf;

    fn tmp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "weir_wab_reader_{label}_{}.wab",
            std::process::id()
        ))
    }

    /// Writes a minimal sealed-style segment: header, then `[len][crc][payload]`
    /// per record, then the sentinel. The footer is not required by the reader
    /// (it stops at the sentinel), so this is enough to drive `SegmentReader`.
    fn write_segment(path: &Path, records: &[&[u8]]) {
        let mut f = File::create(path).unwrap();
        f.write_all(&build_segment_header(0xFFFF)).unwrap();
        for r in records {
            f.write_all(&(r.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&crc32fast::hash(r).to_le_bytes()).unwrap();
            f.write_all(r).unwrap();
        }
        f.write_all(&SENTINEL).unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn reads_all_records_in_order() {
        let path = tmp_path("order");
        write_segment(&path, &[b"alpha", b"beta", b"gamma"]);
        let got: Vec<Payload> = SegmentReader::open(&path)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            got,
            vec![
                Payload::from_static(b"alpha"),
                Payload::from_static(b"beta"),
                Payload::from_static(b"gamma"),
            ]
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_segment_yields_no_records() {
        let path = tmp_path("empty");
        write_segment(&path, &[]);
        let got: Vec<_> = SegmentReader::open(&path).unwrap().collect();
        assert!(got.is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = tmp_path("badmagic");
        let mut f = File::create(&path).unwrap();
        let mut header = build_segment_header(0);
        header[0] = b'X'; // "WEIR" -> "XEIR"
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        let err = SegmentReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Human-readable: leads with the ASCII rendering of the found bytes and
        // names the expected magic.
        let msg = err.to_string();
        assert!(msg.contains("XEIR"), "expected ASCII rendering in: {msg}");
        assert!(
            msg.contains("expected b\"WEIR\""),
            "expected the expected-magic hint in: {msg}"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_bad_magic_renders_nonprintable_bytes_as_dots() {
        let path = tmp_path("badmagic_nonprintable");
        let mut f = File::create(&path).unwrap();
        let mut header = build_segment_header(0);
        // Fully non-printable magic: ASCII rendering should be all dots, but the
        // raw bytes must still be present for debugging.
        header[0..4].copy_from_slice(&[0x00, 0x01, 0x02, 0x03]);
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        let err = SegmentReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains("\"....\""), "expected dotted ASCII in: {msg}");
        assert!(msg.contains("expected b\"WEIR\""), "msg: {msg}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_unknown_version() {
        let path = tmp_path("badversion");
        let mut f = File::create(&path).unwrap();
        let mut header = build_segment_header(0);
        header[4] = 99;
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        let err = SegmentReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn crc_mismatch_is_reported_as_invalid_data() {
        let path = tmp_path("crc");
        // Hand-write a record whose stored CRC doesn't match the payload.
        let mut f = File::create(&path).unwrap();
        f.write_all(&build_segment_header(0)).unwrap();
        let payload = b"corruptme";
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&0xdead_beefu32.to_le_bytes()).unwrap(); // wrong CRC
        f.write_all(payload).unwrap();
        f.write_all(&SENTINEL).unwrap();
        f.sync_all().unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let first = reader.next().unwrap();
        let err = first.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Iteration ends after the error.
        assert!(reader.next().is_none());
        // A mid-stream error surfaces as an `Err` item, not a clean terminal
        // branch — so neither `terminated_cleanly` flag is set.
        assert_eq!(reader.terminated_cleanly(), None);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn iterating_past_end_stays_none_and_is_fused() {
        // FusedIterator is a compile-time bound here; the runtime check is that
        // repeated next() after exhaustion keeps returning None.
        fn assert_fused<I: std::iter::FusedIterator>(_it: &I) {}

        let path = tmp_path("fused_unit");
        write_segment(&path, &[b"x", b"y"]);
        let mut reader = SegmentReader::open(&path).unwrap();
        assert_fused(&reader);
        assert_eq!(reader.next().unwrap().unwrap(), Payload::from_static(b"x"));
        assert_eq!(reader.next().unwrap().unwrap(), Payload::from_static(b"y"));
        assert!(reader.next().is_none()); // sentinel
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn torn_len_at_eof_ends_cleanly_and_sets_done() {
        // An *active* (un-sealed) segment that ends right after a complete
        // record — no sentinel — hits UnexpectedEof on the next payload_len
        // read. That must end cleanly (None, never Err) and stay fused.
        let path = tmp_path("torn_len");
        let mut f = File::create(&path).unwrap();
        f.write_all(&build_segment_header(0)).unwrap();
        let r = b"rec";
        f.write_all(&(r.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&crc32fast::hash(r).to_le_bytes()).unwrap();
        f.write_all(r).unwrap(); // ends here: no sentinel, no next len
        f.sync_all().unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Payload::from_static(b"rec")
        );
        assert!(reader.next().is_none(), "torn len at EOF ends cleanly");
        assert!(reader.next().is_none(), "stays None after end");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_on_short_file_reports_too_short() {
        let path = tmp_path("short");
        File::create(&path).unwrap().write_all(b"WEI").unwrap(); // 3 bytes
        let err = SegmentReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains("too short"), "expected 'too short' in: {msg}");
        assert!(msg.contains("header"), "expected 'header' in: {msg}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_on_empty_file_reports_too_short() {
        let path = tmp_path("zero");
        File::create(&path).unwrap(); // 0 bytes
        let err = SegmentReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains("too short"), "expected 'too short' in: {msg}");
        assert!(msg.contains("header"), "expected 'header' in: {msg}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn oversized_payload_len_rejected_before_alloc() {
        let path = tmp_path("oversize");
        let mut f = File::create(&path).unwrap();
        f.write_all(&build_segment_header(0)).unwrap();
        // payload_len just over the hard cap; no payload bytes follow — the
        // reader must reject on the length check, not try to allocate/read it.
        let bogus_len = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
        f.write_all(&bogus_len.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.sync_all().unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let err = reader.next().unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // A structural error is NOT a clean termination (mirrors the CRC path).
        assert_eq!(reader.terminated_cleanly(), None);
        std::fs::remove_file(&path).ok();
    }

    /// After an iteration error the reader stays recoverable: `get_ref` and
    /// `into_inner` hand back the underlying reader rather than panicking, so a
    /// forensics consumer can inspect the raw bytes around a corruption.
    #[test]
    fn into_inner_after_error_yields_usable_reader() {
        use std::io::Read;
        let path = tmp_path("into_inner_after_err");
        let mut bytes = build_segment_header(0).to_vec();
        // record 0: valid.
        let r0 = b"hello";
        bytes.extend_from_slice(&(r0.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(r0).to_le_bytes());
        bytes.extend_from_slice(r0);
        // record 1: a CRC that does not match the payload → iteration Err.
        let r1 = b"world";
        bytes.extend_from_slice(&(r1.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        bytes.extend_from_slice(r1);
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Payload::from_static(b"hello")
        );
        assert!(
            reader.next().unwrap().is_err(),
            "record 1's bad CRC must error"
        );
        // get_ref is callable post-error; into_inner recovers the reader and it is
        // still usable for raw reads (no panic / no poisoning of the file handle).
        let _ = reader.get_ref();
        let mut inner = reader.into_inner();
        let mut rest = Vec::new();
        inner
            .read_to_end(&mut rest)
            .expect("reader usable after an iteration error");
        std::fs::remove_file(&path).ok();
    }

    // ---- Forensics read surface (sweep #6) ----

    use format::build_segment_footer;

    /// Builds a fully sealed segment to a `Vec<u8>`: header, records, sentinel,
    /// and a footer carrying a correct whole-file CRC over the pre-sentinel
    /// bytes. The single source of truth for the verify tests; returns the exact
    /// byte boundaries the tests need to corrupt or truncate.
    struct SealedBytes {
        bytes: Vec<u8>,
        /// Offset of the sentinel (== length of all pre-sentinel bytes).
        sentinel_off: usize,
        /// Offset of the first payload byte of the first record (for flipping).
        first_payload_off: usize,
    }

    fn build_sealed_segment(shard: u16, records: &[&[u8]]) -> SealedBytes {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(shard));
        let mut data_bytes = 0u64;
        let mut first_payload_off = 0usize;
        for (i, r) in records.iter().enumerate() {
            bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
            if i == 0 {
                first_payload_off = bytes.len();
            }
            bytes.extend_from_slice(r);
            data_bytes += r.len() as u64;
        }
        let sentinel_off = bytes.len();
        let file_crc = crc32fast::hash(&bytes); // CRC over all pre-sentinel bytes
        bytes.extend_from_slice(&SENTINEL);
        bytes.extend_from_slice(&build_segment_footer(
            records.len() as u64,
            data_bytes,
            file_crc,
            1_700_000_000_000_000_000i64,
        ));
        SealedBytes {
            bytes,
            sentinel_off,
            first_payload_off,
        }
    }

    fn write_bytes(label: &str, bytes: &[u8]) -> PathBuf {
        let path = tmp_path(label);
        let mut f = File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
        path
    }

    #[test]
    fn open_parses_and_exposes_header() {
        let path = tmp_path("hdr_smoke");
        write_segment(&path, &[b"x"]);
        let reader = SegmentReader::open(&path).unwrap();
        let h = reader.header();
        assert_eq!(h.shard_id, 0xFFFF); // write_segment uses 0xFFFF
        assert_eq!(h.format_version, format::FORMAT_VERSION);
        assert!(h.created_at > 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn into_inner_returns_underlying_reader() {
        let path = tmp_path("into_inner");
        write_segment(&path, &[b"only"]);
        let mut reader = SegmentReader::open(&path).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Payload::from_static(b"only")
        );
        // get_ref borrows non-destructively; into_inner takes ownership.
        let _ = reader.get_ref();
        let inner = reader.into_inner();
        // The escape hatch yields a usable BufReader<File>.
        let _: BufReader<File> = inner;
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn terminated_cleanly_true_on_sealed_segment() {
        let path = tmp_path("term_clean");
        write_segment(&path, &[b"a", b"b"]); // ends in a sentinel
        let mut reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.terminated_cleanly(), None); // not yet ended
        while reader.next().is_some() {}
        assert_eq!(reader.terminated_cleanly(), Some(true));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn terminated_cleanly_false_on_torn_active_tail() {
        // Active segment that ends right after a complete record, no sentinel.
        let path = tmp_path("term_torn");
        let mut f = File::create(&path).unwrap();
        f.write_all(&build_segment_header(0)).unwrap();
        let r = b"rec";
        f.write_all(&(r.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&crc32fast::hash(r).to_le_bytes()).unwrap();
        f.write_all(r).unwrap();
        f.sync_all().unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        while reader.next().is_some() {}
        assert_eq!(reader.terminated_cleanly(), Some(false));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_ok_with_correct_metas() {
        let sealed = build_sealed_segment(7, &[b"alpha", b"beta", b"gamma"]);
        let path = write_bytes("verify_ok", &sealed.bytes);
        let v = verify_sealed_segment(&path).unwrap();
        assert_eq!(v.header.shard_id, 7);
        assert_eq!(v.footer.record_count, 3);
        assert_eq!(v.footer.data_bytes, (5 + 4 + 5) as u64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_empty_is_ok() {
        let sealed = build_sealed_segment(0, &[]);
        let path = write_bytes("verify_empty", &sealed.bytes);
        let v = verify_sealed_segment(&path).unwrap();
        assert_eq!(v.footer.record_count, 0);
        assert_eq!(v.footer.data_bytes, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_detects_flipped_payload_byte() {
        let mut sealed = build_sealed_segment(1, &[b"corruptme"]);
        // Flip a payload byte: the record CRC catches it first (BadRecord), and
        // even were that to pass, the whole-file CRC would not match.
        sealed.bytes[sealed.first_payload_off] ^= 0xff;
        let path = write_bytes("verify_flip", &sealed.bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::BadRecord(_)) => {}
            other => panic!("expected BadRecord on flipped payload, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_detects_file_crc_mismatch() {
        // Corrupt the footer's file_crc32 directly: framing + record CRCs all
        // pass, only the whole-file CRC check fails. file_crc32 is at footer
        // bytes [16..20]; the footer starts at sentinel_off + 4.
        let mut sealed = build_sealed_segment(2, &[b"hello", b"world"]);
        let footer_off = sealed.sentinel_off + SENTINEL.len();
        sealed.bytes[footer_off + 16] ^= 0xff;
        let path = write_bytes("verify_crc", &sealed.bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_truncated_before_footer_errors() {
        // Keep the sentinel but drop the footer entirely.
        let sealed = build_sealed_segment(3, &[b"data"]);
        let truncated = &sealed.bytes[..sealed.sentinel_off + SENTINEL.len()];
        let path = write_bytes("verify_nofoot", truncated);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::MissingFooter) => {}
            other => panic!("expected MissingFooter, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_no_sentinel_is_rejected() {
        // Active segment (header + a record, no sentinel/footer): NoSentinel.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(0));
        let r = b"rec";
        bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
        bytes.extend_from_slice(r);
        let path = write_bytes("verify_nosent", &bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::NoSentinel) => {}
            other => panic!("expected NoSentinel, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_too_short_for_header() {
        let path = write_bytes("verify_short", b"WEI");
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::TooShort) => {}
            other => panic!("expected TooShort, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_rejects_trailing_bytes() {
        // A valid sealed segment with one stray byte appended after the footer.
        // Everything parses + CRCs match, but the file does not end at the
        // footer, so the trailing-byte check rejects it.
        let mut sealed = build_sealed_segment(4, &[b"alpha", b"beta"]);
        sealed.bytes.push(0xAB);
        let path = write_bytes("verify_trailing", &sealed.bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::TrailingBytes) => {}
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_detects_footer_count_corruption() {
        // record_count / data_bytes sit AFTER the sentinel, so file_crc32 does not
        // cover them — corrupting either still leaves the CRC valid and must be
        // caught by the footer cross-check.
        let base = build_sealed_segment(7, &[b"alpha", b"beta", b"gamma"]); // 3 recs, 14 bytes
        let foot = base.bytes.len() - SEGMENT_FOOTER_LEN;

        let mut b1 = base.bytes.clone();
        b1[foot..foot + 8].copy_from_slice(&99u64.to_le_bytes());
        let p1 = write_bytes("verify_footer_rc", &b1);
        match verify_sealed_segment(&p1) {
            Err(SegmentVerifyError::FooterMismatch {
                field: "record_count",
                expected: 99,
                computed: 3,
            }) => {}
            other => panic!("expected record_count FooterMismatch, got {other:?}"),
        }
        std::fs::remove_file(&p1).ok();

        let mut b2 = base.bytes.clone();
        b2[foot + 8..foot + 16].copy_from_slice(&12_345u64.to_le_bytes());
        let p2 = write_bytes("verify_footer_db", &b2);
        match verify_sealed_segment(&p2) {
            Err(SegmentVerifyError::FooterMismatch {
                field: "data_bytes",
                expected: 12_345,
                computed: 14,
            }) => {}
            other => panic!("expected data_bytes FooterMismatch, got {other:?}"),
        }
        std::fs::remove_file(&p2).ok();
    }

    #[test]
    fn list_segment_files_propagates_read_dir_error() {
        // Fail closed on a missing directory rather than returning an empty Ok.
        let missing = tmp_path("list_missing_dir");
        let err = list_segment_files(&missing).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn verify_sealed_segment_rejects_bad_header() {
        // Corrupt the header magic on an otherwise valid sealed segment: the
        // header parse fails before any records are walked.
        let mut sealed = build_sealed_segment(5, &[b"data"]);
        sealed.bytes[0] = b'X'; // "WEIR" -> "XEIR"
        let path = write_bytes("verify_bad_header", &sealed.bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::Header(_)) => {}
            other => panic!("expected Header(..), got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn verify_sealed_segment_rejects_oversized_record() {
        // Hand-build a segment whose first record declares a payload_len past
        // MAX_PAYLOAD_HARD_CAP. The verifier rejects on the length check
        // (BadRecord) before attempting to read/hash the bogus payload.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(6));
        let bogus_len = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
        bytes.extend_from_slice(&bogus_len.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
        let path = write_bytes("verify_oversize", &bytes);
        match verify_sealed_segment(&path) {
            Err(SegmentVerifyError::BadRecord(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
            }
            other => panic!("expected BadRecord on oversized record, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn list_segment_files_classifies_and_sorts() {
        let dir = std::env::temp_dir().join(format!("weir_wab_list_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Create one of each + an unrelated file that must be ignored.
        for name in [
            "seg_00000002.wab",
            "seg_00000000.wab.sealed",
            "seg_00000001.wab.confirmed",
            "notes.txt",
        ] {
            File::create(dir.join(name)).unwrap();
        }
        let listed = list_segment_files(&dir).unwrap();
        // notes.txt ignored; the other three present, sorted by path.
        let names: Vec<(String, SegmentState)> = listed
            .iter()
            .map(|(p, s)| (p.file_name().unwrap().to_string_lossy().into_owned(), *s))
            .collect();
        assert_eq!(
            names,
            vec![
                ("seg_00000000.wab.sealed".to_string(), SegmentState::Sealed),
                (
                    "seg_00000001.wab.confirmed".to_string(),
                    SegmentState::Confirmed
                ),
                ("seg_00000002.wab".to_string(), SegmentState::Active),
            ]
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
