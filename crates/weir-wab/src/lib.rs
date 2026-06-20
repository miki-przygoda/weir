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
//! See [`mod@format`] for the on-disk byte layout.

pub mod format;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use weir_core::MAX_PAYLOAD_HARD_CAP;

use format::{FORMAT_VERSION, SEGMENT_HEADER_LEN, SEGMENT_MAGIC};

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
///   (`UnexpectedEof`) — yields a single `Some(Err(..))` and then iteration
///   stops (every subsequent call returns `None`).
///
/// In short: all good records up to the first corruption, then a single clean
/// `Err` — except a torn `payload_len` at EOF, which is indistinguishable from a
/// clean end and so returns `None` rather than `Err`.
#[derive(Debug)]
pub struct SegmentReader {
    reader: BufReader<File>,
    done: bool,
}

impl SegmentReader {
    /// Opens a segment file and validates its header (magic + format version)
    /// before any records are read. Fails with [`io::ErrorKind::InvalidData`]
    /// for a bad magic or an unknown format version.
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

        Ok(SegmentReader {
            reader,
            done: false,
        })
    }
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
            return Some(Err(e));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut payload_buf = vec![0u8; payload_len];
        if let Err(e) = self.reader.read_exact(&mut payload_buf) {
            self.done = true;
            return Some(Err(e));
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
        std::fs::remove_file(&path).ok();
    }
}
