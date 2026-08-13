//! A forensic reader that continues past a corrupt record.
//!
//! [`SegmentReader`](crate::SegmentReader) stops at the first bad record, and
//! crash recovery and the drain depend on that. This reader exists for a
//! different job: reading a **quarantined** segment, where the whole point is
//! the records sitting *after* the corruption.
//!
//! # Why that matters
//!
//! When recovery meets mid-file corruption it truncates and seals the valid
//! prefix (which is delivered normally) and copies the *whole* original to
//! `quarantine/`, precisely because acked-durable records may sit after the
//! corrupt one. Those records exist nowhere else, and a reader that stops at
//! the corruption cannot reach them.
//!
//! # It never fabricates records
//!
//! Two guards. A record whose declared length exceeds the segment's stored cap
//! makes the position of the next record unknowable, so iteration ends with
//! [`RecoveryItem::Desynced`]. And a sustained run of consecutive verification
//! failures — the signature of a length corrupted to a plausible-but-wrong
//! value, which leaves the reader mid-record — also ends iteration. Recovering
//! nothing is strictly better than inventing data.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use weir_core::{MAX_PAYLOAD_HARD_CAP, Payload};

use crate::format::{
    Compression, SEGMENT_HEADER_LEN, SegmentHeaderMeta, max_stored_record_bytes,
    parse_segment_header,
};

/// How many consecutive verification failures end iteration. A single corrupt
/// record is the case this reader exists for; a run of them means the reader has
/// lost the framing and everything it produces afterwards would be fiction.
const MAX_CONSECUTIVE_SKIPS: u32 = 3;

/// One step of a forensic read.
#[derive(Debug)]
pub enum RecoveryItem {
    /// A record whose CRC verified. Safe to re-deliver.
    Record(Payload),
    /// A record that failed verification and was stepped over. The reader was
    /// already positioned at the next record, so iteration continues.
    Skipped {
        /// Byte offset of the record's length field.
        offset: u64,
        /// The length the record declared.
        declared_len: u32,
        /// Why it failed.
        reason: String,
    },
    /// The reader can no longer establish where the next record begins.
    /// Iteration ends after this item.
    Desynced {
        /// Byte offset at which the reader gave up.
        offset: u64,
        /// Why.
        reason: String,
    },
}

/// Reads a segment forensically, continuing past corrupt records.
#[derive(Debug)]
pub struct RecoveryReader {
    reader: BufReader<File>,
    header: SegmentHeaderMeta,
    /// Byte offset of the next record's length field.
    pos: u64,
    done: bool,
    consecutive_skips: u32,
}

impl RecoveryReader {
    /// Opens a segment and validates its header. Fails only when the header
    /// itself is unreadable — every later problem becomes an item, not an error.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut header = [0u8; SEGMENT_HEADER_LEN];
        reader.read_exact(&mut header)?;
        let header = parse_segment_header(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(RecoveryReader {
            reader,
            header,
            pos: SEGMENT_HEADER_LEN as u64,
            done: false,
            consecutive_skips: 0,
        })
    }

    /// The parsed segment header.
    pub fn header(&self) -> &SegmentHeaderMeta {
        &self.header
    }

    fn desync(&mut self, reason: impl Into<String>) -> Option<RecoveryItem> {
        self.done = true;
        Some(RecoveryItem::Desynced {
            offset: self.pos,
            reason: reason.into(),
        })
    }
}

impl Iterator for RecoveryReader {
    type Item = RecoveryItem;

    fn next(&mut self) -> Option<RecoveryItem> {
        if self.done {
            return None;
        }
        let record_offset = self.pos;

        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            // A clean end of records with no sentinel — the segment simply
            // stopped. Not a desync; nothing was misread.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.done = true;
                return None;
            }
            Err(e) => return self.desync(format!("read length field: {e}")),
        }
        let declared_len = u32::from_le_bytes(len_buf);
        if declared_len == 0 {
            // End-of-records sentinel.
            self.done = true;
            return None;
        }

        // Guard 1: an implausible length means the next record's position is
        // unknowable, so stop rather than guess.
        let cap = match self.header.compression {
            Compression::None => MAX_PAYLOAD_HARD_CAP,
            Compression::Zstd => max_stored_record_bytes(),
        };
        if declared_len as usize > cap {
            return self.desync(format!(
                "record declares {declared_len} bytes, above the stored cap {cap}; \
                 cannot locate the next record"
            ));
        }

        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            return self.desync(format!("read CRC field: {e}"));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut stored = vec![0u8; declared_len as usize];
        if let Err(e) = self.reader.read_exact(&mut stored) {
            return self.desync(format!(
                "truncated: record declares {declared_len} bytes, read failed: {e}"
            ));
        }
        // Reading the payload already advanced us to the next record boundary —
        // that is what makes resync free rather than a seek.
        self.pos = record_offset + 8 + declared_len as u64;

        if crc32fast::hash(&stored) != expected_crc {
            self.consecutive_skips += 1;
            // Guard 2: a run of failures means the framing is lost — a length
            // corrupted to a plausible value leaves us mid-record, and every
            // "record" after it is fiction.
            if self.consecutive_skips >= MAX_CONSECUTIVE_SKIPS {
                return self.desync(format!(
                    "{MAX_CONSECUTIVE_SKIPS} consecutive records failed verification; \
                     the record framing is lost"
                ));
            }
            return Some(RecoveryItem::Skipped {
                offset: record_offset,
                declared_len,
                reason: "CRC mismatch".to_string(),
            });
        }
        self.consecutive_skips = 0;

        let plain = match self.header.compression {
            Compression::None => stored,
            Compression::Zstd => match zstd::bulk::decompress(&stored, MAX_PAYLOAD_HARD_CAP) {
                Ok(p) => p,
                Err(e) => {
                    self.consecutive_skips += 1;
                    return Some(RecoveryItem::Skipped {
                        offset: record_offset,
                        declared_len,
                        reason: format!("decompression failed: {e}"),
                    });
                }
            },
        };
        Some(RecoveryItem::Record(Payload::from(plain)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::format::{Compression, SENTINEL, build_segment_header};
    use std::io::Write;
    use std::path::PathBuf;

    fn tmp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "weir_recovery_reader_{label}_{}.wab.sealed",
            std::process::id()
        ))
    }

    /// Writes a segment, optionally corrupting one record's PAYLOAD (leaving its
    /// length intact) or its LENGTH field (which is unrecoverable).
    fn write_segment(
        path: &std::path::Path,
        records: &[&[u8]],
        corrupt_payload_at: Option<usize>,
        corrupt_len_at: Option<usize>,
    ) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&build_segment_header(0, Compression::None));
        for (i, r) in records.iter().enumerate() {
            let mut len = r.len() as u32;
            if corrupt_len_at == Some(i) {
                // A wildly implausible length: the reader cannot locate the next
                // record, so it must give up rather than guess.
                len = u32::MAX - 1;
            }
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
            let mut payload = r.to_vec();
            if corrupt_payload_at == Some(i) {
                payload[0] ^= 0xff; // CRC now mismatches; length still correct
            }
            buf.extend_from_slice(&payload);
        }
        buf.extend_from_slice(&SENTINEL);
        std::fs::File::create(path)
            .unwrap()
            .write_all(&buf)
            .unwrap();
    }

    #[test]
    fn clean_segment_yields_only_records() {
        let p = tmp_path("clean");
        write_segment(&p, &[b"one", b"two", b"three"], None, None);
        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        assert_eq!(items.len(), 3);
        assert!(items.iter().all(|i| matches!(i, RecoveryItem::Record(_))));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn corrupt_payload_is_skipped_and_the_tail_is_recovered() {
        // THE test this whole feature exists for. SegmentReader stops at the bad
        // record; RecoveryReader must reach the records after it.
        let p = tmp_path("skip_tail");
        write_segment(
            &p,
            &[b"before", b"CORRUPT", b"after1", b"after2"],
            Some(1),
            None,
        );

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        let records: Vec<&[u8]> = items
            .iter()
            .filter_map(|i| match i {
                RecoveryItem::Record(pl) => Some(pl.as_ref()),
                _ => None,
            })
            .collect();
        assert_eq!(
            records,
            vec![&b"before"[..], &b"after1"[..], &b"after2"[..]],
            "the records AFTER the corruption must be recovered"
        );
        let skipped = items
            .iter()
            .filter(|i| matches!(i, RecoveryItem::Skipped { .. }))
            .count();
        assert_eq!(skipped, 1, "exactly the corrupt record is skipped");
        assert!(
            !items
                .iter()
                .any(|i| matches!(i, RecoveryItem::Desynced { .. })),
            "a payload-only corruption must not desync"
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn corrupt_length_desyncs_rather_than_fabricating_records() {
        // The guard that separates recovering data from inventing it.
        let p = tmp_path("desync_len");
        write_segment(&p, &[b"before", b"BADLEN", b"after"], None, Some(1));

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        assert!(
            matches!(items.last(), Some(RecoveryItem::Desynced { .. })),
            "an implausible length must end iteration with Desynced, got {items:?}"
        );
        let records = items
            .iter()
            .filter(|i| matches!(i, RecoveryItem::Record(_)))
            .count();
        assert_eq!(
            records, 1,
            "only the record before the bad length is recovered"
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_run_of_consecutive_failures_desyncs() {
        // A length corrupted to a plausible-but-wrong value leaves the reader
        // mid-record, so everything after is garbage. Bound the cascade.
        let p = tmp_path("cascade");
        write_segment(
            &p,
            &[
                b"aaaaaaaa",
                b"bbbbbbbb",
                b"cccccccc",
                b"dddddddd",
                b"eeeeeeee",
            ],
            None,
            None,
        );
        // Corrupt every record's PAYLOAD so every CRC fails in a row, leaving the
        // length fields intact. Corrupting the length fields too would trip the
        // cap guard on the first record instead, and this test would pass without
        // ever exercising MAX_CONSECUTIVE_SKIPS — which is the guard it names.
        // Layout: 24-byte header, then per 8-byte record [len 4][crc 4][payload 8],
        // so record i's payload occupies 32 + 16*i .. 40 + 16*i.
        let mut bytes = std::fs::read(&p).unwrap();
        for i in 0..5 {
            for b in bytes[32 + 16 * i..40 + 16 * i].iter_mut() {
                *b ^= 0x5a;
            }
        }
        std::fs::write(&p, &bytes).unwrap();

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        // Pin WHICH guard fired. Asserting only "last is Desynced" would also
        // pass if the cap guard tripped on record 0, and this test would never
        // exercise the run-of-skips guard it is named for.
        match items.last() {
            Some(RecoveryItem::Desynced { reason, .. }) => assert!(
                reason.contains("consecutive"),
                "must desync on the consecutive-skip guard, not the cap guard: {reason}"
            ),
            other => panic!("a sustained run of failures must stop, not emit garbage: {other:?}"),
        }
        assert!(
            items.len() < 5,
            "iteration must stop before walking all five records, got {items:?}"
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn truncated_tail_desyncs_cleanly() {
        let p = tmp_path("truncated");
        write_segment(&p, &[b"one", b"two"], None, None);
        let len = std::fs::metadata(&p).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&p)
            .unwrap()
            .set_len(len - 6)
            .unwrap();

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        assert!(items.iter().any(|i| matches!(i, RecoveryItem::Record(_))));
        assert!(matches!(items.last(), Some(RecoveryItem::Desynced { .. })));
        std::fs::remove_file(&p).ok();
    }
}
