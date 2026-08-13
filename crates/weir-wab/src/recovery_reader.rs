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
//!
//! # A clean end is *verified*, never assumed
//!
//! After a [`RecoveryItem::Skipped`] the reader may have lost alignment, and a
//! CRC mismatch cannot distinguish "the payload bytes flipped" (framing intact)
//! from "the length was wrong, so we read the wrong bytes" (framing lost). A
//! misaligned reader that lands on four zero bytes — extremely common inside
//! real payloads — would otherwise read them as the end-of-records sentinel and
//! stop, silently leaving recoverable records behind while reporting success.
//! That is the one outcome worse than reporting failure, because the whole
//! feature exists to reach records that are unreachable elsewhere.
//!
//! So iteration ends cleanly (a bare `None`, no item) only when the walk
//! accounts for the file *exactly*: the frames summed to a position that can
//! legitimately end a segment. Anything else is [`RecoveryItem::Desynced`]. The
//! check is positional and therefore exact — it never scans forward looking for
//! a plausible next header, because that heuristic can itself be wrong, and
//! being wrong there means fabricating a record.
//!
//! The consumer contract this buys: **absence of a trailing `Desynced` means
//! the walk genuinely reached the end of the record stream.**

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use weir_core::{MAX_PAYLOAD_HARD_CAP, Payload};

use crate::format::{
    Compression, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, SENTINEL, SegmentHeaderMeta,
    max_stored_record_bytes, parse_segment_header,
};

/// How many consecutive verification failures end iteration. A single corrupt
/// record is the case this reader exists for; a run of them means the reader has
/// lost the framing and everything it produces afterwards would be fiction.
///
/// Public so operator-facing output and documentation can name the bound rather
/// than duplicating the number and letting it drift.
pub const MAX_CONSECUTIVE_SKIPS: u32 = 3;

/// One step of a forensic read.
///
/// `#[non_exhaustive]`: this ships in a published crate, and a forensic reader
/// is likely to grow ways of describing what it found. Matching on it requires a
/// wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
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
    /// Size of the file, captured at open. The end-of-stream checks are
    /// positional, so they need it.
    file_len: u64,
    done: bool,
    consecutive_skips: u32,
    /// Set when guard 2 trips, so the failing record's `Skipped` is emitted
    /// first and the `Desynced` follows on the next call. Without this the third
    /// failing record would produce no item at all and the `Desynced` would
    /// carry the *following* record's offset.
    pending_desync: Option<(u64, String)>,
}

impl RecoveryReader {
    /// Opens a segment and validates its header. Fails only when the header
    /// itself is unreadable — every later problem becomes an item, not an error.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut header = [0u8; SEGMENT_HEADER_LEN];
        reader.read_exact(&mut header).map_err(|e| {
            // Match SegmentReader::open's contextual message rather than letting
            // the bare "failed to fill whole buffer" reach an operator.
            if e.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment file too short: need a {SEGMENT_HEADER_LEN}-byte header, \
                         file is truncated/empty: {}",
                        path.display()
                    ),
                )
            } else {
                e
            }
        })?;
        let header = parse_segment_header(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(RecoveryReader {
            reader,
            header,
            pos: SEGMENT_HEADER_LEN as u64,
            file_len,
            done: false,
            consecutive_skips: 0,
            pending_desync: None,
        })
    }

    /// The parsed segment header.
    pub fn header(&self) -> &SegmentHeaderMeta {
        &self.header
    }

    /// Whether four zero bytes at `offset` can be a *genuine* end-of-records
    /// sentinel, judged purely by position — the exact alternative to guessing.
    ///
    /// A sentinel is written only at seal time, immediately followed by the
    /// fixed-size footer, so in a sealed segment it occupies exactly one offset.
    /// The footer-less case is admitted too, and is safe by construction: if the
    /// sentinel is the last thing in the file there are no bytes left after it,
    /// so treating it as the end discards nothing whatever the alignment.
    ///
    /// Residual: a misaligned reader landing on four zero bytes at precisely the
    /// sealed-sentinel offset is accepted, leaving at most the footer's worth of
    /// bytes unexamined. Tightening that would require guessing at content,
    /// which is the thing this reader must never do.
    fn is_sentinel_offset(&self, offset: u64) -> bool {
        let after = offset.saturating_add(SENTINEL.len() as u64);
        after.saturating_add(SEGMENT_FOOTER_LEN as u64) == self.file_len || after == self.file_len
    }

    fn desync(&mut self, offset: u64, reason: impl Into<String>) -> Option<RecoveryItem> {
        self.done = true;
        Some(RecoveryItem::Desynced {
            offset,
            reason: reason.into(),
        })
    }
}

impl Iterator for RecoveryReader {
    type Item = RecoveryItem;

    fn next(&mut self) -> Option<RecoveryItem> {
        // Guard 2 queues this behind the failing record's own `Skipped`.
        if let Some((offset, reason)) = self.pending_desync.take() {
            self.done = true;
            return Some(RecoveryItem::Desynced { offset, reason });
        }
        if self.done {
            return None;
        }
        let record_offset = self.pos;

        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.done = true;
                // An active segment carries no sentinel — it simply stops — so
                // running out exactly at EOF is its clean end. "Exactly" is the
                // load-bearing word: it means every frame summed to the file
                // length, which proves alignment held. Stray bytes mean a
                // partial length field, which after a skip is indistinguishable
                // from a misaligned landing.
                if record_offset == self.file_len {
                    return None;
                }
                let stray = self.file_len.saturating_sub(record_offset);
                return Some(RecoveryItem::Desynced {
                    offset: record_offset,
                    reason: format!(
                        "{stray} trailing byte(s) at EOF: a partial length field, so the \
                         record stream did not end on a frame boundary"
                    ),
                });
            }
            Err(e) => return self.desync(record_offset, format!("read length field: {e}")),
        }
        let declared_len = u32::from_le_bytes(len_buf);
        if declared_len == 0 {
            self.done = true;
            // Four zero bytes are the end-of-records sentinel only where a
            // sentinel can actually sit. Anywhere else they are payload the
            // reader has drifted into, and stopping there would silently
            // discard every record that follows.
            if self.is_sentinel_offset(record_offset) {
                return None;
            }
            return Some(RecoveryItem::Desynced {
                offset: record_offset,
                reason: format!(
                    "four zero bytes at offset {record_offset}, which is not where a \
                     sentinel can sit in a {}-byte segment; the reader has drifted into \
                     payload and cannot locate the next record",
                    self.file_len
                ),
            });
        }

        // Guard 1: an implausible length means the next record's position is
        // unknowable, so stop rather than guess.
        let cap = match self.header.compression {
            Compression::None => MAX_PAYLOAD_HARD_CAP,
            Compression::Zstd => max_stored_record_bytes(),
        };
        if declared_len as usize > cap {
            return self.desync(
                record_offset,
                format!(
                    "record declares {declared_len} bytes, above the stored cap {cap}; \
                     cannot locate the next record"
                ),
            );
        }

        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            return self.desync(record_offset, format!("read CRC field: {e}"));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut stored = vec![0u8; declared_len as usize];
        if let Err(e) = self.reader.read_exact(&mut stored) {
            return self.desync(
                record_offset,
                format!("truncated: record declares {declared_len} bytes, read failed: {e}"),
            );
        }
        // Reading the payload already advanced us to the next record boundary —
        // that is what makes resync free rather than a seek.
        self.pos = record_offset + 8 + declared_len as u64;

        if crc32fast::hash(&stored) != expected_crc {
            self.consecutive_skips += 1;
            // Guard 2: a run of failures means the framing is lost — a length
            // corrupted to a plausible value leaves us mid-record, and every
            // "record" after it is fiction. Queue the desync rather than
            // returning it, so this record still reports its own `Skipped` and
            // the `Desynced` carries this record's offset, not the next one's.
            if self.consecutive_skips >= MAX_CONSECUTIVE_SKIPS {
                self.pending_desync = Some((
                    record_offset,
                    format!(
                        "{MAX_CONSECUTIVE_SKIPS} consecutive records failed verification; \
                         the record framing is lost"
                    ),
                ));
            }
            return Some(RecoveryItem::Skipped {
                offset: record_offset,
                declared_len,
                reason: "CRC mismatch".to_string(),
            });
        }
        // A CRC match proves this record's framing was intact, so the cascade
        // budget resets here regardless of what decoding does below.
        self.consecutive_skips = 0;

        let plain = match self.header.compression {
            Compression::None => stored,
            Compression::Zstd => match zstd::bulk::decompress(&stored, MAX_PAYLOAD_HARD_CAP) {
                Ok(p) => p,
                Err(e) => {
                    // Deliberately does NOT touch `consecutive_skips`. This
                    // failure arrives *after* the CRC matched, so the reader is
                    // provably still aligned — it is a decode failure, not a
                    // sync-loss signal. Feeding it to the cascade budget would
                    // let a single undecodable record shorten the run to two
                    // and desync early, losing recoverable records behind it.
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

// Every terminal branch sets `done`, which short-circuits on entry, and the one
// deferred item (`pending_desync`) is taken before that check and sets `done`
// itself. Once this yields `None` it yields `None` forever, so the fused
// guarantee is exact — state it, as `SegmentReader` does.
impl std::iter::FusedIterator for RecoveryReader {}

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
        // Pin WHICH guard fired. Asserting only "last is Desynced" passes even
        // with the cap guard deleted: the reader then allocates ~4 GiB, the
        // payload read hits EOF, and the truncation branch desyncs for an
        // unrelated reason. Naming the reason is what makes this cover guard 1.
        match items.last() {
            Some(RecoveryItem::Desynced { reason, .. }) => assert!(
                reason.contains("stored cap"),
                "an implausible length must desync on the CAP guard: {reason}"
            ),
            other => panic!("an implausible length must end iteration with Desynced: {other:?}"),
        }
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
    fn a_length_one_byte_over_the_cap_desyncs_before_allocating() {
        // The boundary case, and the one that isolates the cap guard from every
        // other branch: nothing follows the length field, so with the guard
        // removed the reader would allocate 16 MiB on a corrupt length before
        // discovering there is nothing to read into it.
        let p = tmp_path("cap_boundary");
        let mut bytes = build_segment_header(0, Compression::None).to_vec();
        bytes.extend_from_slice(&((MAX_PAYLOAD_HARD_CAP + 1) as u32).to_le_bytes());
        std::fs::write(&p, &bytes).unwrap();

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        match items.as_slice() {
            [RecoveryItem::Desynced { offset, reason }] => {
                assert_eq!(
                    *offset, SEGMENT_HEADER_LEN as u64,
                    "offset of the bad record"
                );
                assert!(
                    reason.contains("stored cap"),
                    "must desync on the cap guard: {reason}"
                );
            }
            other => panic!("expected exactly one Desynced from the cap guard, got {other:?}"),
        }
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
            Some(RecoveryItem::Desynced { reason, offset }) => {
                assert!(
                    reason.contains("consecutive"),
                    "must desync on the consecutive-skip guard, not the cap guard: {reason}"
                );
                // The offset must name the record that FAILED, not the one after
                // it. Record i's length field sits at 24 + 16*i, so the third
                // failure is at 56.
                assert_eq!(
                    *offset, 56,
                    "Desynced must carry the failing record's offset"
                );
            }
            other => panic!("a sustained run of failures must stop, not emit garbage: {other:?}"),
        }
        // The third failing record is still a skip and must still be reported —
        // a consumer counting skips would otherwise undercount by one.
        let skipped = items
            .iter()
            .filter(|i| matches!(i, RecoveryItem::Skipped { .. }))
            .count();
        assert_eq!(
            skipped, MAX_CONSECUTIVE_SKIPS as usize,
            "every failing record reports its own Skipped, got {items:?}"
        );
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
        match items.last() {
            Some(RecoveryItem::Desynced { reason, .. }) => assert!(
                reason.contains("truncated"),
                "must desync on the truncation branch specifically: {reason}"
            ),
            other => panic!("a truncated tail must end with Desynced, got {other:?}"),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn zero_bytes_mid_file_are_not_mistaken_for_the_sentinel() {
        // A misaligned reader landing on four zero bytes must NOT report a clean
        // end. A CRC mismatch cannot tell "payload bytes flipped" (framing
        // intact) from "the length was wrong" (framing lost); in the second case
        // the reader resumes at an arbitrary offset, and zero runs are common
        // inside real payloads. Reporting success there would silently discard
        // the acked records this whole feature exists to reach.
        let p = tmp_path("false_clean_end");
        let mut bytes = build_segment_header(0, Compression::None).to_vec();
        // Record 0 at 24: declares 12 bytes with a CRC that will not match, so
        // it is skipped and the reader resumes at 24 + 8 + 12 = 44.
        bytes.extend_from_slice(&12u32.to_le_bytes());
        bytes.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        bytes.extend_from_slice(&[0xAA; 12]);
        // 44..48: four zero bytes the drifted reader will read as a length.
        bytes.extend_from_slice(&[0u8; 4]);
        // 48: an intact record that a "clean end" at 44 would silently drop.
        let hello = b"hello";
        bytes.extend_from_slice(&(hello.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(hello).to_le_bytes());
        bytes.extend_from_slice(hello);
        std::fs::write(&p, &bytes).unwrap();

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        match items.last() {
            Some(RecoveryItem::Desynced { offset, reason }) => {
                assert_eq!(*offset, 44, "must give up at the zero run, not before");
                assert!(
                    reason.contains("zero bytes"),
                    "must name the drift, not a clean end: {reason}"
                );
            }
            other => panic!(
                "four zero bytes mid-file must desync, not silently end the walk \
                 and drop the intact record behind them; got {other:?}"
            ),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_decompression_failure_does_not_shorten_the_cascade_budget() {
        // A decompression failure arrives AFTER the CRC matched, so that
        // record's framing was provably intact and the reader is provably still
        // aligned. It is a decode failure, not a sync-loss signal. If it fed the
        // consecutive-skip budget, two CRC failures would desync instead of
        // three and the good record at the end would be lost.
        let p = tmp_path("zstd_budget");
        let mut bytes = build_segment_header(0, Compression::Zstd).to_vec();

        // Record 0: CRC valid over bytes that are not a zstd frame.
        let not_a_frame = b"definitely not a zstd frame";
        bytes.extend_from_slice(&(not_a_frame.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(not_a_frame).to_le_bytes());
        bytes.extend_from_slice(not_a_frame);

        // Records 1 and 2: real frames with deliberately wrong CRCs.
        for r in [&b"one"[..], &b"two"[..]] {
            let frame = zstd::bulk::compress(r, 1).unwrap();
            bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
            bytes.extend_from_slice(&frame);
        }

        // Record 3: intact, and the one that must survive.
        let frame = zstd::bulk::compress(b"recovered", 1).unwrap();
        bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&frame).to_le_bytes());
        bytes.extend_from_slice(&frame);
        bytes.extend_from_slice(&SENTINEL);
        std::fs::write(&p, &bytes).unwrap();

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
            vec![&b"recovered"[..]],
            "the record after two CRC failures must survive a preceding \
             decompression failure, got {items:?}"
        );
        assert!(
            !items
                .iter()
                .any(|i| matches!(i, RecoveryItem::Desynced { .. })),
            "three skips of which one was a decode failure must not desync: {items:?}"
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn open_on_a_short_file_explains_itself() {
        // Task 3 puts this string in front of an operator, so it must say more
        // than "failed to fill whole buffer".
        let p = tmp_path("short_open");
        std::fs::write(&p, b"WEI").unwrap();
        let err = RecoveryReader::open(&p).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains("too short"), "expected 'too short' in: {msg}");
        assert!(msg.contains("header"), "expected 'header' in: {msg}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn iteration_is_fused_after_a_desync() {
        fn assert_fused<I: std::iter::FusedIterator>(_it: &I) {}

        let p = tmp_path("fused");
        write_segment(&p, &[b"before", b"BADLEN", b"after"], None, Some(1));
        let mut reader = RecoveryReader::open(&p).unwrap();
        assert_fused(&reader);
        while reader.next().is_some() {}
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        std::fs::remove_file(&p).ok();
    }
}
