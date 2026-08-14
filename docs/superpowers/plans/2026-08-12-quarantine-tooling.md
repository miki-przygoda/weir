# Quarantine tooling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make quarantined records reachable. Today weir preserves acked
records into `quarantine/` when recovery meets corruption, and nothing ships
that can read them back.

**Architecture:** A new `RecoveryReader` in `weir-wab` that continues past a
corrupt record instead of stopping at it, plus `weir-ctl quarantine
list|inspect|requeue` built on it, an on-disk gauge, and a counter for the
recovery-failure arm that currently only logs.

**Tech Stack:** Rust 2024, `weir-wab`, `weir-ctl`, `prometheus-client`.

## Global Constraints

- **Source spec:** `docs/superpowers/specs/2026-08-12-backpressure-and-quarantine-design.md`,
  §5. Sections 3–4 (the WAB cap) are a **separate plan** — do not implement them here.
- **Branch:** `v2/main-line`.
- **`SegmentReader`'s contract must not change.** It stops at the first bad
  record, and recovery and the drain depend on that. `RecoveryReader` is a
  separate type, not a mode flag.
- **Never fabricate records.** When the reader cannot establish where the next
  record begins, it must stop, not guess. Recovering nothing beats inventing
  data — this is the single most important property in this plan.
- **The gate** for every "run the tests" step. Clippy and `cargo deny` are part
  of it — an earlier version of this list omitted them, and the sibling plan
  (`2026-08-12-wab-cap-and-growth-warning`) hit a red clippy between two of its
  tasks for exactly that reason:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets -- -D warnings
  cargo clippy --all-targets --all-features -- -D warnings
  cargo clippy --all-targets --no-default-features -- -D warnings
  cargo test --workspace --exclude weir-server
  cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
  cargo test -p weir-server --bins -- --test-threads=1
  cargo deny check advisories bans licenses sources
  ```

---

## As built: the Task 1 interfaces Tasks 3–5 must code against

Task 1 shipped at `bb803a1` after a review round, and the public surface differs
from the sketch in Task 1 below (which is preserved as written, not retrofitted).
**These are the facts to code against:**

- **`RecoveryItem` is `#[non_exhaustive]`.** Every `match` on it needs a wildcard
  arm or it will not compile.
- **`MAX_CONSECUTIVE_SKIPS` is public** and re-exported from `weir_wab`, so
  operator-facing output and docs can name it.
- **`RecoveryReader: Iterator<Item = RecoveryItem>`** — the item is the bare enum,
  *not* `io::Result<RecoveryItem>`. It also implements `FusedIterator`.
- **A trailing `Desynced` is always last**; its absence means the walk reached the
  end of the segment.
- **The failing record always gets its own `Skipped` before a `Desynced`**, so a
  cascade reads `[Skipped, Skipped, Skipped, Desynced]` and counts are complete.
- **The clean-end check is positional**, and deliberately accepts a sentinel at
  either `file_len - SENTINEL - SEGMENT_FOOTER_LEN` (sealed) or `file_len -
  SENTINEL` (footer-less) — `recovery_reader.rs:171-172`. The footer-less arm is
  what lets the test helpers below omit the 32-byte footer.
- **A clean end means "every byte is accounted for", never "every record was
  recovered".** A length corrupted to a plausible value can tile exactly to EOF,
  swallowing intact records inside its declared range; nothing is fabricated and
  the `Skipped` names the exact byte range, but the records inside it are gone.
  **Task 3's operator-facing output must be worded that way** — an operator who
  reads "clean" as "everything was recovered" will delete a segment that still
  held data.

---

## A simplification found while reading the code

The spec (§5.2) describes resync as *"skip exactly `declared_len` bytes and
attempt the next header."* Reading `SegmentReader::next` shows that is already
free: it reads the length, the CRC field, **and the full payload**, and only
*then* verifies the CRC. So after a CRC failure the reader is **already
positioned at the next record boundary** — no seek and no explicit skip.

What the guards actually need to protect against is therefore narrower than the
spec assumed, and this plan implements them as:

1. **A length-cap check before reading**, since an implausible `declared_len`
   means we cannot know where the next record starts.
2. **A consecutive-skip limit**, since a length corrupted to a *plausible but
   wrong* value leaves the reader mid-record, and every subsequent "record" is
   garbage. Bounding the run stops a cascade of fabricated `Skipped` items.

Same guarantee as the spec's "plausible next header" rule, achieved more simply
and without a heuristic that could itself be wrong.

---

### Task 1: `RecoveryReader`

**Files:**
- Create: `crates/weir-wab/src/recovery_reader.rs`
- Modify: `crates/weir-wab/src/lib.rs` (module declaration + re-exports)

**Interfaces:**
- Consumes: `SegmentHeaderMeta`, `Compression`, `max_stored_record_bytes`,
  `parse_segment_header`, `Payload` — all existing in `weir-wab`.
- Produces:
  ```rust
  pub enum RecoveryItem {
      Record(Payload),
      Skipped { offset: u64, declared_len: u32, reason: String },
      Desynced { offset: u64, reason: String },
  }
  pub struct RecoveryReader { /* … */ }
  impl RecoveryReader {
      pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;
      pub fn header(&self) -> &SegmentHeaderMeta;
  }
  impl Iterator for RecoveryReader { type Item = RecoveryItem; }
  ```
  Tasks 3 and 4 consume this.

- [ ] **Step 1: Write the failing tests**

Create `crates/weir-wab/src/recovery_reader.rs` with only a test module for
now:

```rust
//! Placeholder — implementation lands in Step 3.

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
        std::fs::File::create(path).unwrap().write_all(&buf).unwrap();
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
        write_segment(&p, &[b"before", b"CORRUPT", b"after1", b"after2"], Some(1), None);

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
            !items.iter().any(|i| matches!(i, RecoveryItem::Desynced { .. })),
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
        assert_eq!(records, 1, "only the record before the bad length is recovered");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn a_run_of_consecutive_failures_desyncs() {
        // A length corrupted to a plausible-but-wrong value leaves the reader
        // mid-record, so everything after is garbage. Bound the cascade.
        let p = tmp_path("cascade");
        write_segment(
            &p,
            &[b"aaaaaaaa", b"bbbbbbbb", b"cccccccc", b"dddddddd", b"eeeeeeee"],
            None,
            None,
        );
        // Corrupt every record's payload so every CRC fails in a row.
        let mut bytes = std::fs::read(&p).unwrap();
        for b in bytes.iter_mut().skip(24) {
            *b ^= 0x5a;
        }
        std::fs::write(&p, &bytes).unwrap();

        let items: Vec<_> = RecoveryReader::open(&p).unwrap().collect();
        assert!(
            matches!(items.last(), Some(RecoveryItem::Desynced { .. })),
            "a sustained run of failures must stop, not emit garbage forever"
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
```

Close the `mod tests` block, and add to `crates/weir-wab/src/lib.rs`:

```rust
mod recovery_reader;
pub use recovery_reader::{RecoveryItem, RecoveryReader};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p weir-wab recovery_reader`
Expected: FAIL — `cannot find type RecoveryReader`.

- [ ] **Step 3: Implement `RecoveryReader`**

Replace the placeholder comment at the top of
`crates/weir-wab/src/recovery_reader.rs` (keep the test module):

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p weir-wab recovery_reader`
Expected: PASS — 5 passed.

If `a_run_of_consecutive_failures_desyncs` fails because iteration ends before
three skips accumulate, the corruption pattern in the test is destroying the
length fields too — adjust the test to corrupt only payload bytes, not the
whole file, rather than weakening `MAX_CONSECUTIVE_SKIPS`.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt
cargo clippy -p weir-wab --all-targets -- -D warnings
cargo test -p weir-wab
```

```bash
git add crates/weir-wab
git commit -m "feat(wab): RecoveryReader — read past a corrupt record

SegmentReader stops at the first bad record, and recovery and the drain depend
on that, so this is a separate type rather than a mode flag.

It exists because of what a quarantined segment actually contains. When
recovery meets mid-file corruption it truncates and seals the valid prefix
(delivered normally) and copies the WHOLE original to quarantine/, precisely
because acked-durable records may sit AFTER the corrupt one. Those records
exist nowhere else, and a reader that stops at the corruption cannot reach
them.

Resync turned out to be free: SegmentReader already reads the full payload
before verifying the CRC, so after a failure the reader is already positioned
at the next record. No seek, no explicit skip.

Two guards, and they are the point. A declared length above the stored cap
makes the next record's position unknowable, so iteration ends with Desynced. A
run of consecutive failures — the signature of a length corrupted to a
plausible-but-wrong value, which leaves the reader mid-record — also ends it.
Recovering nothing beats inventing data, and there is a test for each.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The quarantine gauge and the recovery-failure counter

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs`
- Modify: `crates/weir-server/src/wab/recovery.rs` (the swallowed-error arm)
- Modify: `crates/weir-server/src/drain/mod.rs` (the idle poll that rescans)
- Modify: `crates/weir-server/tests/system.rs` (the metric guard list)

**Interfaces:**
- Consumes: nothing.
- Produces: `Metrics::quarantine_bytes_on_disk: Gauge<f64, AtomicU64>` and
  `Metrics::recovery_segments_failed: Counter<u64, AtomicU64>`.

- [ ] **Step 1: Write the failing test for the counter**

Add to `mod tests` in `crates/weir-server/src/wab/recovery.rs`:

```rust
    #[test]
    fn a_segment_whose_recovery_fails_is_counted_not_just_logged() {
        // Today this arm only logs, so a segment left for manual inspection is
        // invisible to monitoring. On a read-only mount it repeats every boot
        // with nothing but a log line.
        let dir = tmp_dir("recovery_failed_counter");
        let shard_dir = dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        // A file with a valid name but a header too short to parse: recover_segment
        // returns Err, and recover_shard_dir swallows it.
        let path = crate::wab::segment::segment_path(&shard_dir, 1);
        fs::write(&path, b"nope").unwrap();

        let metrics = noop_metrics();
        recover_open_segments(&dir, &metrics).unwrap();
        assert_eq!(
            metrics.recovery_segments_failed.get(),
            1,
            "a swallowed recovery failure must still be counted"
        );
        fs::remove_dir_all(dir).ok();
    }
```

The exact entry point (`recover_open_segments` vs `recover_shard_dir`) and
whether a 4-byte file reaches the error arm rather than the quarantine arm must
be checked against the module — read `recover_shard_dir` first and pick an
input that lands in the `Err(e) =>` arm at `recovery.rs:128-130`.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-server --bins recovery_fails_is_counted -- --test-threads=1`
Expected: FAIL — no field `recovery_segments_failed`.

- [ ] **Step 3: Register both metrics**

In `crates/weir-server/src/metrics/mod.rs`, add the fields:

```rust
    /// Segments whose crash recovery failed and were left for manual inspection.
    pub recovery_segments_failed: Counter<u64, AtomicU64>,
    /// Bytes currently held in the quarantine directory.
    pub quarantine_bytes_on_disk: Gauge<f64, AtomicU64>,
```

and register them:

```rust
        let recovery_segments_failed = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_recovery_segments_failed",
            "WAB segments whose crash recovery returned an error and were left on \
             disk for manual inspection. Non-zero means acked records may be \
             unreachable — check the startup logs for the path and the cause"
        );
        let quarantine_bytes_on_disk = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_quarantine_bytes_on_disk",
            "Bytes held in the quarantine/ subdirectory: forensic copies of \
             corrupt segments preserved because acked records may sit after the \
             corruption. Unlike the quarantine counters this survives a restart. \
             Recover them with `weir-ctl quarantine requeue`"
        );
```

plus both names in the constructor's struct literal.

- [ ] **Step 4: Count the failure**

In `crates/weir-server/src/wab/recovery.rs`, the arm at 128-130 currently reads:

```rust
            Err(e) => {
                error!(path = %path.display(), error = %e, "recovery failed; segment left for manual inspection");
            }
```

Make it:

```rust
            Err(e) => {
                // Counted, not just logged: a segment left here holds acked
                // records that nothing will reach, and on a read-only mount this
                // repeats every boot. A log line is not an alertable signal.
                metrics.recovery_segments_failed.inc();
                error!(path = %path.display(), error = %e, "recovery failed; segment left for manual inspection");
            }
```

- [ ] **Step 5: Refresh the quarantine gauge**

In `crates/weir-server/src/drain/mod.rs`, at the idle poll that already calls
`dead_letter.rescan()` (~line 486), add a quarantine scan beside it. Reuse the
same directory-sizing approach `dead_letter` uses rather than writing a second
one; the quarantine dir is `<wab_dir>/quarantine`.

Set `metrics.quarantine_bytes_on_disk` to the total. On a missing directory the
total is `0` — an absent quarantine dir is the normal, healthy case and must
not error.

- [ ] **Step 6: Add both to the metric guard list**

`crates/weir-server/tests/system.rs`, beside the other entries:

```rust
        "weir_recovery_segments_failed",
        "weir_quarantine_bytes_on_disk",
```

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test -p weir-server --bins -- --test-threads=1
cargo test -p weir-server --test system metrics_all_families_registered
```

```bash
git add crates/weir-server
git commit -m "feat(metrics): count recovery failures and gauge quarantine bytes

Two signals that did not exist. recover_shard_dir caught recover_segment errors
with a log line and no counter, so a segment left for manual inspection was
invisible to monitoring — and on a read-only mount that repeats every boot.

Quarantine had only per-process counters, which vanish on restart, while
dead-letter and live-WAB bytes both had on-disk gauges. So the directory
holding preserved acked records was the one place monitoring could not see.
weir_quarantine_bytes_on_disk closes that asymmetry.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `weir-ctl quarantine list | inspect`

**Files:**
- Modify: `crates/weir-ctl/src/main.rs`
- Modify: `crates/weir-ctl/Cargo.toml` (if `weir-wab` is not already a dependency)

**Interfaces:**
- Consumes: `RecoveryReader`, `RecoveryItem` (Task 1).
- Produces: the `QuarantineCommand` enum that Task 4 extends with `Requeue`.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `crates/weir-ctl/src/main.rs`, mirroring the existing
`dl` tests:

```rust
    #[test]
    fn quarantine_list_on_a_missing_dir_is_not_an_error() {
        // No quarantine dir is the normal, healthy case.
        let dir = tmp_dir("q_list_missing");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(cmd_quarantine_list(&dir, false).is_ok());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_list_reports_segments_and_bytes() {
        let dir = tmp_dir("q_list");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"a", b"b"]);
        assert!(cmd_quarantine_list(&dir, false).is_ok());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_list_finds_both_extensions_and_collision_suffixes() {
        // THE regression test for this command. Crash recovery quarantines
        // ACTIVE segments, so its copies end `.wab` — and that is the mid-file
        // corruption case, the one where acked records sit after the corruption.
        // A `.wab.sealed`-only filter lists zero of them and reports success.
        // The drain contributes `.wab.sealed`, and non_clobbering_dest appends
        // `.N` to either on a name collision.
        let dir = tmp_dir("q_list_exts");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"a"]);
        write_q_segment(&q, "shard_01__seg_00000002.wab.sealed", &[b"b"]);
        write_q_segment(&q, "shard_00__seg_00000001.wab.1", &[b"c"]);
        write_q_segment(&q, "shard_01__seg_00000002.wab.sealed.2", &[b"d"]);
        std::fs::write(q.join("operator-notes.txt"), b"not a segment").unwrap();

        let segs = quarantine_segments(&q).unwrap();
        assert_eq!(
            segs.len(),
            4,
            "both extensions and their collision suffixes must be listed, got {segs:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_inspect_reports_recovered_and_skipped_counts() {
        // The diagnostic that does not exist today: which records are readable,
        // which are not, and where.
        let dir = tmp_dir("q_inspect");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        let report = quarantine_inspect_report(&q.join(name)).unwrap();
        assert_eq!(report.recovered, 2, "records either side of the corruption");
        assert_eq!(report.skipped, 1);
        assert!(!report.desynced, "a payload-only corruption must not desync");
        std::fs::remove_dir_all(dir).ok();
    }
```

`write_q_segment` / `write_q_segment_with_corruption` build a segment the same
way Task 1's test helper does — header, `len|crc|payload` per record, sentinel.
Factor one helper rather than two if the shapes converge.

**On the missing footer.** These helpers write no 32-byte segment footer, and
that is fine *by design*: `RecoveryReader`'s clean-end check accepts a sentinel
at `file_len - SENTINEL - SEGMENT_FOOTER_LEN` (sealed) **or** at
`file_len - SENTINEL` (footer-less), see `recovery_reader.rs:171-172`. If that
second arm is ever "simplified" away these tests desync instead of reporting a
clean end. Do not add a fake footer to work around a failure here — find out
which arm changed.

**Mutation check before moving on.** Tighten `is_quarantined_segment_name` to
`.wab.sealed` only and confirm
`quarantine_list_finds_both_extensions_and_collision_suffixes` goes red. If it
stays green the test is not pinning the thing it exists to pin.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p weir-ctl quarantine`
Expected: FAIL — `cannot find function cmd_quarantine_list`.

- [ ] **Step 3: Add the subcommand enum**

In `crates/weir-ctl/src/main.rs`, beside `Dl(DlCommand)`:

```rust
    /// Inspect and recover quarantined segments.
    ///
    /// Quarantine holds forensic copies of segments where recovery met
    /// corruption. Acked records may sit AFTER the corrupt record, and those
    /// records exist nowhere else — this is how you get them back.
    #[command(subcommand)]
    Quarantine(QuarantineCommand),
```

and the enum:

```rust
/// Subcommands under `weir-ctl quarantine`.
#[derive(Subcommand)]
enum QuarantineCommand {
    /// List quarantined segments (count + bytes + origin shard).
    List {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
    },
    /// Report what is readable in one quarantined segment: how many records
    /// verify, how many are corrupt and at what offsets, and whether the reader
    /// lost the record framing entirely.
    Inspect {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Segment file name, as printed by `quarantine list`.
        segment: String,
    },
}
```

Wire both into the dispatch `match` beside the `Dl` arm.

**Do NOT add a `json` field to these variants.** `--json` is declared once as a
**global** flag on `Cli` (`#[arg(long, global = true)]`, `main.rs:35`) and
threaded in at dispatch — `DlCommand::List { wab_dir } => cmd_dl_list(&wab_dir,
json)` (`main.rs:149`). Declaring it per-variant would collide with the global
one. That is why the test snippets above call `cmd_quarantine_list(&dir, false)`
with a trailing bool the enum does not mention: it comes from `cli.json`. Follow
the same shape — `QuarantineCommand::List { wab_dir } => cmd_quarantine_list(&wab_dir, json)`.

- [ ] **Step 4: Implement the helpers and commands**

```rust
fn quarantine_dir(wab_dir: &Path) -> PathBuf {
    wab_dir.join("quarantine")
}

/// Every quarantined segment with its size, sorted by name.
///
/// A missing directory yields an empty list, not an error: no quarantine dir is
/// the normal, healthy state.
///
/// **The extension is NOT `.wab.sealed` only** — that assumption would hide
/// exactly the segments this command exists for. Quarantine names are
/// `{shard_name}__{original_file_name}` (`recovery.rs` `quarantine` /
/// `copy_to_quarantine`), and there are two producers writing different
/// extensions:
///
/// - **crash recovery** processes only files ending in `EXT_ACTIVE` (`.wab`),
///   so its copies are `shard_00__seg_00000001.wab`. This is the mid-file
///   corruption case — the one where acked records sit AFTER the corruption,
///   which is the entire premise of this feature;
/// - **the drain** quarantines sealed segments, so its copies end `.wab.sealed`.
///
/// On top of that, `non_clobbering_dest` appends `.1` … `.10000` on a name
/// collision, to *either* form. So `shard_00__seg_00000001.wab.sealed.1` is a
/// legal quarantined name.
///
/// Match the `.wab` / `.wab.sealed` stem with an optional numeric suffix. Do not
/// tighten this to one extension.
fn quarantine_segments(q_dir: &Path) -> Result<Vec<(PathBuf, u64)>, String> {
    let entries = match std::fs::read_dir(q_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", q_dir.display())),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let is_segment = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_quarantined_segment_name);
        if !is_segment {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        out.push((path, size));
    }
    out.sort();
    Ok(out)
}

/// Whether a quarantine directory entry is a preserved WAB segment.
///
/// Accepts `…​.wab` and `…​.wab.sealed`, each optionally followed by the
/// `.N` collision suffix `non_clobbering_dest` adds. Anything else in the
/// directory (an operator's notes, a partial copy) is ignored rather than
/// offered up for requeue.
fn is_quarantined_segment_name(name: &str) -> bool {
    // Strip a trailing `.<digits>` collision suffix, if any, then require one of
    // the two segment extensions.
    let stem = match name.rsplit_once('.') {
        Some((head, tail)) if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) => head,
        _ => name,
    };
    stem.ends_with(".wab") || stem.ends_with(".wab.sealed")
}

/// What a forensic read of one segment found.
struct QuarantineReport {
    recovered: usize,
    skipped: usize,
    desynced: bool,
    skipped_offsets: Vec<u64>,
    desync_reason: Option<String>,
}

fn quarantine_inspect_report(path: &Path) -> Result<QuarantineReport, String> {
    let reader = weir_wab::RecoveryReader::open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut r = QuarantineReport {
        recovered: 0,
        skipped: 0,
        desynced: false,
        skipped_offsets: Vec::new(),
        desync_reason: None,
    };
    for item in reader {
        match item {
            weir_wab::RecoveryItem::Record(_) => r.recovered += 1,
            weir_wab::RecoveryItem::Skipped { offset, .. } => {
                r.skipped += 1;
                r.skipped_offsets.push(offset);
            }
            weir_wab::RecoveryItem::Desynced { reason, .. } => {
                r.desynced = true;
                r.desync_reason = Some(reason);
            }
            // `RecoveryItem` is #[non_exhaustive] (it ships in a published
            // crate), so this arm is REQUIRED to compile. Counting an unknown
            // variant as recovered would overstate what requeue can deliver, and
            // counting it as skipped would understate it — so it is neither.
            other => {
                return Err(format!(
                    "unknown RecoveryItem variant {other:?} — weir-ctl is older \
                     than the weir-wab it is reading; upgrade weir-ctl before \
                     trusting this report"
                ));
            }
        }
    }
    Ok(r)
}
```

`cmd_quarantine_list` mirrors `cmd_dl_list`: `ensure_wab_dir`, enumerate
`*.wab.sealed` under `quarantine/`, print name + bytes (plus the origin shard,
which is the `shard_NN__` prefix), honour `--json`, and treat a missing
directory as empty rather than an error.

`cmd_quarantine_inspect` prints the report: records recovered, records skipped
with their offsets, and — if the reader desynced — the reason, plus the
explicit note that records beyond that point are **not** recoverable by this
tool.

- [ ] **Step 5: Run to verify they pass**

Run: `cargo test -p weir-ctl quarantine`
Expected: PASS — 3 passed.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt
cargo clippy -p weir-ctl --all-targets -- -D warnings
cargo test -p weir-ctl
```

```bash
git add crates/weir-ctl
git commit -m "feat(ctl): weir-ctl quarantine list and inspect

Before this, weir-ctl contained the string \"quarantine\" exactly once — in a
test comment. The directory holding preserved acked records had no tooling at
all.

`list` mirrors `dl list`. `inspect` is new: it reports how many records verify,
how many are corrupt and at what byte offsets, and whether the reader lost the
framing entirely — the diagnostic needed to decide whether a segment is worth
requeueing, which requeue itself lands next.

A missing quarantine directory is the healthy case and is reported as empty
rather than as an error.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `weir-ctl quarantine requeue`

**Files:**
- Modify: `crates/weir-ctl/src/main.rs`

**Interfaces:**
- Consumes: `QuarantineCommand` (Task 3), `RecoveryReader` (Task 1).
- Produces: nothing.

Mirrors `cmd_dl_requeue`'s shape — dry run by default, `--yes` to apply,
connect once, segment-by-segment — with **two deliberate differences**:

1. It uses `RecoveryReader`, so a corrupt record is skipped rather than making
   the segment skipped wholesale. That inversion is the entire point: every
   quarantined segment is corrupt by definition, so `dl requeue`'s
   skip-the-whole-segment rule would recover nothing.
2. A segment that **desyncs before yielding any record** is not deleted. There
   is nothing to confirm, and deleting it destroys the only forensic copy.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn quarantine_requeue_defaults_to_a_dry_run() {
        let dir = tmp_dir("q_requeue_dry");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        // No socket needed: a dry run must not connect.
        cmd_quarantine_requeue(&dir, Path::new("/nonexistent.sock"), Durability::Batched, false, false)
            .unwrap();
        assert!(q.join(name).exists(), "a dry run must not delete anything");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_reports_the_already_delivered_prefix() {
        // Requeueing re-sends records that already reached the sink when recovery
        // sealed the valid prefix. That is within the at-least-once contract, but
        // the operator is told the count BEFORE they pass --yes.
        let dir = tmp_dir("q_requeue_dupes");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment_with_corruption(
            &q,
            "shard_00__seg_00000001.wab.sealed",
            &[b"pre1", b"pre2", b"BADREC", b"post1"],
            2,
        );
        let plan = quarantine_requeue_plan(&q).unwrap();
        assert_eq!(plan.total_records, 3, "3 verifiable records across the corruption");
        assert_eq!(plan.segments_desynced, 0);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_segment_that_desyncs_with_no_records_is_not_deleted() {
        // Nothing to confirm, and deleting it destroys the only forensic copy.
        let dir = tmp_dir("q_requeue_desync");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_bad_length(&q, name, 0);

        let plan = quarantine_requeue_plan(&q).unwrap();
        assert_eq!(plan.total_records, 0);
        assert_eq!(plan.segments_desynced, 1);
        assert!(q.join(name).exists());
        std::fs::remove_dir_all(dir).ok();
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p weir-ctl quarantine_requeue`
Expected: FAIL — `cannot find function cmd_quarantine_requeue`.

- [ ] **Step 3: Add the subcommand variant**

Extend `QuarantineCommand`:

```rust
    /// Re-submit recoverable records from quarantined segments through the
    /// daemon's socket, then delete each segment once all of them are accepted.
    /// Defaults to a dry run.
    ///
    /// Records that fail verification are SKIPPED, not re-sent — unlike
    /// `dl requeue`, which skips a corrupt segment wholesale. Every quarantined
    /// segment is corrupt by definition, so that rule would recover nothing.
    ///
    /// Re-delivery is at-least-once, and it WILL re-send records that already
    /// reached the sink: recovery delivered the valid prefix when it sealed it,
    /// and the prefix lives in this same file. The dry run prints the count.
    ///
    /// A dedup-capable sink will NOT filter those duplicates. The dedup token is
    /// derived from a batch's contents AND its boundaries, and a requeue
    /// re-batches, so the sink sees genuinely distinct batches and accepts both.
    Requeue {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Daemon Unix socket to push the records back through.
        #[arg(long, visible_alias = "socket-path", default_value = DEFAULT_SOCKET)]
        socket: PathBuf,
        /// Durability tier for the re-pushed records: sync | batched | buffered.
        #[arg(long, default_value = "batched", value_parser = parse_durability)]
        durability: Durability,
        /// Actually requeue. Without this flag, prints what would be requeued.
        #[arg(long)]
        yes: bool,
    },
```

- [ ] **Step 4: Implement the plan and the command**

```rust
/// What a requeue would do, computed without connecting or mutating anything.
struct QuarantineRequeuePlan {
    segments: usize,
    total_records: usize,
    total_skipped: usize,
    segments_desynced: usize,
}

fn quarantine_requeue_plan(q_dir: &Path) -> Result<QuarantineRequeuePlan, String> {
    let mut plan = QuarantineRequeuePlan {
        segments: 0,
        total_records: 0,
        total_skipped: 0,
        segments_desynced: 0,
    };
    for (path, _sz) in quarantine_segments(q_dir)? {
        plan.segments += 1;
        let r = quarantine_inspect_report(&path)?;
        plan.total_records += r.recovered;
        plan.total_skipped += r.skipped;
        if r.desynced {
            plan.segments_desynced += 1;
        }
    }
    Ok(plan)
}
```

`cmd_quarantine_requeue` then:

- computes the plan and, without `--yes`, prints it and returns — **including
  an explicit line that the already-delivered prefix will be re-sent**, so the
  operator decides rather than discovers;
- with `--yes`, connects once, and per segment collects the `Record` items
  first, pushes them all, and deletes the segment **only if** at least one
  record was recovered and every push was accepted;
- leaves a segment in place when it yielded no records, and reports it.

- [ ] **Step 5: Run to verify they pass**

Run: `cargo test -p weir-ctl quarantine`
Expected: PASS — 6 passed (3 from Task 3, 3 here).

- [ ] **Step 6: The end-to-end test that justifies the feature**

Add to `crates/weir-server/tests/system.rs`:

```rust
#[test]
fn quarantined_records_after_a_corruption_can_be_recovered() {
    // The round trip the whole feature exists for. SegmentReader cannot reach
    // the records after the corruption; this asserts RecoveryReader can, end to
    // end through the daemon.
    //
    // 1. Write a segment, corrupt one record's payload mid-file.
    // 2. Start the daemon so recovery quarantines it.
    // 3. `quarantine requeue --yes`.
    // 4. Assert the sink received the records that sat AFTER the corruption.
    //
    // Build on the existing crash/recovery system tests for daemon lifecycle;
    // the corruption itself mirrors
    // `mid_file_corruption_in_a_compressed_segment_quarantines` in
    // crates/weir-server/src/wab/recovery.rs.
}
```

Fill in the body against the existing system-test harness — do not invent a
second one. If the assertion cannot be made through the noop sink, use the
recording HTTP sink pattern the http tests already use.

- [ ] **Step 7: Full gate and commit**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
```

```bash
git add crates/weir-ctl crates/weir-server/tests/system.rs
git commit -m "feat(ctl): weir-ctl quarantine requeue

Recovers the records a quarantined segment exists to preserve — the ones
sitting AFTER the corrupt record, which live nowhere else.

Inverts dl requeue's rule deliberately. dl requeue skips a corrupt segment
wholesale so a corrupt segment is never partially delivered; every quarantined
segment is corrupt by definition, so applying that rule here would recover
nothing. Instead the corrupt RECORD is skipped and the rest are recovered.

A segment that desyncs without yielding any record is left in place: there is
nothing to confirm, and deleting it would destroy the only forensic copy.

The dry run states up front that the already-delivered prefix will be re-sent —
recovery delivered it when it sealed the valid prefix, and the prefix lives in
this same file. This is within the at-least-once contract, and the dedup token
will NOT save you: DedupToken is derived from a batch's contents AND its
boundaries, and a requeue re-batches, so a dedup-capable sink sees genuinely
distinct batches and accepts both. The operator decides rather than discovers.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/monitoring.md`
- Modify: `docs/wab_format.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/operations/` — add a quarantine recovery runbook

**Interfaces:** Consumes everything; produces nothing.

- [ ] **Step 1: Metrics**

Add `weir_quarantine_bytes_on_disk` and `weir_recovery_segments_failed_total`
to the table in `docs/monitoring.md`, beside the existing quarantine counters.
For the counter, state plainly what a non-zero value means: acked records may
be unreachable, check the startup logs for the path and cause.

- [ ] **Step 2: A recovery runbook**

The gap the sweep found was not only tooling but the absence of a procedure.
Add a short runbook covering: how to notice (the gauge and the counter), how to
triage (`quarantine list`, then `inspect` per segment), how to interpret a
desync (records past that point are not recoverable by this tool), and how to
recover (`requeue`, dry run first, and that the prefix is re-sent).

Cross-reference it from `docs/wab_format.md`'s description of the quarantine
directory, which currently explains what the directory is but not what to do
about it.

Document the **naming**, because an operator reading the directory by hand needs
it and because getting it wrong is what this plan's own Task 3 originally did:
entries are `{shard_name}__{original_file_name}`, ending `.wab` when crash
recovery preserved an active segment and `.wab.sealed` when the drain preserved
a sealed one, with a `.N` suffix appended on a name collision.

- [ ] **Step 2b: Correct the stale comment in `main.rs`**

`crates/weir-server/src/main.rs:70-71`, inside `compute_wab_bytes_on_disk`,
claims *"quarantine/ holds forensic .wab.sealed copies"*. It holds both
extensions — see above. The code is correct (it skips the whole directory
either way); only the comment is wrong, and it is the same wrong belief that
would have made `quarantine list` report an empty directory.

- [ ] **Step 3: CHANGELOG**

```markdown
- **Quarantined records are reachable again.** When crash recovery meets
  mid-file corruption it seals and delivers the valid prefix, then copies the
  whole segment to `quarantine/` — precisely because acked records may sit
  *after* the corrupt one. Those records exist nowhere else, and until now
  nothing could read them: `weir-ctl` had no quarantine command at all, and the
  only quarantine metrics were per-process counters that vanished on restart.

  New `weir-ctl quarantine list | inspect | requeue`, built on a new
  `weir-wab::RecoveryReader` that continues past a corrupt record instead of
  stopping at it. It never fabricates records: a declared length above the
  stored cap, or a run of consecutive verification failures, ends the read
  rather than guessing where the next record begins.

  `requeue` **does** re-send records that already reached the sink — the
  delivered prefix and the preserved tail live in the same file — so the dry run
  prints that count before you pass `--yes`. A dedup-capable sink will not filter
  them either: the dedup token covers a batch's contents *and* its boundaries,
  and a requeue re-batches. A segment that yields no recoverable records is left
  in place rather than deleted.

  Also adds `weir_quarantine_bytes_on_disk` (a gauge, so it survives a restart)
  and `weir_recovery_segments_failed_total`, for the recovery arm that
  previously only logged.
```

- [ ] **Step 4: Full gate and commit**

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --exclude weir-server
cargo test -p weir-server --bins -- --test-threads=1
cargo deny check advisories bans licenses sources
```

```bash
git add docs CHANGELOG.md
git commit -m "docs: quarantine recovery runbook and the two new signals

The sweep found not only missing tooling but a missing procedure: the docs
explained what quarantine/ is and never what to do about it. Adds the triage
path (gauge and counter, then list, then inspect per segment, then requeue) and
states what a desync means — records past that point are not recoverable by
this tool, which an operator needs to know before assuming the data is safe.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes

**Spec coverage.** §5.1 (what a quarantined file contains) → the rationale
throughout, and Task 4's inverted skip rule. §5.2 `RecoveryReader` → Task 1.
§5.3 the three subcommands → Tasks 3–4. §5.4 gauge → Task 2. §5.5
recovery-failure counter → Task 2. §7 quarantine tests → Tasks 1, 3, 4.

**Deviation from the spec, deliberate.** §5.2 specifies resync as "skip
`declared_len` bytes, then check the next header is plausible". Reading
`SegmentReader::next` showed the skip is already free — the payload is read
before the CRC is verified, so the reader is positioned correctly after a
failure. The plausible-next-header heuristic is replaced by a
consecutive-failure limit, which achieves the same guarantee (never fabricate
records) without a heuristic that could itself misjudge. Task 1's commit
message records this.

**Corrections found proofreading this plan after Tasks 1–2 shipped.** Recorded
here because each was wrong in a way that would have shipped silently:

- **Task 3's segment filter was `.wab.sealed` only.** Quarantine holds both
  extensions, and the `.wab` half is the crash-recovery half — the mid-file
  corruption case, the one where acked records sit after the corruption, which
  is this plan's entire premise. `list` would have printed "empty" and `requeue`
  recovered nothing, both reporting success. Fixed in Task 3 Step 4, with
  `quarantine_list_finds_both_extensions_and_collision_suffixes` and a mutation
  check to pin it.
- **Task 3's `RecoveryItem` match had no wildcard arm**, and the enum became
  `#[non_exhaustive]` in Task 1's fix round. It would not have compiled.
- **Task 4's commit message claimed duplicates are "deduped by the batch
  token".** They are not: `DedupToken` covers batch boundaries as well as
  contents — `weir-sink-sdk/src/lib.rs` has
  `dedup_token_distinguishes_different_batch_boundaries` to pin exactly that —
  and a requeue re-batches. The `Requeue` help text and the dry-run wording were
  already right; only the reassuring clause was wrong, and it is the clause that
  would talk an operator out of the dry run.
- **The Global Constraints gate omitted clippy and `cargo deny`.** Added.
- **Not a defect, though it reads like one:** the Task 3/4 test snippets pass a
  trailing `bool` that the subcommand variants never declare. That is correct —
  `--json` is a global flag on `Cli` threaded in at dispatch, exactly as `dl`
  does it. Do not "fix" it by adding a `json` field to the variants; that would
  collide with the global.
- **Task 1's body below is the pre-review version** and still contains the
  defects the review found (the unconditional zero-length sentinel, the zstd
  path feeding the consecutive-skip budget, no `#[non_exhaustive]`, no
  `FusedIterator`, a private `MAX_CONSECUTIVE_SKIPS`, and a cascade test the
  implementer proved vacuous). It is left as written because Task 1 is done;
  **code against the "As built" section near the top of this plan instead.**

**Known rough edges for the implementer.**
- Task 2 Step 1's test input must actually reach the `Err(e) =>` arm at
  `recovery.rs:128-130` rather than the quarantine arm. Read `recover_shard_dir`
  and pick accordingly.
- Task 3's `write_q_segment*` helpers overlap Task 1's `write_segment`. Factor
  one if the shapes converge; do not maintain two.
- Task 4 Step 6 is a test **body left to be written against the existing
  harness**. That is deliberate — inventing a second daemon-lifecycle harness
  would be worse than reusing the crash/recovery tests' — but it is the one step
  here that is not copy-paste, and it is the step that proves the feature works.
