//! Read-only facts from a `wab_dir`, for `weir-ctl status` when no daemon
//! answers, or to fill in what an old daemon's /metrics lacks (spec §14).
//!
//! Never takes, probes or releases `.weir.lock`, never calls
//! `check_confirmed`, and never writes: no sidecar, no rename, no unlink. A
//! sealed segment the live drain deletes between the listing and the open was
//! delivered, so `NotFound` skips it silently.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use weir_wab::format::{
    CONFIRMED_LEN, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, confirmed_path_for, parse_confirmed,
    parse_segment_footer, parse_segment_header,
};
use weir_wab::{SegmentState, list_segment_files};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ShardDisk {
    pub name: String,
    /// Parsed from `shard_NN`.
    pub id: Option<u16>,
    /// Sealed segments with no `.confirmed` sidecar.
    pub undelivered_segments: u64,
    /// Sum of their footers' `record_count`.
    pub undelivered_records: u64,
    /// Undelivered segments whose header or footer could not be read.
    pub unreadable_segments: u64,
    pub oldest_undelivered_created_ns: Option<i64>,
    pub active_segments: u64,
    pub oldest_active_created_ns: Option<i64>,
    /// `drained_at` of the newest `.confirmed` (by name: counters are zero-padded).
    pub last_delivery_ns: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DiskFacts {
    pub wab_dir: PathBuf,
    pub shards: Vec<ShardDisk>,
    pub dead_letter_files: u64,
    pub dead_letter_bytes: u64,
    pub quarantine_files: u64,
    pub quarantine_bytes: u64,
    pub scanned_at_ms: u64,
}

impl DiskFacts {
    pub fn undelivered_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.undelivered_segments).sum()
    }
    pub fn undelivered_records(&self) -> u64 {
        self.shards.iter().map(|s| s.undelivered_records).sum()
    }
    pub fn unreadable_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.unreadable_segments).sum()
    }
    pub fn active_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.active_segments).sum()
    }
    pub fn last_delivery_ns(&self) -> Option<i64> {
        self.shards.iter().filter_map(|s| s.last_delivery_ns).max()
    }
    /// An upper bound, like the daemon's (spec §9.1): header `created_at` of
    /// the oldest undelivered segment.
    pub fn oldest_undelivered_age_secs(&self, now_ms: u64) -> Option<f64> {
        let oldest = self
            .shards
            .iter()
            .filter_map(|s| s.oldest_undelivered_created_ns)
            .min()?;
        let now_ns = (now_ms as i128) * 1_000_000;
        Some((((now_ns - oldest as i128).max(0)) as f64) / 1e9)
    }
}

pub(crate) fn scan_disk(wab_dir: &Path, now_ms: u64) -> Result<DiskFacts, String> {
    let entries =
        std::fs::read_dir(wab_dir).map_err(|e| format!("read {}: {e}", wab_dir.display()))?;
    let mut shards = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        if !path.is_dir() || name.starts_with('.') || !crate::is_shard_dir_name(&name) {
            continue;
        }
        shards.push(scan_shard(&path, name));
    }
    shards.sort_by(|a, b| a.name.cmp(&b.name));

    let dl = crate::dl_segments(&crate::dead_letter_dir(wab_dir))?;
    let q = crate::quarantine_segments(&crate::quarantine_dir(wab_dir))?;
    Ok(DiskFacts {
        wab_dir: wab_dir.to_path_buf(),
        shards,
        dead_letter_files: dl.len() as u64,
        dead_letter_bytes: dl.iter().map(|(_, s)| *s).sum(),
        quarantine_files: q.len() as u64,
        quarantine_bytes: q.iter().map(|(_, s)| *s).sum(),
        scanned_at_ms: now_ms,
    })
}

fn scan_shard(dir: &Path, name: String) -> ShardDisk {
    let mut s = ShardDisk {
        id: name.strip_prefix("shard_").and_then(|n| n.parse().ok()),
        name,
        ..ShardDisk::default()
    };
    let Ok(files) = list_segment_files(dir) else {
        return s;
    };
    let mut newest_confirmed: Option<PathBuf> = None;
    for (path, state) in files {
        match state {
            SegmentState::Confirmed => newest_confirmed = Some(path), // sorted by path
            SegmentState::Sealed => {
                if confirmed_path_for(&path).exists() {
                    continue; // delivered; only the delete is pending
                }
                match read_sealed_meta(&path) {
                    Ok(Some((created, records))) => {
                        s.undelivered_segments += 1;
                        s.undelivered_records += records;
                        s.oldest_undelivered_created_ns = Some(
                            s.oldest_undelivered_created_ns
                                .map_or(created, |o| o.min(created)),
                        );
                    }
                    Ok(None) => {
                        s.undelivered_segments += 1;
                        s.unreadable_segments += 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => {
                        s.undelivered_segments += 1;
                        s.unreadable_segments += 1;
                    }
                }
            }
            SegmentState::Active => match read_header_created(&path) {
                Ok(created) => {
                    s.active_segments += 1;
                    if let Some(c) = created {
                        s.oldest_active_created_ns =
                            Some(s.oldest_active_created_ns.map_or(c, |o| o.min(c)));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => s.active_segments += 1,
            },
        }
    }
    if let Some(p) = newest_confirmed {
        let mut buf = [0u8; CONFIRMED_LEN];
        if File::open(&p)
            .and_then(|mut f| f.read_exact(&mut buf))
            .is_ok()
        {
            s.last_delivery_ns = parse_confirmed(&buf).ok().map(|m| m.drained_at);
        }
    }
    s
}

/// `(created_at, record_count)`, or `None` if the header or footer does not
/// parse or is implausible (footer `data_bytes` larger than the file).
fn read_sealed_meta(path: &Path) -> io::Result<Option<(i64, u64)>> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    if len < (SEGMENT_HEADER_LEN + SEGMENT_FOOTER_LEN) as u64 {
        return Ok(None);
    }
    let mut head = [0u8; SEGMENT_HEADER_LEN];
    f.read_exact(&mut head)?;
    let Ok(h) = parse_segment_header(&head) else {
        return Ok(None);
    };
    f.seek(SeekFrom::Start(len - SEGMENT_FOOTER_LEN as u64))?;
    let mut foot = [0u8; SEGMENT_FOOTER_LEN];
    f.read_exact(&mut foot)?;
    match parse_segment_footer(&foot) {
        Ok(m) if m.data_bytes <= len => Ok(Some((h.created_at, m.record_count))),
        _ => Ok(None),
    }
}

fn read_header_created(path: &Path) -> io::Result<Option<i64>> {
    let mut f = File::open(path)?;
    let mut head = [0u8; SEGMENT_HEADER_LEN];
    match f.read_exact(&mut head) {
        Ok(()) => Ok(parse_segment_header(&head).ok().map(|h| h.created_at)),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None), // just created
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weir_wab::format::{
        Compression, build_confirmed, build_segment_footer, build_segment_header,
    };

    const NOW_MS: u64 = 1_700_000_100_000;
    const T0_NS: i64 = 1_700_000_000_000_000_000; // 100 s before NOW_MS

    fn tmp(label: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("weir_ctl_offline_{label}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Header (with a chosen created_at) + sentinel + footer. No record bodies:
    /// the scan reads only the header and the footer.
    fn segment_bytes(created_ns: i64, records: u64) -> Vec<u8> {
        let mut header = build_segment_header(0, Compression::None);
        header[8..16].copy_from_slice(&created_ns.to_le_bytes());
        let mut b = header.to_vec();
        b.extend_from_slice(&[0u8; 4]);
        b.extend_from_slice(&build_segment_footer(records, 0, 0, created_ns + 1));
        b
    }

    fn tree(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                out.push(p.strip_prefix(dir).unwrap().display().to_string());
                if p.is_dir() {
                    stack.push(p);
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn counts_undelivered_records_ages_and_last_delivery() {
        let wab = tmp("counts");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        // Delivered: sealed with its sidecar (delete pending), and an older sidecar alone.
        std::fs::write(
            s0.join("seg_00000001.wab.confirmed"),
            build_confirmed(T0_NS, 4, T0_NS + 10_000_000_000),
        )
        .unwrap();
        std::fs::write(s0.join("seg_00000002.wab.sealed"), segment_bytes(T0_NS, 9)).unwrap();
        std::fs::write(
            s0.join("seg_00000002.wab.confirmed"),
            build_confirmed(T0_NS, 9, T0_NS + 20_000_000_000),
        )
        .unwrap();
        // Undelivered: two sealed without sidecars.
        std::fs::write(
            s0.join("seg_00000003.wab.sealed"),
            segment_bytes(T0_NS + 5_000_000_000, 7),
        )
        .unwrap();
        std::fs::write(
            s0.join("seg_00000004.wab.sealed"),
            segment_bytes(T0_NS + 30_000_000_000, 11),
        )
        .unwrap();
        // Active.
        std::fs::write(
            s0.join("seg_00000005.wab"),
            &segment_bytes(T0_NS + 40_000_000_000, 0)[..24],
        )
        .unwrap();

        let f = scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(f.shards.len(), 1);
        let s = &f.shards[0];
        assert_eq!((s.name.as_str(), s.id), ("shard_00", Some(0)));
        assert_eq!(s.undelivered_segments, 2);
        assert_eq!(s.undelivered_records, 18);
        assert_eq!(s.oldest_undelivered_created_ns, Some(T0_NS + 5_000_000_000));
        assert_eq!(s.active_segments, 1);
        assert_eq!(s.oldest_active_created_ns, Some(T0_NS + 40_000_000_000));
        assert_eq!(
            s.last_delivery_ns,
            Some(T0_NS + 20_000_000_000),
            "newest sidecar by name"
        );
        let age = f.oldest_undelivered_age_secs(NOW_MS).unwrap();
        assert!((age - 95.0).abs() < 0.001, "{age}");
        assert_eq!(f.scanned_at_ms, NOW_MS);
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn dead_letter_and_quarantine_are_rolled_up_not_scanned_as_shards() {
        let wab = tmp("rollup");
        std::fs::create_dir_all(wab.join("shard_00")).unwrap();
        let dl = wab.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        std::fs::write(dl.join("dl_00000001.wab.sealed"), vec![0u8; 50]).unwrap();
        let q = wab.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        std::fs::write(q.join("shard_00__seg_00000009.wab.sealed"), vec![0u8; 70]).unwrap();
        std::fs::write(wab.join(".weir.lock"), b"pid=1\n").unwrap();

        let f = scan_disk(&wab, NOW_MS).unwrap();
        let names: Vec<&str> = f.shards.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["shard_00"]);
        assert_eq!((f.dead_letter_files, f.dead_letter_bytes), (1, 50));
        assert_eq!((f.quarantine_files, f.quarantine_bytes), (1, 70));
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn a_short_sealed_file_is_undelivered_and_unreadable() {
        let wab = tmp("short");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        std::fs::write(s0.join("seg_00000001.wab.sealed"), b"WEIR").unwrap();
        let f = scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(f.undelivered_segments(), 1);
        assert_eq!(f.undelivered_records(), 0);
        assert_eq!(f.unreadable_segments(), 1);
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn a_missing_wab_dir_is_an_error() {
        let missing =
            std::env::temp_dir().join(format!("weir_ctl_offline_missing_{}_x", std::process::id()));
        assert!(scan_disk(&missing, NOW_MS).unwrap_err().contains("read "));
    }

    #[test]
    fn the_scan_writes_nothing_and_leaves_the_lock_alone() {
        let wab = tmp("readonly");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        std::fs::write(s0.join("seg_00000001.wab.sealed"), segment_bytes(T0_NS, 3)).unwrap();
        std::fs::write(wab.join(".weir.lock"), b"pid=1\nhost=h\n").unwrap();
        let before = tree(&wab);
        scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(tree(&wab), before, "no sidecar, rename or unlink");
        assert_eq!(
            std::fs::read(wab.join(".weir.lock")).unwrap(),
            b"pid=1\nhost=h\n"
        );
        std::fs::remove_dir_all(&wab).ok();
    }
}
