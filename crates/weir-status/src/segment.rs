//! A sealed or active segment's identity, `shard_NN/seg_NNNNNNNN`, as the
//! drain's `segment_identity` names it (weir-server `wab/segment.rs`) minus the
//! extension. A structured type, so a public document can carry a segment
//! without carrying a free-form path.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId {
    pub shard: u16,
    pub counter: u64,
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard_{:02}/seg_{:08}", self.shard, self.counter)
    }
}

/// Parses a non-empty run of ASCII digits (no sign, unlike `str::parse`).
fn digits<T: std::str::FromStr>(s: &str) -> Option<T> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

impl SegmentId {
    /// Parses `shard_<n>/seg_<digits>` with an optional `.wab` or
    /// `.wab.sealed` suffix. Anything else (dead-letter files, sidecars,
    /// temp files, free text) is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        let (shard, seg) = s.split_once('/')?;
        let shard: u16 = digits(shard.strip_prefix("shard_")?)?;
        let seg = seg.strip_prefix("seg_")?;
        let end = seg.find(|c: char| !c.is_ascii_digit()).unwrap_or(seg.len());
        let rest = &seg[end..];
        if !(rest.is_empty() || rest == ".wab" || rest == ".wab.sealed") {
            return None;
        }
        Some(Self {
            shard,
            counter: digits(&seg[..end])?,
        })
    }

    /// The identity of a segment file `<..>/shard_NN/seg_NNNNNNNN[.wab[.sealed]]`.
    pub fn from_path(path: &Path) -> Option<Self> {
        let file = path.file_name()?.to_str()?;
        let shard = path.parent()?.file_name()?.to_str()?;
        Self::parse(&format!("{shard}/{file}"))
    }
}

impl Serialize for SegmentId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SegmentId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("not a segment id: {s:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn segment_id_renders_and_parses_the_wab_layout() {
        let id = SegmentId {
            shard: 3,
            counter: 42,
        };
        assert_eq!(id.to_string(), "shard_03/seg_00000042");
        for s in [
            "shard_03/seg_00000042",
            "shard_03/seg_00000042.wab",
            "shard_03/seg_00000042.wab.sealed",
        ] {
            assert_eq!(SegmentId::parse(s), Some(id), "{s}");
        }
        assert_eq!(
            SegmentId::from_path(Path::new(
                "/var/lib/weir/wab/shard_03/seg_00000042.wab.sealed"
            )),
            Some(id)
        );
        assert_eq!(SegmentId::parse("shard_255/seg_1").unwrap().shard, 255);
    }

    #[test]
    fn anything_else_is_not_a_segment_id() {
        for s in [
            "",
            "seg_00000001",
            "shard_00/",
            "shard_00/seg_",
            "shard_xx/seg_00000001",
            "shard_+3/seg_00000001",
            "dead_letter/dl_00000001.wab.sealed",
            "shard_00/seg_00000001.wab.confirmed",
            "shard_00/seg_00000001.wab.confirmed.tmp",
            "shard_00/seg_00000001 extra",
        ] {
            assert_eq!(SegmentId::parse(s), None, "{s:?}");
        }
    }

    #[test]
    fn segment_id_serialises_as_its_display_string() {
        let id = SegmentId {
            shard: 0,
            counter: 7,
        };
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"shard_00/seg_00000007\"");
        assert_eq!(serde_json::from_str::<SegmentId>(&json).unwrap(), id);
        assert!(serde_json::from_str::<SegmentId>("\"not a segment\"").is_err());
    }
}
