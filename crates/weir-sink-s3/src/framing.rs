//! Turning a commit batch into an object body.
//!
//! # Why NDJSON is the default
//!
//! The point of an object-store archive is that Athena, DuckDB, Spark and Glue
//! can read the bucket directly. Newline-delimited records are what those
//! engines expect; a weir-specific container would forfeit the main reason to
//! write to object storage at all.
//!
//! A record containing a newline cannot be represented in that framing. It is
//! dead-lettered rather than written, exactly as the HTTP sink's NDJSON mode
//! already does — writing it anyway would split one record into two lines
//! downstream, silently corrupting the shape of the data. Operators with such
//! payloads use [`Framing::LengthPrefixed`].

use weir_core::Payload;

/// How records are laid out inside one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Framing {
    /// Newline-delimited records — readable by every common query engine.
    /// Records containing `\n` or `\r` are dead-lettered.
    #[default]
    Ndjson,
    /// `u64` little-endian length prefix per record. Binary-safe; needs a
    /// weir-aware reader. File extension `weirbin`.
    LengthPrefixed,
}

impl Framing {
    /// Parses the `sink_s3_framing` config value.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ndjson" => Some(Self::Ndjson),
            "length-prefixed" => Some(Self::LengthPrefixed),
            _ => None,
        }
    }
}

/// Object body compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Store uncompressed.
    None,
    /// zstd. Already in weir's tree for WAB format v2; read by Athena and Spark.
    #[default]
    Zstd,
    /// gzip, for engines that do not read zstd.
    Gzip,
}

impl Compression {
    /// Parses the `sink_s3_compression` config value.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "zstd" => Some(Self::Zstd),
            "gzip" => Some(Self::Gzip),
            _ => None,
        }
    }
}

/// A framed batch: the bytes to upload, plus the partition of the input.
pub(crate) struct Framed {
    pub(crate) body: Vec<u8>,
    pub(crate) committed: Vec<Payload>,
    pub(crate) dead_lettered: Vec<(Payload, String)>,
}

/// The dead-letter reason for a record NDJSON cannot represent. A constant so
/// the test and the operator-facing string cannot drift.
const NEWLINE_REASON: &str = "record contains a newline; NDJSON framing cannot represent it \
     (set sink_s3_framing = \"length-prefixed\" for binary payloads)";

/// Frames and compresses a batch.
pub(crate) fn frame(records: Vec<Payload>, framing: Framing, compression: Compression) -> Framed {
    let mut raw = Vec::new();
    let mut committed = Vec::new();
    let mut dead_lettered = Vec::new();

    for record in records {
        match framing {
            Framing::Ndjson => {
                if record.as_ref().iter().any(|b| *b == b'\n' || *b == b'\r') {
                    dead_lettered.push((record, NEWLINE_REASON.to_string()));
                    continue;
                }
                raw.extend_from_slice(record.as_ref());
                raw.push(b'\n');
            }
            Framing::LengthPrefixed => {
                raw.extend_from_slice(&(record.as_ref().len() as u64).to_le_bytes());
                raw.extend_from_slice(record.as_ref());
            }
        }
        committed.push(record);
    }

    // Every record was dead-lettered. Return an empty body so the caller skips
    // the upload entirely — an empty object is pure noise in the bucket, and a
    // zero-byte NDJSON file confuses every query engine that reads it.
    let body = if raw.is_empty() {
        Vec::new()
    } else {
        compress(&raw, compression)
    };

    Framed {
        body,
        committed,
        dead_lettered,
    }
}

fn compress(raw: &[u8], compression: Compression) -> Vec<u8> {
    match compression {
        Compression::None => raw.to_vec(),
        // Level 3 is zstd's default: the fsync-adjacent tradeoff weir already
        // made for WAB format v2, and comfortably faster than the network hop
        // it is feeding.
        Compression::Zstd => {
            zstd::encode_all(raw, 3).expect("zstd encoding of an in-memory buffer cannot fail")
        }
        Compression::Gzip => {
            use std::io::Write as _;
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(raw)
                .expect("gzip encoding into a Vec cannot fail");
            e.finish().expect("gzip finish into a Vec cannot fail")
        }
    }
}

/// File extension for a framing/compression pair, without a leading dot.
pub(crate) fn extension(framing: Framing, compression: Compression) -> String {
    let base = match framing {
        Framing::Ndjson => "ndjson",
        // Deliberately not `bin`: the framing is weir-specific and the name
        // should say so rather than imply a format a reader might guess wrong.
        Framing::LengthPrefixed => "weirbin",
    };
    match compression {
        Compression::None => base.to_string(),
        Compression::Zstd => format!("{base}.zst"),
        Compression::Gzip => format!("{base}.gz"),
    }
}

/// HTTP `Content-Type` for a framing.
pub(crate) fn content_type(framing: Framing) -> &'static str {
    match framing {
        Framing::Ndjson => "application/x-ndjson",
        Framing::LengthPrefixed => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(b: &[u8]) -> Payload {
        Payload::from(b)
    }

    #[test]
    fn ndjson_joins_records_with_newlines_and_ends_with_one() {
        let f = frame(
            vec![p(b"one"), p(b"two")],
            Framing::Ndjson,
            Compression::None,
        );
        assert_eq!(f.body, b"one\ntwo\n");
        assert_eq!(f.committed.len(), 2);
        assert!(f.dead_lettered.is_empty());
    }

    #[test]
    fn a_newline_bearing_record_is_dead_lettered_not_silently_split() {
        // Framing it anyway would turn one record into two lines downstream —
        // a silent corruption of the data's shape. sink/http.rs makes the same
        // call for the same reason.
        let f = frame(
            vec![p(b"ok"), p(b"has\nnewline")],
            Framing::Ndjson,
            Compression::None,
        );
        assert_eq!(f.body, b"ok\n");
        assert_eq!(f.committed.len(), 1);
        assert_eq!(f.dead_lettered.len(), 1);
        assert!(
            f.dead_lettered[0].1.contains("newline"),
            "{}",
            f.dead_lettered[0].1
        );
    }

    #[test]
    fn a_carriage_return_is_also_rejected() {
        // A lone \r is a line terminator to several readers, so it splits the
        // record just as \n does.
        let f = frame(vec![p(b"has\rcr")], Framing::Ndjson, Compression::None);
        assert_eq!(f.dead_lettered.len(), 1);
        assert!(f.committed.is_empty());
    }

    #[test]
    fn empty_records_survive_ndjson_framing() {
        let f = frame(vec![p(b""), p(b"x")], Framing::Ndjson, Compression::None);
        assert_eq!(f.body, b"\nx\n");
        assert_eq!(f.committed.len(), 2);
    }

    #[test]
    fn length_prefixed_framing_is_binary_safe() {
        // The escape hatch for payloads NDJSON cannot represent.
        let f = frame(
            vec![p(b"has\nnewline")],
            Framing::LengthPrefixed,
            Compression::None,
        );
        assert!(
            f.dead_lettered.is_empty(),
            "binary framing must never dead-letter on content"
        );
        let mut expected = 11u64.to_le_bytes().to_vec();
        expected.extend_from_slice(b"has\nnewline");
        assert_eq!(f.body, expected);
    }

    #[test]
    fn zstd_compression_round_trips() {
        let f = frame(
            vec![p(b"one"), p(b"two")],
            Framing::Ndjson,
            Compression::Zstd,
        );
        let out = zstd::decode_all(&f.body[..]).expect("body should be valid zstd");
        assert_eq!(out, b"one\ntwo\n");
    }

    #[test]
    fn gzip_compression_round_trips() {
        use std::io::Read as _;
        let f = frame(
            vec![p(b"one"), p(b"two")],
            Framing::Ndjson,
            Compression::Gzip,
        );
        let mut d = flate2::read::GzDecoder::new(&f.body[..]);
        let mut out = Vec::new();
        d.read_to_end(&mut out).expect("body should be valid gzip");
        assert_eq!(out, b"one\ntwo\n");
    }

    #[test]
    fn extensions_name_both_the_framing_and_the_codec() {
        assert_eq!(extension(Framing::Ndjson, Compression::None), "ndjson");
        assert_eq!(extension(Framing::Ndjson, Compression::Zstd), "ndjson.zst");
        assert_eq!(extension(Framing::Ndjson, Compression::Gzip), "ndjson.gz");
        assert_eq!(
            extension(Framing::LengthPrefixed, Compression::Zstd),
            "weirbin.zst"
        );
    }

    #[test]
    fn an_all_dead_lettered_batch_produces_an_empty_body() {
        // The caller must not upload an empty object; S3Sink asserts it doesn't.
        // Note this must hold for a COMPRESSED framing too: zstd of an empty
        // input is a non-empty frame header, which would upload as a valid but
        // meaningless object.
        for c in [Compression::None, Compression::Zstd, Compression::Gzip] {
            let f = frame(vec![p(b"a\nb")], Framing::Ndjson, c);
            assert!(f.committed.is_empty());
            assert!(
                f.body.is_empty(),
                "compression {c:?} produced a non-empty body"
            );
        }
    }

    #[test]
    fn config_values_parse_and_reject_unknowns() {
        assert_eq!(Framing::parse("ndjson"), Some(Framing::Ndjson));
        assert_eq!(
            Framing::parse("length-prefixed"),
            Some(Framing::LengthPrefixed)
        );
        assert_eq!(Framing::parse("ndjson "), None);
        assert_eq!(Compression::parse("zstd"), Some(Compression::Zstd));
        assert_eq!(Compression::parse("snappy"), None);
    }
}
