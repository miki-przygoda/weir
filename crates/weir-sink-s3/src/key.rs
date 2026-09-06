//! Object key construction.
//!
//! # Why the key is shaped this way
//!
//! weir's drain is at-least-once: after a crash it re-commits a byte-identical
//! batch. A key containing wall-clock would place that replay under a *new*
//! partition path, so the bucket would hold two objects with identical content
//! and every query would count the records twice. The **partition** therefore
//! comes from the owning WAB segment's `created_at`, which is written once into
//! the segment header and read back unchanged.
//!
//! The **filename** is the batch's first [`RecordId`] plus its record count —
//! deliberately *not* its `DedupToken`. A `DedupToken` is a pure content hash,
//! so two batches carrying identical bytes share one. An S3 key is
//! last-write-wins, so a token-named object is **destroyed** by the next batch
//! of the same repetitive records: a heartbeat producer at the default batch
//! size acks 360,000 records in an hour and leaves 100 in the bucket. `RecordId`
//! mixes the record's WAB coordinate into the digest, which is exactly the
//! uniqueness a content hash cannot carry.
//!
//! The count is included because the start coordinate alone still collides when
//! `sink_max_batch_size` changes: a resized batch beginning at the same index
//! would overwrite an object holding different content. With the count present
//! a resize yields a distinct key, degrading that failure to duplication —
//! which the at-least-once contract already absorbs.
//!
//! Rendering is always **UTC**. Local time would make the key depend on the
//! host's `TZ`, reintroducing the same duplication across a DST change.
//!
//! [`RecordId`]: weir_sink_sdk::RecordId

use crate::time::{Utc, utc_from_unix_nanos};

/// Why a partition template was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TemplateError {
    /// A `%X` where `X` is not one of `Y`, `m`, `d`, `H`.
    UnknownSpecifier(char),
    /// The template ended with a bare `%`.
    TrailingPercent,
    /// The template contained a `.` or `..` path segment.
    DotSegment,
    /// A segment was empty (an interior `//`) or contained only whitespace.
    EmptySegment,
    /// A character that a URL parser would strip, reinterpret, or truncate at.
    UnsafeCharacter(char),
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownSpecifier(c) => write!(
                f,
                "unknown partition specifier '%{c}'; only %Y, %m, %d and %H are supported"
            ),
            Self::TrailingPercent => write!(f, "partition template ends with a bare '%'"),
            Self::DotSegment => write!(
                f,
                "'.' and '..' path segments are not allowed: a URL parser removes them, so the \
                 object would be signed under one key and written under another"
            ),
            Self::EmptySegment => write!(
                f,
                "empty or whitespace-only path segment (an interior '//' or '  /'): it becomes a \
                 zero-length key component and breaks Hive partition discovery"
            ),
            Self::UnsafeCharacter(c) => write!(
                f,
                "character {c:?} is not allowed: a URL parser strips or truncates at it, so the \
                 signed key and the written key would differ"
            ),
        }
    }
}

impl std::error::Error for TemplateError {}

/// One piece of a parsed partition template.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Literal(String),
    Year,
    Month,
    Day,
    Hour,
}

/// A validated partition template.
///
/// The syntax is deliberately tiny — `%Y`, `%m`, `%d`, `%H`, everything else
/// literal. It is **not** strftime: a full implementation would admit `%s` and
/// other wall-clock-shaped specifiers that defeat the replay-stability the key
/// scheme exists to provide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionTemplate {
    pieces: Vec<Piece>,
}

/// Rejects path text that a URL parser would rewrite, so that the key weir
/// signs is always the key weir writes.
///
/// Encoding alone cannot cover this. AWS's `UriEncode` leaves `.` unescaped (it
/// is in the unreserved set) and exempts `/`, so `.` and `..` segments survive
/// the encoder untouched — and `Url::parse` then applies RFC 3986
/// `remove_dot_segments` and deletes them. `a/../../b/x` is signed as written
/// and sent as `/b/x`: either a `403 SignatureDoesNotMatch`, or, if the caller
/// signs the URL it parsed, a silent write to the **wrong key**.
///
/// A literal newline or tab is worse still: `Url::parse` removes those outright
/// rather than encoding them, so `a<LF>b` reaches the wire as `ab`.
///
/// Applied to `sink_s3_prefix` and to partition templates alike — an earlier
/// version validated only the template, leaving the prefix (also operator free
/// text) able to escape by exactly the route the template check existed to
/// close.
pub(crate) fn validate_path_text(s: &str) -> Result<(), TemplateError> {
    for seg in s.trim_matches('/').split('/') {
        if seg.is_empty() || seg.trim().is_empty() {
            // A bare "" from an empty input is fine; an interior // is not.
            if s.trim_matches('/').is_empty() {
                continue;
            }
            return Err(TemplateError::EmptySegment);
        }
        if seg == "." || seg == ".." {
            return Err(TemplateError::DotSegment);
        }
    }
    if let Some(c) = s
        .chars()
        .find(|c| c.is_control() || matches!(c, '#' | '?' | '\\'))
    {
        return Err(TemplateError::UnsafeCharacter(c));
    }
    Ok(())
}

impl PartitionTemplate {
    /// Parses a template, rejecting unknown specifiers rather than passing
    /// them through.
    pub(crate) fn parse(s: &str) -> Result<Self, TemplateError> {
        validate_path_text(s)?;
        let mut pieces = Vec::new();
        let mut literal = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c != '%' {
                literal.push(c);
                continue;
            }
            let Some(spec) = chars.next() else {
                return Err(TemplateError::TrailingPercent);
            };
            let piece = match spec {
                'Y' => Piece::Year,
                'm' => Piece::Month,
                'd' => Piece::Day,
                'H' => Piece::Hour,
                other => return Err(TemplateError::UnknownSpecifier(other)),
            };
            if !literal.is_empty() {
                pieces.push(Piece::Literal(std::mem::take(&mut literal)));
            }
            pieces.push(piece);
        }
        if !literal.is_empty() {
            pieces.push(Piece::Literal(literal));
        }
        Ok(Self { pieces })
    }

    /// Renders the template for an instant. Always UTC.
    fn render(&self, t: Utc) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        for piece in &self.pieces {
            match piece {
                Piece::Literal(s) => out.push_str(s),
                Piece::Year => {
                    let _ = write!(out, "{:04}", t.year);
                }
                Piece::Month => {
                    let _ = write!(out, "{:02}", t.month);
                }
                Piece::Day => {
                    let _ = write!(out, "{:02}", t.day);
                }
                Piece::Hour => {
                    let _ = write!(out, "{:02}", t.hour);
                }
            }
        }
        out
    }

    /// True when the template renders nothing (partitioning disabled).
    fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }
}

/// The filename stem for a batch: its first record's WAB coordinate plus the
/// record count.
///
/// **Not the `DedupToken`** — see the module docs. A content hash is identical
/// for two batches carrying identical bytes, and an S3 key is last-write-wins,
/// so a token-named object is destroyed by the next batch of the same
/// repetitive records.
/// # Injectivity
///
/// `count` renders as decimal digits, which contain no `-`, so the **last** `-`
/// in the result is always the separator and the decomposition is unique for
/// any `hex` — including one of varying length or containing `-`. That, not the
/// fixed 64-char width of a `RecordId`, is what makes the name collision-free.
pub(crate) fn batch_name(first_record_id_hex: &str, count: usize) -> String {
    format!("{first_record_id_hex}-{count}")
}

/// Builds the full object key.
///
/// `created_at` is the owning segment's creation time in unix nanoseconds;
/// `name` comes from [`batch_name`]; `ext` is the framing plus compression
/// suffix (e.g. `ndjson.zst`), without a leading dot.
pub(crate) fn object_key(
    prefix: &str,
    tmpl: &PartitionTemplate,
    created_at: i64,
    name: &str,
    ext: &str,
) -> String {
    let mut parts: Vec<&str> = Vec::new();
    let prefix = prefix.trim_matches('/');
    if !prefix.is_empty() {
        parts.push(prefix);
    }
    let rendered;
    if !tmpl.is_empty() {
        rendered = tmpl.render(utc_from_unix_nanos(created_at));
        let trimmed = rendered.trim_matches('/');
        if !trimmed.is_empty() {
            parts.push(trimmed);
        }
    }
    let leaf = format!("{name}.{ext}");
    parts.push(&leaf);
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    // A RecordId hex, NOT a DedupToken. See the module docs for why that
    // distinction is the difference between duplication and data loss.
    const RID: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn tmpl(s: &str) -> PartitionTemplate {
        PartitionTemplate::parse(s).expect("template should parse")
    }

    // 2026-09-06T14:25:30Z.
    const T: i64 = 1_788_704_730 * 1_000_000_000;

    #[test]
    fn the_default_template_renders_hive_style_utc_partitions() {
        let key = object_key(
            "archive",
            &tmpl("dt=%Y-%m-%d/hour=%H"),
            T,
            &batch_name(RID, 100),
            "ndjson.zst",
        );
        assert_eq!(
            key,
            format!("archive/dt=2026-09-06/hour=14/{RID}-100.ndjson.zst")
        );
    }

    #[test]
    fn the_same_inputs_always_produce_the_same_key() {
        // Replay stability. The drain is at-least-once: after a crash it
        // re-commits a byte-identical batch from the same segment coordinate.
        let t = tmpl("dt=%Y-%m-%d/hour=%H");
        let n = batch_name(RID, 100);
        assert_eq!(
            object_key("p", &t, T, &n, "ndjson"),
            object_key("p", &t, T, &n, "ndjson")
        );
    }

    #[test]
    fn distinct_batches_of_identical_records_get_distinct_keys() {
        // THE collision guard. Two batches of byte-identical records share a
        // DedupToken; if that token were the filename they would share a key,
        // and PutObject is last-write-wins, so the second batch would DESTROY
        // the first. RecordId mixes the WAB coordinate in, so they differ here
        // even though their bytes do not.
        let t = tmpl("dt=%Y-%m-%d/hour=%H");
        let a = batch_name(&"1".repeat(64), 100);
        let b = batch_name(&"2".repeat(64), 100);
        assert_ne!(
            object_key("p", &t, T, &a, "ndjson"),
            object_key("p", &t, T, &b, "ndjson"),
            "distinct batches must not share an object key"
        );
    }

    #[test]
    fn a_changed_batch_size_yields_a_distinct_key() {
        // sink_max_batch_size must be constant. If it changes, a resized batch
        // starting at the same coordinate must NOT overwrite the old object
        // with different content — the count makes it a new key, degrading the
        // failure to duplication, which at-least-once absorbs.
        let t = tmpl("dt=%Y-%m-%d/hour=%H");
        assert_ne!(
            object_key("p", &t, T, &batch_name(RID, 100), "ndjson"),
            object_key("p", &t, T, &batch_name(RID, 50), "ndjson")
        );
    }

    #[test]
    fn the_partition_is_rendered_in_utc_not_local_time() {
        // This instant is 2026-09-06 in UTC but ALREADY 2026-09-07 in
        // Pacific/Auckland (UTC+12), so a local-time render would produce a
        // different date. A real assertion — not a TZ-mutation test, which
        // would prove nothing against a pure function that never reads TZ and
        // would race sibling tests that read AWS_* from the environment.
        let key = object_key("", &tmpl("dt=%Y-%m-%d"), T, &batch_name(RID, 1), "ndjson");
        assert!(key.starts_with("dt=2026-09-06/"), "must be UTC, got {key}");
    }

    #[test]
    fn an_empty_prefix_produces_no_leading_slash() {
        let key = object_key("", &tmpl("dt=%Y-%m-%d"), T, &batch_name(RID, 1), "ndjson");
        assert_eq!(key, format!("dt=2026-09-06/{RID}-1.ndjson"));
        assert!(!key.starts_with('/'), "S3 keys must not start with '/'");
    }

    #[test]
    fn an_empty_template_disables_partitioning() {
        assert_eq!(
            object_key("p", &tmpl(""), 0, &batch_name(RID, 7), "ndjson"),
            format!("p/{RID}-7.ndjson")
        );
    }

    #[test]
    fn a_trailing_slash_on_the_prefix_does_not_double_up() {
        assert_eq!(
            object_key("p/", &tmpl(""), 0, &batch_name(RID, 7), "ndjson"),
            format!("p/{RID}-7.ndjson")
        );
    }

    #[test]
    fn month_day_and_hour_are_zero_padded() {
        // 2026-01-02T03:04:05Z — single digits everywhere.
        let key = object_key(
            "",
            &tmpl("%Y/%m/%d/%H"),
            1_767_323_045 * 1_000_000_000,
            &batch_name(RID, 1),
            "x",
        );
        assert!(key.starts_with("2026/01/02/03/"), "got {key}");
    }

    #[test]
    fn an_unknown_specifier_is_rejected_at_parse_time() {
        // %s (unix seconds) would reintroduce wall-clock instability, and a
        // silent passthrough would ship a broken key scheme. Reject loudly.
        assert!(matches!(
            PartitionTemplate::parse("dt=%Y/%s").unwrap_err(),
            TemplateError::UnknownSpecifier('s')
        ));
    }

    #[test]
    fn a_trailing_percent_is_rejected() {
        assert!(matches!(
            PartitionTemplate::parse("dt=%Y/%").unwrap_err(),
            TemplateError::TrailingPercent
        ));
    }

    #[test]
    fn dot_segments_are_rejected_in_templates() {
        // Encoding cannot save these: AWS's unreserved set includes '.' and
        // exempts '/', so both survive uri_encode_path untouched and Url::parse
        // then removes them -- the object is signed under one key and written
        // under another.
        for t in ["../etc", "a/../b", "a/./b", "."] {
            assert!(
                matches!(
                    PartitionTemplate::parse(t).unwrap_err(),
                    TemplateError::DotSegment
                ),
                "template {t:?} must be rejected"
            );
        }
    }

    #[test]
    fn the_prefix_is_validated_by_the_same_rule_as_the_template() {
        // The gap an earlier version left: only the template was checked, while
        // sink_s3_prefix -- equally operator-supplied free text -- could escape
        // by exactly the route the template check existed to close.
        assert!(matches!(
            validate_path_text("a/../../b").unwrap_err(),
            TemplateError::DotSegment
        ));
        assert!(validate_path_text("archive/raw").is_ok());
        assert!(validate_path_text("").is_ok());
    }

    #[test]
    fn interior_empty_and_whitespace_only_segments_are_rejected() {
        // An interior '//' is a zero-length key component: stable, but it
        // breaks Hive partition discovery and confuses `aws s3 sync`.
        assert!(matches!(
            validate_path_text("a//b").unwrap_err(),
            TemplateError::EmptySegment
        ));
        assert!(matches!(
            validate_path_text("   ").unwrap_err(),
            TemplateError::EmptySegment
        ));
        // Leading and trailing slashes are trimmed, not an error.
        assert!(validate_path_text("/a/b/").is_ok());
    }

    #[test]
    fn characters_a_url_parser_would_rewrite_are_rejected() {
        // '#' and '?' truncate the path; control characters are deleted.
        for bad in ["arch#ive", "dt=?x", "a\nb", "a\tb", "a\\b"] {
            assert!(
                matches!(
                    validate_path_text(bad).unwrap_err(),
                    TemplateError::UnsafeCharacter(_)
                ),
                "{bad:?} must be rejected"
            );
        }
        // Spaces and non-ASCII are legal: uri_encode_path handles them.
        assert!(validate_path_text("my archive/caf\u{e9}").is_ok());
    }

    #[test]
    fn the_batch_name_separator_is_load_bearing() {
        // Collision freedom rests on the LAST '-' being the separator, which
        // holds because a decimal count contains no '-'. Without the separator
        // these two distinct batches would produce the same name.
        assert_ne!(batch_name("a1", 23), batch_name("a12", 3));
        assert_eq!(batch_name("a1", 23), "a1-23");
    }

    #[test]
    fn prefix_and_template_slashes_are_trimmed_on_both_ends() {
        let k = object_key("/p/", &tmpl("/dt=%Y/"), T, &batch_name(RID, 1), "x");
        assert_eq!(k, format!("p/dt=2026/{RID}-1.x"));
    }
}
