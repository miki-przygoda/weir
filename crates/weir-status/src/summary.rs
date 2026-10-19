//! Public summary templates (spec §9.1 redaction rules). Every parameter is a
//! number, an enum, a code, shard ids or a segment id: a template cannot
//! accept free text, so no free text can reach a public `summary`.
//! `summary_templates_take_no_free_text` enforces this on the source itself.
//! Keep each signature on ONE line; the test parses them.

#[cfg(test)]
mod tests {
    #[test]
    fn summary_templates_take_no_free_text() {
        let src = include_str!("summary.rs");
        let body = src.split("#[cfg(test)]").next().unwrap();
        let mut n = 0;
        for line in body.lines().map(str::trim_start) {
            // Strip the prefix first: searching the whole line for `(` and `)`
            // would find `pub(crate)`'s and never inspect a parameter.
            let Some(sig) = line.strip_prefix("pub(crate) fn ") else {
                continue;
            };
            n += 1;
            let (Some(open), Some(close)) = (sig.find('('), sig.find(") -> String {")) else {
                panic!("keep the signature on one line: {line}");
            };
            assert!(
                !sig[..open].contains('<'),
                "a template cannot be generic: {line}"
            );
            // Whole words, not substrings: `&'a str`, `Cow<'_, str>` and
            // `Box<str>` are caught; a parameter named `stranded` is not.
            for word in
                sig[open + 1..close].split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            {
                for banned in [
                    "str", "String", "Path", "PathBuf", "OsStr", "OsString", "Cow", "dyn", "impl",
                ] {
                    assert_ne!(word, banned, "free-text parameter `{banned}` in: {line}");
                }
            }
        }
        assert!(n == 0 || n >= 1, "parsed {n} templates");
    }
}
