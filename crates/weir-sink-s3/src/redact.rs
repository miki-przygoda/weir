//! Log-hygiene helpers.
//!
//! `weir-server` has equivalents — `sink/mod.rs::sanitize_log_excerpt` (`:135`)
//! and `::redact_url_password` (`:50`) — but both are `pub(crate)` and
//! deliberately so: the daemon does not publish a log-hygiene API. This crate
//! therefore carries its own copies.
//!
//! That duplication is the first concrete cost of building this sink as an
//! out-of-tree crate, and it is worth paying: promoting those helpers would
//! commit weir to maintaining a public surface it never intended. If a third
//! sink crate ever needs them, the answer is a small `weir-sink-util` crate,
//! not a wider `weir-sink-sdk`.

/// A string that never appears in `Debug` output.
///
/// The sink's config is `Debug`-formatted into the daemon's startup `INFO` log,
/// so a secret with a derived `Debug` would be written to disk in plaintext on
/// every boot. There is deliberately no `Display` and no `Deref`: reading the
/// secret requires calling [`SecretString::expose`], which is greppable.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SecretString(String);

impl SecretString {
    /// Wraps a secret.
    pub(crate) fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Yields the secret. Named to make call sites obvious in review.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

impl From<&str> for SecretString {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

/// Replaces control characters with `.` for safe logging.
///
/// Sink response bodies are interpolated into log lines and dead-letter
/// reasons. Without this, a hostile or compromised endpoint could embed
/// newlines — forging entire log records — or terminal escape sequences, and
/// have the daemon emit them verbatim. Mirrors weir-server's S29 defence.
pub(crate) fn sanitize_log_excerpt(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '.' } else { c })
        .collect()
}

/// Truncates an excerpt to `max` bytes on a character boundary.
///
/// An S3 error body is normally a short XML document, but nothing guarantees
/// that — and an unbounded excerpt in a dead-letter reason is written to disk
/// once per failed record.
pub(crate) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_never_appears_in_debug_output() {
        let s = SecretString::new("wJalrXUtnFEMI/K7MDENG");
        let rendered = format!("{s:?}");
        assert!(!rendered.contains("wJalr"), "secret leaked: {rendered}");
        assert!(rendered.contains("redacted"), "{rendered}");
        // Also inside a containing struct, which is how it actually reaches logs.
        #[derive(Debug)]
        struct Cfg {
            #[allow(dead_code)]
            key: SecretString,
        }
        let outer = format!(
            "{:?}",
            Cfg {
                key: SecretString::new("wJalrXUtnFEMI/K7MDENG")
            }
        );
        assert!(
            !outer.contains("wJalr"),
            "secret leaked via container: {outer}"
        );
    }

    #[test]
    fn the_secret_is_still_reachable_deliberately() {
        assert_eq!(SecretString::new("abc").expose(), "abc");
    }

    #[test]
    fn control_characters_are_stripped_from_log_excerpts() {
        // A hostile endpoint's response body reaches the log and the
        // dead-letter reason. Newlines there forge log records (S29).
        let out = sanitize_log_excerpt("ok\nFAKE LOG LINE\x1b[31m");
        assert!(!out.contains('\n'));
        assert!(!out.contains('\x1b'));
        assert_eq!(out, "ok.FAKE LOG LINE.[31m");
    }

    #[test]
    fn truncation_lands_on_a_character_boundary() {
        // Slicing a multi-byte character in half would panic, and this input is
        // attacker-controlled.
        let s = "aaaa\u{e9}\u{e9}\u{e9}";
        for max in 0..s.len() + 2 {
            let t = truncate(s, max);
            assert!(t.chars().count() <= s.chars().count() + 1, "max={max}");
        }
        assert_eq!(truncate("short", 100), "short");
        assert_eq!(truncate("abcdef", 3), "abc…");
    }
}
