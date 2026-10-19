//! Tracing setup (spec §5.13, SP-D). No new dependencies.
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

/// The filter for `log_level`, and the warning to log when it does not
/// parse. Previously an invalid level fell back to `info` silently, so a
/// typo looked like a working config.
pub(crate) fn env_filter_for(level: &str) -> (EnvFilter, Option<String>) {
    match EnvFilter::try_new(level) {
        Ok(f) => (f, None),
        Err(e) => (
            EnvFilter::new("info"),
            Some(format!(
                "log_level {level:?} is not a valid filter ({e}); logging at \"info\" instead"
            )),
        ),
    }
}

/// Installs the global subscriber. Colour only when stdout, the stream the
/// fmt layer writes to, is a terminal: a journal or a redirected file gets
/// plain text. Returns the invalid-level warning for the caller to emit
/// once the subscriber exists.
pub(crate) fn init(level: &str) -> Option<String> {
    let (filter, warning) = env_filter_for(level);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    warning
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_level_has_no_warning() {
        for level in ["info", "warn", "weir_server=debug,info"] {
            assert!(env_filter_for(level).1.is_none(), "{level}");
        }
    }

    #[test]
    fn an_invalid_level_falls_back_to_info_and_says_so() {
        let (filter, warning) = env_filter_for("weir_server=notalevel");
        let w = warning.expect("a warning");
        assert!(
            w.contains("weir_server=notalevel") && w.contains("\"info\""),
            "{w}"
        );
        assert_eq!(filter.to_string(), "info");
    }
}
