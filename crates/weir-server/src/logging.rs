//! Tracing setup (spec §5.13, SP-D). No new dependencies.
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

/// The filter for `log_level`, and the warning to log when it does not
/// parse. Previously an invalid level fell back to `info` silently, so a
/// typo looked like a working config.
///
/// A bare word must be a level name. `EnvFilter` would take a misspelt level
/// such as `warning` as a *target* directive (that target at TRACE, nothing
/// else), which turns every weir log line off without a word; so a target
/// is accepted only in the `target=level` form.
pub(crate) fn env_filter_for(level: &str) -> (EnvFilter, Option<String>) {
    let fallback = |why: String| {
        (
            EnvFilter::new("info"),
            Some(format!(
                "log_level {level:?} is not a valid filter ({why}); logging at \"info\" instead"
            )),
        )
    };
    if let Some(bad) = level.split(',').map(str::trim).find(|d| {
        !d.is_empty()
            && !d.contains(['=', '[', '{'])
            && d.parse::<tracing_subscriber::filter::LevelFilter>()
                .is_err()
    }) {
        return fallback(format!(
            "{bad:?} is not a level; use off, error, warn, info, debug or trace, \
             or target=level for one target"
        ));
    }
    match EnvFilter::try_new(level) {
        Ok(f) => (f, None),
        Err(e) => fallback(e.to_string()),
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

    /// F58: a mistyped level name such as "warning" parses as a bare
    /// *target* directive, which enables only that (nonexistent) target and
    /// so silently disables every log line. It must warn and fall back.
    #[test]
    fn a_misspelt_level_name_is_not_taken_as_a_target() {
        for level in [
            "warning",
            "information",
            "WARNING",
            "info,warning",
            "weir_server=debug,verbose",
        ] {
            let (filter, warning) = env_filter_for(level);
            let w = warning.unwrap_or_else(|| panic!("{level}: no warning"));
            assert!(w.contains(level) && w.contains("\"info\""), "{w}");
            assert_eq!(filter.to_string(), "info", "{level}");
        }
    }

    #[test]
    fn bare_levels_in_any_case_and_targeted_directives_still_parse() {
        for level in [
            "WARN",
            "Error",
            "off",
            "trace",
            "weir_server=debug",
            "info,weir_core=trace",
        ] {
            assert!(env_filter_for(level).1.is_none(), "{level}");
        }
    }
}
