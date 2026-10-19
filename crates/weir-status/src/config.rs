//! Effective configuration as the status document carries it (spec §5.14, §9.1).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One rendered config value. Untagged: `true`, `5`, `-1`, `"x"`, `null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
    Null,
}

/// The fixed public subset (spec §9.1): numbers and booleans only, so it
/// cannot carry a path, a URL, a host or a bucket.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PublicConfig {
    pub shard_count: Option<u64>,
    pub worker_count: Option<u64>,
    pub wab_max_bytes: Option<u64>,
    pub dead_letter_max_bytes: Option<u64>,
    pub wab_segment_max_bytes: Option<u64>,
    pub wab_segment_max_age_secs: Option<u64>,
    pub wab_segment_max_lifetime_secs: Option<u64>,
    pub sink_max_retries: Option<u64>,
    pub sink_timeout_secs: Option<u64>,
    pub health_poll_interval_secs: Option<u64>,
    pub shutdown_timeout_secs: Option<u64>,
    pub drain_shutdown_budget_secs: Option<u64>,
    pub max_connections: Option<u64>,
    pub peer_uid_check: Option<bool>,
    pub tls_enabled: Option<bool>,
}

impl PublicConfig {
    /// Picks the public keys out of a full config view; every other key is
    /// ignored, which is what makes the subset fixed.
    pub fn from_view(view: &BTreeMap<String, ConfigValue>) -> Self {
        let u = |k: &str| match view.get(k) {
            Some(ConfigValue::U64(v)) => Some(*v),
            _ => None,
        };
        let b = |k: &str| match view.get(k) {
            Some(ConfigValue::Bool(v)) => Some(*v),
            _ => None,
        };
        PublicConfig {
            shard_count: u("shard_count"),
            worker_count: u("worker_count"),
            wab_max_bytes: u("wab_max_bytes"),
            dead_letter_max_bytes: u("dead_letter_max_bytes"),
            wab_segment_max_bytes: u("wab_segment_max_bytes"),
            wab_segment_max_age_secs: u("wab_segment_max_age_secs"),
            wab_segment_max_lifetime_secs: u("wab_segment_max_lifetime_secs"),
            sink_max_retries: u("sink_max_retries"),
            sink_timeout_secs: u("sink_timeout_secs"),
            health_poll_interval_secs: u("health_poll_interval_secs"),
            shutdown_timeout_secs: u("shutdown_timeout_secs"),
            drain_shutdown_budget_secs: u("drain_shutdown_budget_secs"),
            max_connections: u("max_connections"),
            peer_uid_check: b("peer_uid_check"),
            tls_enabled: b("tls_enabled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_config_takes_only_the_fixed_public_keys() {
        let mut view = BTreeMap::new();
        view.insert("shard_count".to_string(), ConfigValue::U64(4));
        view.insert("peer_uid_check".to_string(), ConfigValue::Bool(true));
        view.insert("tls_enabled".to_string(), ConfigValue::Bool(false));
        view.insert(
            "sink_url".to_string(),
            ConfigValue::Str("https://h:443".into()),
        );
        view.insert(
            "wab_dir".to_string(),
            ConfigValue::Str("/var/lib/weir/wab".into()),
        );
        let p = PublicConfig::from_view(&view);
        assert_eq!(p.shard_count, Some(4));
        assert_eq!(p.peer_uid_check, Some(true));
        assert_eq!(p.tls_enabled, Some(false));
        assert_eq!(p.worker_count, None);
        let json = serde_json::to_string(&p).unwrap();
        assert!(
            !json.contains("https") && !json.contains("/var/lib"),
            "{json}"
        );
    }

    #[test]
    fn config_values_serialise_untagged() {
        let v = vec![
            ConfigValue::Bool(true),
            ConfigValue::U64(5),
            ConfigValue::I64(-1),
            ConfigValue::Str("x".into()),
            ConfigValue::Null,
        ];
        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"[true,5,-1,"x",null]"#
        );
    }
}
