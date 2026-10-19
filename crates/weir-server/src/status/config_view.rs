//! ConfigView (spec §5.14): the effective configuration as the status
//! document and the startup log line carry it. Built by an EXHAUSTIVE
//! destructure of `Config` with no `..`: a field added to `Config` does not
//! compile until it is classified here. This is a *disclosure*
//! classification (who may see it), not SP-L's reloadable/restart-only one;
//! SP-L reuses the destructure.
//!
//! Classes: `Public` (the fixed §9.1 subset), `Admin` (shown on the admin
//! surface and in the startup line), `AdminRedacted` (shown reduced: a URL
//! as scheme://host[:port], a key id as a presence flag). A `Never` field
//! (the S3 secret; URL passwords; the bearer token, which is not in
//! `Config` at all) is bound to `_` and never entered.
use crate::config::Config;
use std::collections::BTreeMap;
use std::fmt;
use weir_status::{ConfigEvidence, ConfigValue, PublicConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Disclosure {
    Public,
    Admin,
    AdminRedacted,
}

pub(crate) struct ConfigView {
    entries: BTreeMap<&'static str, (Disclosure, ConfigValue)>,
    sink_type: &'static str,
    commit_timeout_secs: u64,
}

const UNPARSEABLE: &str = "<unparseable>";

/// `scheme://host[:port]` of a URL: userinfo, path, query and fragment are
/// dropped. Webhook-style URLs carry the secret in the path, which the
/// existing log redactors (sink/mod.rs:50-120) do not cover. Anything
/// without `://` is not echoed at all.
///
/// When the boundary between userinfo and path is ambiguous (an `@` after
/// the first `/`, `?` or `#`, as in an unencoded `/` inside a password), or
/// what remains is not a plausible `host[:port]`, nothing is echoed either:
/// over-redacting a display value is harmless, echoing half a password is not.
pub(crate) fn url_origin(url: &str) -> String {
    let Some(i) = url.find("://") else {
        return UNPARSEABLE.to_string();
    };
    let (scheme, rest) = (&url[..i], &url[i + 3..]);
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let scheme_ok = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let host_ok = !host.is_empty()
        && host.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '[' | ']' | '%')
        });
    if !scheme_ok || !host_ok || tail.contains('@') {
        return UNPARSEABLE.to_string();
    }
    format!("{scheme}://{host}")
}

#[derive(Default)]
struct Entries(BTreeMap<&'static str, (Disclosure, ConfigValue)>);

impl Entries {
    fn public(&mut self, key: &'static str, v: ConfigValue) {
        self.0.insert(key, (Disclosure::Public, v));
    }
    fn admin(&mut self, key: &'static str, v: ConfigValue) {
        self.0.insert(key, (Disclosure::Admin, v));
    }
    fn redacted(&mut self, key: &'static str, v: ConfigValue) {
        self.0.insert(key, (Disclosure::AdminRedacted, v));
    }
}

fn u(v: u64) -> ConfigValue {
    ConfigValue::U64(v)
}
fn b(v: bool) -> ConfigValue {
    ConfigValue::Bool(v)
}
fn s(v: impl fmt::Display) -> ConfigValue {
    ConfigValue::Str(v.to_string())
}
fn opt_s(v: Option<impl fmt::Display>) -> ConfigValue {
    v.map_or(ConfigValue::Null, s)
}

impl From<&Config> for ConfigView {
    fn from(c: &Config) -> Self {
        let Config {
            socket_path,
            wab_dir,
            shard_count,
            worker_count,
            batch_size,
            batch_deadline_ms,
            wab_segment_max_bytes,
            wab_segment_max_age_secs,
            wab_segment_max_lifetime_secs,
            wab_max_bytes,
            wab_compression,
            wab_compression_level,
            wab_dir_lock,
            max_connections,
            max_payload_bytes,
            max_batch_records,
            metrics_port,
            metrics_bind,
            metrics_max_connections,
            livez_checks,
            peer_uid_check,
            shutdown_timeout_secs,
            drain_shutdown_budget_secs,
            connection_read_timeout_secs,
            sink_type,
            sink_url,
            sink_timeout_secs,
            sink_max_batch_size,
            sink_send_idempotency_key,
            sink_http_concurrency,
            sink_http_batch,
            sink_max_retries,
            sink_retry_base_delay_ms,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_table,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_column,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_id_column,
            #[cfg(feature = "mysql-sink")]
            sink_mysql_insert_mode,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_table,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_column,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_id_column,
            #[cfg(feature = "postgres-sink")]
            sink_postgres_insert_mode,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_database,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_table,
            #[cfg(feature = "clickhouse-sink")]
            sink_clickhouse_column,
            sink_s3_bucket,
            sink_s3_region,
            sink_s3_endpoint,
            sink_s3_force_path_style,
            sink_s3_prefix,
            sink_s3_partition,
            sink_s3_framing,
            sink_s3_compression,
            sink_s3_access_key_id,
            // Never: a secret. Named so the destructure stays exhaustive.
            sink_s3_secret_access_key: _,
            sink_s3_storage_class,
            sink_s3_sse,
            sink_s3_sse_kms_key_id,
            dead_letter_max_bytes,
            dead_letter_check_interval_secs,
            health_poll_interval_secs,
            log_level,
            tcp_bind,
            tls_cert_path,
            tls_key_path,
            tls_client_ca_path,
            tls_handshake_timeout_secs,
        } = c;

        let mut v = Entries::default();
        // Public: the fixed §9.1 subset (numbers and booleans only).
        v.public("shard_count", u(*shard_count as u64));
        v.public("worker_count", u(*worker_count as u64));
        v.public("wab_max_bytes", u(*wab_max_bytes));
        v.public("dead_letter_max_bytes", u(*dead_letter_max_bytes));
        v.public("wab_segment_max_bytes", u(*wab_segment_max_bytes));
        v.public("wab_segment_max_age_secs", u(*wab_segment_max_age_secs));
        v.public(
            "wab_segment_max_lifetime_secs",
            u(*wab_segment_max_lifetime_secs),
        );
        v.public("sink_max_retries", u(u64::from(*sink_max_retries)));
        v.public("sink_timeout_secs", u(*sink_timeout_secs));
        v.public("health_poll_interval_secs", u(*health_poll_interval_secs));
        v.public("shutdown_timeout_secs", u(*shutdown_timeout_secs));
        v.public("drain_shutdown_budget_secs", u(*drain_shutdown_budget_secs));
        v.public("max_connections", u(*max_connections as u64));
        v.public("peer_uid_check", b(*peer_uid_check));
        v.public("tls_enabled", b(tcp_bind.is_some()));
        // Admin.
        v.admin("socket_path", s(socket_path.display()));
        v.admin("wab_dir", s(wab_dir.display()));
        v.admin("batch_size", u(*batch_size as u64));
        v.admin("batch_deadline_ms", u(*batch_deadline_ms));
        v.admin("wab_compression", s(wab_compression));
        v.admin(
            "wab_compression_level",
            ConfigValue::I64(i64::from(*wab_compression_level)),
        );
        v.admin(
            "wab_dir_lock",
            s(match wab_dir_lock {
                crate::wab::lock::LockPolicy::Required => "required",
                crate::wab::lock::LockPolicy::BestEffort => "best-effort",
            }),
        );
        v.admin("max_payload_bytes", u(*max_payload_bytes as u64));
        v.admin("max_batch_records", u(*max_batch_records as u64));
        v.admin("metrics_port", u(u64::from(*metrics_port)));
        v.admin("metrics_bind", s(metrics_bind));
        v.admin(
            "metrics_max_connections",
            u(*metrics_max_connections as u64),
        );
        // Admin, not public: the §9.1 public config subset is numbers and
        // booleans only; the public document carries the list as
        // `verdict.livez_checks` instead.
        v.admin(
            "livez_checks",
            s(livez_checks
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(",")),
        );
        v.admin(
            "connection_read_timeout_secs",
            u(*connection_read_timeout_secs),
        );
        v.admin("sink_type", s(sink_type.as_str()));
        match sink_url.0.as_deref() {
            Some(url) => {
                v.redacted("sink_url", s(url_origin(url)));
                v.redacted("sink_url_path_redacted", b(true));
            }
            None => v.admin("sink_url", ConfigValue::Null),
        }
        v.admin("sink_max_batch_size", u(*sink_max_batch_size as u64));
        v.admin("sink_send_idempotency_key", b(*sink_send_idempotency_key));
        v.admin("sink_http_concurrency", u(*sink_http_concurrency as u64));
        v.admin("sink_http_batch", s(sink_http_batch));
        v.admin("sink_retry_base_delay_ms", u(*sink_retry_base_delay_ms));
        #[cfg(feature = "mysql-sink")]
        {
            v.admin("sink_mysql_table", s(sink_mysql_table));
            v.admin("sink_mysql_column", s(sink_mysql_column));
            v.admin("sink_mysql_id_column", opt_s(sink_mysql_id_column.as_ref()));
            v.admin(
                "sink_mysql_insert_mode",
                s(format!("{sink_mysql_insert_mode:?}")),
            );
        }
        #[cfg(feature = "postgres-sink")]
        {
            v.admin("sink_postgres_table", s(sink_postgres_table));
            v.admin("sink_postgres_column", s(sink_postgres_column));
            v.admin(
                "sink_postgres_id_column",
                opt_s(sink_postgres_id_column.as_ref()),
            );
            v.admin(
                "sink_postgres_insert_mode",
                s(format!("{sink_postgres_insert_mode:?}")),
            );
        }
        #[cfg(feature = "clickhouse-sink")]
        {
            v.admin("sink_clickhouse_database", s(sink_clickhouse_database));
            v.admin("sink_clickhouse_table", s(sink_clickhouse_table));
            v.admin("sink_clickhouse_column", s(sink_clickhouse_column));
        }
        v.admin("sink_s3_bucket", s(sink_s3_bucket));
        v.admin("sink_s3_region", s(sink_s3_region));
        v.redacted(
            "sink_s3_endpoint",
            if sink_s3_endpoint.is_empty() {
                ConfigValue::Null
            } else {
                s(url_origin(sink_s3_endpoint))
            },
        );
        v.admin("sink_s3_force_path_style", b(*sink_s3_force_path_style));
        v.admin("sink_s3_prefix", s(sink_s3_prefix));
        v.admin("sink_s3_partition", s(sink_s3_partition));
        v.admin("sink_s3_framing", s(sink_s3_framing));
        v.admin("sink_s3_compression", s(sink_s3_compression));
        v.redacted(
            "sink_s3_access_key_id_set",
            b(!sink_s3_access_key_id.is_empty()),
        );
        v.admin("sink_s3_storage_class", s(sink_s3_storage_class));
        v.admin("sink_s3_sse", s(sink_s3_sse));
        v.redacted(
            "sink_s3_sse_kms_key_id_set",
            b(!sink_s3_sse_kms_key_id.is_empty()),
        );
        v.admin(
            "dead_letter_check_interval_secs",
            u(*dead_letter_check_interval_secs),
        );
        v.admin("log_level", s(log_level));
        v.admin("tcp_bind", opt_s(tcp_bind.as_ref()));
        v.admin(
            "tls_cert_path",
            opt_s(tls_cert_path.as_ref().map(|p| p.display())),
        );
        v.admin(
            "tls_key_path",
            opt_s(tls_key_path.as_ref().map(|p| p.display())),
        );
        v.admin(
            "tls_client_ca_path",
            opt_s(tls_client_ca_path.as_ref().map(|p| p.display())),
        );
        v.admin("tls_handshake_timeout_secs", u(*tls_handshake_timeout_secs));

        ConfigView {
            entries: v.0,
            sink_type: sink_type.as_str(),
            commit_timeout_secs: c.commit_timeout().as_secs(),
        }
    }
}

impl ConfigView {
    pub(crate) fn view(&self) -> BTreeMap<String, ConfigValue> {
        self.entries
            .iter()
            .map(|(k, (_, v))| (k.to_string(), v.clone()))
            .collect()
    }

    pub(crate) fn public(&self) -> PublicConfig {
        PublicConfig::from_view(&self.view())
    }

    pub(crate) fn evidence(&self) -> ConfigEvidence {
        ConfigEvidence {
            sink_type: self.sink_type.to_string(),
            commit_timeout_secs: Some(self.commit_timeout_secs),
            public: self.public(),
            view: self.view(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 5 / PR 6 read these
    pub(crate) fn disclosure(&self, key: &str) -> Option<Disclosure> {
        self.entries.get(key).map(|(d, _)| *d)
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 5 / PR 6 read these
    pub(crate) fn sink_type(&self) -> &'static str {
        self.sink_type
    }

    /// A view with no entries and the noop sink, for board tests.
    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        ConfigView {
            entries: BTreeMap::new(),
            sink_type: "noop",
            commit_timeout_secs: 60,
        }
    }
}

impl fmt::Debug for ConfigView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Raw<'a>(&'a ConfigValue);
        impl fmt::Debug for Raw<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    ConfigValue::Bool(v) => write!(f, "{v}"),
                    ConfigValue::U64(v) => write!(f, "{v}"),
                    ConfigValue::I64(v) => write!(f, "{v}"),
                    ConfigValue::Str(v) => write!(f, "{v:?}"),
                    ConfigValue::Null => f.write_str("null"),
                }
            }
        }
        f.debug_map()
            .entries(self.entries.iter().map(|(k, (_, v))| (k, Raw(v))))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PartialConfig;
    use std::time::Duration;

    fn config(label: &str, layer: PartialConfig) -> Config {
        let dir = crate::testutil::scratch_dir(&format!("cfgview_{label}"));
        crate::testutil::mkdir_p(&dir);
        Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir),
                ..layer
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .expect("valid config")
    }

    #[test]
    fn public_entries_are_exactly_the_spec_9_1_subset() {
        let v = ConfigView::from(&config("public", PartialConfig::empty()));
        let mut public: Vec<String> = v
            .entries
            .iter()
            .filter(|(_, (d, _))| *d == Disclosure::Public)
            .map(|(k, _)| k.to_string())
            .collect();
        public.sort();
        let mut doc: Vec<String> = serde_json::to_value(PublicConfig::default())
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        doc.sort();
        assert_eq!(public, doc);
        let p = v.public();
        assert!(
            p.shard_count.is_some() && p.tls_enabled == Some(false) && p.peer_uid_check.is_some()
        );
    }

    #[cfg(feature = "http-sink")]
    #[test]
    fn a_sink_url_is_reduced_to_scheme_host_and_port() {
        let v = ConfigView::from(&config(
            "url",
            PartialConfig {
                sink_type: Some("http".into()),
                sink_url: Some(
                    "https://user:SENTINEL_PW@sink.example:8443/hooks/SENTINEL_PATH?token=SENTINEL_TOK"
                        .into(),
                ),
                ..PartialConfig::empty()
            },
        ));
        let view = v.view();
        assert_eq!(
            view["sink_url"],
            ConfigValue::Str("https://sink.example:8443".into())
        );
        assert_eq!(view["sink_url_path_redacted"], ConfigValue::Bool(true));
        assert_eq!(v.disclosure("sink_url"), Some(Disclosure::AdminRedacted));
        let shown = format!("{v:?} {:?}", serde_json::to_string(&view).unwrap());
        assert!(!shown.contains("SENTINEL"), "{shown}");
    }

    #[test]
    fn the_s3_secret_never_enters_the_view_and_the_key_id_is_only_a_presence_flag() {
        let v = ConfigView::from(&config(
            "s3secret",
            PartialConfig {
                sink_s3_secret_access_key: Some("SENTINEL_SECRET".into()),
                sink_s3_access_key_id: Some("SENTINEL_KEYID".into()),
                ..PartialConfig::empty()
            },
        ));
        let shown = format!("{v:?} {}", serde_json::to_string(&v.view()).unwrap());
        assert!(!shown.contains("SENTINEL"), "{shown}");
        assert_eq!(
            v.view()["sink_s3_access_key_id_set"],
            ConfigValue::Bool(true)
        );
        assert!(v.disclosure("sink_s3_secret_access_key").is_none());
    }

    /// `livez_checks` is an admin key (not in the §9.1 public subset),
    /// rendered as the comma-joined catalogue codes.
    #[test]
    fn livez_checks_is_an_admin_key_rendered_comma_joined() {
        let v = ConfigView::from(&config(
            "livez",
            PartialConfig {
                livez_checks: Some("worker_exited,listener_down".into()),
                ..PartialConfig::empty()
            },
        ));
        assert_eq!(v.disclosure("livez_checks"), Some(Disclosure::Admin));
        assert_eq!(
            v.view()["livez_checks"],
            ConfigValue::Str("listener_down,worker_exited".into())
        );
        let empty = ConfigView::from(&config("livez_empty", PartialConfig::empty()));
        assert_eq!(
            empty.view()["livez_checks"],
            ConfigValue::Str(String::new())
        );
    }

    #[test]
    fn url_origin_strips_userinfo_path_query_and_fragment() {
        for (input, want) in [
            ("https://h", "https://h"),
            ("https://u:p@h:1/x?y#z", "https://h:1"),
            ("https://u:p@ss@h/x", "https://h"),
            ("mysql://u:p@db:3306/weir", "mysql://db:3306"),
            ("postgres://u@h/db?sslmode=require", "postgres://h"),
            ("http://h?token=x", "http://h"),
            ("not a url at all", "<unparseable>"),
        ] {
            assert_eq!(url_origin(input), want, "{input}");
        }
    }

    #[test]
    fn evidence_carries_the_sink_type_and_the_commit_backstop() {
        let c = config("evidence", PartialConfig::empty());
        let e = ConfigView::from(&c).evidence();
        assert_eq!(e.sink_type, c.sink_type.as_str());
        assert_eq!(e.commit_timeout_secs, Some(c.commit_timeout().as_secs()));
        assert!(c.commit_timeout() >= Duration::from_secs(60));
        assert_eq!(e.public.wab_max_bytes, Some(c.wab_max_bytes));
        assert!(e.view.contains_key("wab_dir"));
    }

    #[test]
    fn the_startup_line_renders_plain_values() {
        let v = ConfigView::from(&config("debug", PartialConfig::empty()));
        let line = format!("{v:?}");
        assert!(line.contains("\"shard_count\": "), "{line}");
        assert!(
            !line.contains("U64("),
            "values print raw, not as enum variants: {line}"
        );
    }

    /// Stricter than a plain authority split: when an unencoded `/`, `?` or
    /// `#` in a password makes the userinfo boundary ambiguous, or what is
    /// left is not a plausible host, nothing of the URL is echoed.
    #[test]
    fn url_origin_echoes_nothing_when_the_userinfo_boundary_is_ambiguous() {
        for input in [
            "https://user:pa/SENTINEL@h/x",
            "https://user:pa?SENTINEL@h/x",
            "https://user:pa#SENTINEL@h",
            "https://h/hooks/SENTINEL@x",
            "ht tp://h",
            "https://",
        ] {
            assert_eq!(url_origin(input), "<unparseable>", "{input}");
        }
        assert_eq!(url_origin("https://[::1]:8443/x"), "https://[::1]:8443");
        // Userinfo itself may hold anything: it is dropped whole.
        assert_eq!(url_origin("https://SENTINEL PW@h"), "https://h");
    }

    #[test]
    fn empty_for_test_has_no_entries_and_the_noop_sink() {
        let v = ConfigView::empty_for_test();
        assert!(v.view().is_empty());
        assert_eq!(v.sink_type(), "noop");
        assert_eq!(v.evidence().commit_timeout_secs, Some(60));
    }
}
