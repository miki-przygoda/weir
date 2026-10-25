//! The admin socket protocol, version 1 (Foundations spec §11).
//!
//! UTF-8 JSON lines, one object per `\n` each way. Shared by the daemon
//! (weir-server's `admin` module) and weir-ctl, so both sides agree on the
//! envelope, the verb names, the error codes and the default socket path.
//! Pure: no I/O, no clock.

use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = crate::ADMIN_PROTOCOL; // 1; one constant for the document and the envelope
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Read by both binaries (spec §11 "one env var for both binaries").
pub const ENV_SOCKET_PATH: &str = "WEIR_ADMIN_SOCKET_PATH";
pub const SOCKET_SUFFIX: &str = ".admin.sock";
/// Portable `sun_path` limit: 104 bytes on macOS including the NUL.
pub const MAX_SOCKET_PATH_BYTES: usize = 103;
pub const MAX_LINE_BYTES: usize = 64 * 1024;
pub const MAX_WAIT_MS: u64 = 30_000;
pub const DEFAULT_PAUSE_WAIT_MS: u64 = 5_000;
pub const DEFAULT_RETRY_WAIT_MS: u64 = 10_000;

pub const PAUSE_NOTE: &str =
    "takes effect before the next commit batch; in-memory only — a restart resumes delivery";
pub const RETRY_NOTE: &str = "requeued segments restart from record 0; a running retry \
    backoff is cut at most once, never below base_retry_delay";
pub const WARN_UNBOUNDED_WAB: &str = "wab_max_bytes is 0: the WAB grows without bound while paused";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Verb {
    Status,
    Reload,
    DrainPause,
    DrainResume,
    DrainRetryStranded,
}

impl Verb {
    pub const ALL: [Verb; 5] = [
        Verb::Status,
        Verb::Reload,
        Verb::DrainPause,
        Verb::DrainResume,
        Verb::DrainRetryStranded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Verb::Status => "status",
            Verb::Reload => "reload",
            Verb::DrainPause => "drain.pause",
            Verb::DrainResume => "drain.resume",
            Verb::DrainRetryStranded => "drain.retry_stranded",
        }
    }

    pub fn parse(s: &str) -> Option<Verb> {
        Verb::ALL.into_iter().find(|v| v.as_str() == s)
    }

    /// Refused with `shutting_down` after T0 (spec §7 (f)).
    pub fn is_mutating(self) -> bool {
        matches!(
            self,
            Verb::DrainPause | Verb::DrainResume | Verb::DrainRetryStranded
        )
    }

    /// The `admin.verbs` list of the status document.
    pub fn names() -> Vec<&'static str> {
        Verb::ALL.iter().map(|v| v.as_str()).collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InstanceId {
    pub pid: u32,
    pub started_at_ms: u64,
    pub wab_dir: String,
    pub socket_path: String,
    pub wab_dir_dev: u64,
    pub wab_dir_ino: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Request {
    pub v: u32,
    #[serde(default)]
    pub id: Value,
    pub verb: String,
    #[serde(default)]
    pub args: Value,
}

impl Request {
    pub fn new(id: Value, verb: Verb, args: Value) -> Request {
        Request {
            v: PROTOCOL_VERSION,
            id,
            verb: verb.as_str().to_string(),
            args,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Response {
    pub v: u32,
    pub id: Value,
    /// On every response to an authorized peer; absent on `forbidden`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AdminError>,
}

impl Response {
    pub fn success(id: Value, instance: InstanceId, result: Value) -> Response {
        Response {
            v: PROTOCOL_VERSION,
            id,
            instance: Some(instance),
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(id: Value, instance: Option<InstanceId>, error: AdminError) -> Response {
        Response {
            v: PROTOCOL_VERSION,
            id,
            instance,
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AdminError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported: Option<Vec<u32>>,
}

impl AdminError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> AdminError {
        AdminError {
            code,
            message: message.into(),
            retryable: code.retryable(),
            supported: None,
        }
    }

    pub fn unsupported_version(message: impl Into<String>) -> AdminError {
        AdminError {
            supported: Some(SUPPORTED_VERSIONS.to_vec()),
            ..AdminError::new(ErrorCode::UnsupportedVersion, message)
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    UnsupportedVersion,
    UnknownVerb,
    RequestTooLarge,
    Forbidden,
    Busy,
    ShuttingDown,
    Unavailable,
    Internal,
    /// A code from a newer daemon. Never sent.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::UnsupportedVersion => "unsupported_version",
            ErrorCode::UnknownVerb => "unknown_verb",
            ErrorCode::RequestTooLarge => "request_too_large",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::Busy => "busy",
            ErrorCode::ShuttingDown => "shutting_down",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::Internal => "internal",
            ErrorCode::Unknown => "unknown",
        }
    }

    /// Whether repeating the same request later can succeed.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::Busy | ErrorCode::Unavailable | ErrorCode::Internal
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HeldSegment {
    pub segment: String,
    pub processed_records: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PauseResult {
    pub generation: u64,
    pub pause_requested: bool,
    pub paused: bool,
    pub held: bool,
    pub paused_since_ms: Option<u64>,
    pub held_segment: Option<HeldSegment>,
    pub warnings: Vec<String>,
    pub note: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ResumeResult {
    pub generation: u64,
    pub pause_requested: bool,
    pub was_paused: bool,
    pub paused_for_secs: Option<f64>,
    /// Additive within v1: the drain was observed no longer paused or held.
    pub resumed: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RetryStrandedResult {
    pub ticket: u64,
    pub completed: bool,
    pub coalesced: bool,
    pub requeued: Option<u64>,
    pub already_queued: Option<u64>,
    pub backoff_cut: bool,
    /// Admin-only free text (the socket is admin-only).
    pub scan_error: Option<String>,
    pub paused: bool,
    pub note: String,
}

/// The reload outcome on the admin wire. Twin of PR 1's
/// [`crate::ReloadOutcomeCode`] (same names, pinned by a test), plus
/// `Unknown` so an older weir-ctl reads a newer daemon's outcome.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReloadOutcomeWire {
    Applied,
    Noop,
    Failed,
    #[serde(other)]
    Unknown,
}

/// One item's reload outcome on the admin wire. Twin of PR 1's
/// [`crate::ReloadItemOutcome`] (same names, pinned by a test), plus `Unknown`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReloadItemOutcomeWire {
    Reloaded,
    Failed,
    NotConfigured,
    #[serde(other)]
    Unknown,
}

/// One reloadable item on the admin wire. Same shape as PR 1's
/// `AdminReloadItem`, but with the forward-compatible outcome.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReloadItemWire {
    pub item: String,
    pub outcome: ReloadItemOutcomeWire,
    /// Admin only (spec §9.1): the public status type drops it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NotReloaded {
    pub item: String,
    pub reason_code: String,
    pub reason: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReloadResult {
    pub trigger: String,
    pub at_unix_ms: u64,
    pub outcome: ReloadOutcomeWire,
    pub items: Vec<ReloadItemWire>,
    pub not_reloaded: Vec<NotReloaded>,
}

/// `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` (spec §5.9). `None`
/// when `wab_dir` has no parent or no final component (`/`, a relative
/// single component). weir-ctl derives the same path from `--wab-dir`.
pub fn default_socket_path(wab_dir: &Path) -> Option<PathBuf> {
    let base = wab_dir.file_name()?;
    let parent = wab_dir.parent()?;
    if parent.as_os_str().is_empty() {
        return None;
    }
    let mut name = base.to_os_string();
    name.push(SOCKET_SUFFIX);
    Some(parent.join(name))
}

/// Removes `.` components and doubled separators without touching the
/// filesystem (spec §11 "lexical normalization").
pub fn lexical_normalize(p: &Path) -> PathBuf {
    p.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::{Path, PathBuf};

    fn instance() -> InstanceId {
        InstanceId {
            pid: 42,
            started_at_ms: 1_700_000_000_000,
            wab_dir: "/var/lib/weir/wab".into(),
            socket_path: "/run/weir/weir.sock".into(),
            wab_dir_dev: 7,
            wab_dir_ino: 9,
        }
    }

    #[test]
    fn verbs_round_trip_and_unknown_is_none() {
        for v in Verb::ALL {
            assert_eq!(Verb::parse(v.as_str()), Some(v));
        }
        assert_eq!(
            Verb::names(),
            vec![
                "status",
                "reload",
                "drain.pause",
                "drain.resume",
                "drain.retry_stranded"
            ]
        );
        assert_eq!(
            Verb::names(),
            crate::ADMIN_VERBS.to_vec(),
            "the status document's admin.verbs (PR 1) and the protocol name the same verbs"
        );
        assert_eq!(
            Verb::parse("drain_pause"),
            None,
            "the wire spelling is dotted"
        );
        assert_eq!(Verb::parse("STATUS"), None);
        assert!(Verb::DrainPause.is_mutating() && Verb::DrainRetryStranded.is_mutating());
        assert!(!Verb::Status.is_mutating() && !Verb::Reload.is_mutating());
    }

    #[test]
    fn the_default_path_sits_beside_wab_dir() {
        assert_eq!(
            default_socket_path(Path::new("/var/lib/weir/wab")),
            Some(PathBuf::from("/var/lib/weir/wab.admin.sock"))
        );
        assert_eq!(
            default_socket_path(Path::new("/var/lib/weir/wab/")),
            Some(PathBuf::from("/var/lib/weir/wab.admin.sock"))
        );
        assert_eq!(
            default_socket_path(Path::new("/wab")),
            Some(PathBuf::from("/wab.admin.sock"))
        );
        assert_eq!(default_socket_path(Path::new("/")), None);
        assert_eq!(
            default_socket_path(Path::new("wab")),
            None,
            "relative: no parent"
        );
    }

    #[test]
    fn lexical_normalize_drops_dot_and_doubled_separators() {
        assert_eq!(
            lexical_normalize(Path::new("/run/weir//./weir.sock")),
            PathBuf::from("/run/weir/weir.sock")
        );
        assert_eq!(
            lexical_normalize(Path::new("/run/weir/weir.sock")),
            PathBuf::from("/run/weir/weir.sock")
        );
    }

    /// The envelope's key order and shape are the contract weir-ctl parses.
    #[test]
    fn success_envelope_is_pinned() {
        let r = Response::success(json!(7), instance(), json!({"x": 1}));
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"v":1,"id":7,"instance":{"pid":42,"started_at_ms":1700000000000,"wab_dir":"/var/lib/weir/wab","socket_path":"/run/weir/weir.sock","wab_dir_dev":7,"wab_dir_ino":9},"ok":true,"result":{"x":1}}"#
        );
    }

    /// A forbidden peer learns nothing about the daemon: no instance key.
    #[test]
    fn failure_without_instance_omits_the_key() {
        let r = Response::failure(
            serde_json::Value::Null,
            None,
            AdminError::new(ErrorCode::Forbidden, "forbidden"),
        );
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"v":1,"id":null,"ok":false,"error":{"code":"forbidden","message":"forbidden","retryable":false}}"#
        );
    }

    #[test]
    fn unsupported_version_lists_what_is_supported() {
        let e = AdminError::unsupported_version("protocol version 2 is not supported");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "unsupported_version");
        assert_eq!(v["supported"], json!([1]));
        assert_eq!(v["retryable"], false);
    }

    #[test]
    fn retryable_codes() {
        for c in [ErrorCode::Busy, ErrorCode::Unavailable, ErrorCode::Internal] {
            assert!(c.retryable(), "{c:?}");
        }
        for c in [
            ErrorCode::BadRequest,
            ErrorCode::UnsupportedVersion,
            ErrorCode::UnknownVerb,
            ErrorCode::RequestTooLarge,
            ErrorCode::Forbidden,
            ErrorCode::ShuttingDown,
        ] {
            assert!(!c.retryable(), "{c:?}");
        }
    }

    /// An older weir-ctl reading a newer daemon's error code keeps working.
    #[test]
    fn an_unknown_error_code_deserializes() {
        let e: AdminError = serde_json::from_value(json!({
            "code": "rate_limited", "message": "m", "retryable": true
        }))
        .unwrap();
        assert_eq!(e.code, ErrorCode::Unknown);
    }

    #[test]
    fn request_round_trip_and_defaults() {
        let r = Request::new(json!("a"), Verb::DrainPause, json!({"wait_ms": 0}));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(
            s,
            r#"{"v":1,"id":"a","verb":"drain.pause","args":{"wait_ms":0}}"#
        );
        let back: Request = serde_json::from_str(r#"{"v":1,"verb":"status"}"#).unwrap();
        assert_eq!(back.id, serde_json::Value::Null);
        assert_eq!(back.args, serde_json::Value::Null);
    }

    #[test]
    fn reload_result_shape() {
        let r = ReloadResult {
            trigger: "admin".into(),
            at_unix_ms: 5,
            outcome: ReloadOutcomeWire::Noop,
            items: vec![ReloadItemWire {
                item: "tls_certificates".into(),
                outcome: ReloadItemOutcomeWire::NotConfigured,
                detail: None,
            }],
            not_reloaded: vec![],
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["outcome"], "noop");
        assert_eq!(v["items"][0]["outcome"], "not_configured");
        assert!(
            v["items"][0].get("detail").is_none(),
            "None detail is omitted"
        );
    }

    /// The admin wire enums and PR 1's status enums name the same values:
    /// two definitions in one crate, pinned together. The matches are
    /// exhaustive, so a variant added to PR 1's set fails to compile here.
    #[test]
    fn admin_reload_wire_names_match_the_status_enums() {
        for &r in crate::ReloadItemOutcome::ALL {
            let twin = match r {
                crate::ReloadItemOutcome::Reloaded => ReloadItemOutcomeWire::Reloaded,
                crate::ReloadItemOutcome::Failed => ReloadItemOutcomeWire::Failed,
                crate::ReloadItemOutcome::NotConfigured => ReloadItemOutcomeWire::NotConfigured,
            };
            assert_eq!(
                serde_json::to_value(r).unwrap(),
                serde_json::to_value(twin).unwrap(),
                "{r:?}"
            );
        }
        for &r in crate::ReloadOutcomeCode::ALL {
            let twin = match r {
                crate::ReloadOutcomeCode::Applied => ReloadOutcomeWire::Applied,
                crate::ReloadOutcomeCode::Noop => ReloadOutcomeWire::Noop,
                crate::ReloadOutcomeCode::Failed => ReloadOutcomeWire::Failed,
            };
            assert_eq!(
                serde_json::to_value(r).unwrap(),
                serde_json::to_value(twin).unwrap(),
                "{r:?}"
            );
        }
    }
}
