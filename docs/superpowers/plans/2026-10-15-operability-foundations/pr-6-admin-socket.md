# Operability Foundations — PR 6: Admin Unix socket and the status / reload / drain verbs

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 6: Admin Unix socket and the status / reload / drain verbs

**Scope** (spec §16, PR table row 6, size M): the admin Unix socket (derived default path, refusal on an insecure parent, identity in every reply), the admin end of the Reloader (SIGHUP and the `reload` verb share `reload.rs`'s one reload loop), and the verbs `status`, `reload`, `drain.pause`, `drain.resume`, `drain.retry_stranded`. Spec sections 5.9, 5.10, 11; protocol detail in §11; verb semantics in §8; startup slot §6 step 7; exit slot §7 T3.

**Depends on:** PR 4 (Lifecycle, `ShutdownLevel`, the restructured startup with step 7 and the T3 exit), which itself sits on PR 1 (weir-status crate, `StatusBoard`, `AdminCell`, `ConfigView`, serde_json as a normal dependency, arc-swap unconditional) and PR 2 (`DrainControl`, the board's `DrainCell` publication, `RequeueResult`, `DrainExitInfo`). Developed **in parallel with PR 5** (spec §16 "PRs 5 and 6 after 4") and **merged after PR 5** (integrator decision): Task 6.14 reads PR 5's `Config::livez_checks`, and both PRs edit `docs/security/threat-model.md`, so this PR rebases onto PR 5. The `status` verb renders through PR 1's `status::render::render_admin_doc`, the same function PR 5's `/status` uses (see Task 6.9 Interfaces). PR 7 (weir-ctl) consumes the protocol types produced in Task 6.1.

**Behaviour change:** a new socket. By default every daemon now binds `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` (mode 0600, peer uid = daemon euid or root). An explicit `admin_socket_path` that cannot be bound is fatal at startup; a derived default that cannot be bound disables admin with an ERROR and `AdminCell::set_unavailable(AdminReason)`, raising the `admin_unavailable` condition. Wire protocol, on-disk format and every existing metric/`--json` shape are unchanged; config and metrics are additive.

**Merge gate:**
- every unit test below green: `cargo test -p weir-status` and `cargo test -p weir-server --bin weir-server -- --test-threads=1` (bin tests are single-threaded because `bind_hardened` swaps the process umask);
- `cargo test -p weir-server --test system -- --test-threads=1` including system tests 6 (pause, 1,000 pushes, resume), 7 (retry-stranded) and the existing 12 (SIGHUP survives);
- `cargo test -p weir-server --features tls --test tls_listener -- --test-threads=1` (admin reload: good certs applied, corrupt key failed and old certs still handshake);
- `cargo test -p weir-server --test docs_drift` (metric catalogue and the admin config-key guard);
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` and `cargo fmt --all -- --check`;
- the real CI jobs run locally with `act -W .github/workflows/ci.yml -j <job>` for every job, container jobs included;
- **a security review of the admin socket before merge** (Task 6.17, last step): peer predicate, forbidden-reply disclosure, parent-directory refusal, framing bounds, log content.

Test labels must keep `<canonical temp dir>/…/wab.admin.sock` ≤ 103 bytes on macOS (canonical $TMPDIR is ~56 bytes); a longer derived path disables admin (path_too_long).

**Worktree order (Execution layout step 4):** PR 6 runs Tasks 6.1-6.13 in its worktree. Tasks 6.14-6.17 start only after PR 5 has merged and PR 6 is rebased onto main.

**Reconciliations with the spec, decided here (code wins for current behaviour, spec wins for target):**
1. Spec §5.10 sketches a new `Reloader { tls, guard, board, metrics }` with `async fn reload(&self, Trigger) -> ReloadReport` and a report shaped `{trigger, at_unix_ms, outcome: applied|noop|failed, items, not_reloaded}`. The safety floor already shipped `reload.rs` with a different, working shape: a blocking `Reloader::reload(ReloadTrigger) -> ReloadReport{trigger, outcome: Ok|Noop|Failed, changed, rejected, note}` served one at a time by `reload_loop` over a bounded `mpsc` queue that already carries `ReloadRequest { reply: Option<oneshot::Sender<ReloadReport>> }` "for the admin verb". This PR **keeps that code path** (the serialisation the spec's `guard` asks for is the single loop), adds `at_unix_ms`, and maps the internal report to the spec's wire shape in one function, `ReloadReport::to_wire()` (Task 6.8). The admin verb enqueues a `ReloadRequest` with a reply channel, exactly as the safety floor intended.
2. Spec §5.9/§11 order is kept: `bind_admin` calls PR 3's `socket::refuse_if_live(path, "admin socket")` (wording specific to the admin socket, `AddrInUse` on a live socket), then `bind_hardened_with`, whose `remove_stale_socket_at_startup` probes again harmlessly (connect-only). Both refusals carry `ErrorKind::AddrInUse` and classify as `in_use` (Task 6.6).
3. Spec §11 says `admin_socket_path` must differ from `socket_path` "after both binds, by parent (dev, ino) plus basename". The admin socket binds at step 7, before the producer socket (step 11); a colliding producer bind would then fail on the admin socket's *live* listener with the misleading "socket_path is in use by a running process". So the (dev, ino) collision check runs **before** the admin bind (Task 6.6), where it can name the real problem. The lexical check runs at config load (Task 6.2).
4. Spec §11 says "a rejected peer gets one `forbidden` line" and also "`instance` is on every response". Sending `instance` (pid, absolute `wab_dir`, `socket_path`, dev/ino) to a peer that failed authorization contradicts G8. `instance` is therefore on every response **to an authorized peer**; the `forbidden` line omits it (`Response.instance` is `Option`, skipped when `None`). Flagged for the security review.
5. Spec §11 requires the parent directory to be "not group- or world-writable". A 0755 parent owned by *another non-root user* passes that test but lets that user squat the path. The admin bind also refuses a parent whose owner is neither the daemon euid nor root (Task 6.4). Flagged as an open question; it is a strict tightening and both shipped layouts (systemd `StateDirectory`, the image's `/var/lib/weir`) pass it.
6. Spec §5.9 passes `lifecycle` in `AdminState`. The admin server needs only the shutdown level, so `AdminState` holds the `watch::Receiver<ShutdownLevel>` from `Lifecycle::subscribe()` (PR 4) instead of the Lifecycle itself; refusals read `*level_rx.borrow()`.
7. Spec §11 "the verb label is bounded to the 5 verbs plus `_connect` and `_invalid`": label values are the exact verb strings (`drain.pause`, not `drain_pause`), via a hand-written `EncodeLabelValue` (Task 6.3). `outcome` is `ok | error | refused | busy`; the precise error code goes to the INFO/WARN log line, which keeps the family at 13 pre-created series.
8. Spec §5.9 lists `admin/{mod.rs,verbs.rs,identity.rs}`. This PR adds `admin/framing.rs` for the bounded line reader and the per-connection loop so they can be tested with a stub dispatcher.

**Dead code until wiring:** weir-server is a binary crate (its `lib.rs` exposes only `wab::format`), so `dead_code` fires on every `pub(crate)` and `pub` item nothing calls, in the bin target and in the test target that `--all-targets` also lints. The `admin` module is unreachable from `main` until Task 6.14, so Task 6.5 opens `admin/mod.rs` with an unconditional `#![allow(dead_code, reason = "wired into main by Task 6.14")]`, which covers `identity`, `framing`, `verbs` and `test_support` in both targets. Items outside `admin/` that only the admin module or `main`'s step 7 will call carry an item-level allowance with a `reason`, added by the task that creates them:
- Task 6.2: `AdminSocketSetting` (unconditional: the tests compare it only through the derived `PartialEq`, which dead-code analysis ignores, so its fields are unread in the test target too), `Config::admin_socket` and `AdminPathProblem::reason_code` (`cfg_attr(not(test), …)`);
- Task 6.3: `Metrics::{admin_requests, admin_socket_up}` (`cfg_attr(not(test), …)`; Task 6.3's test reads both);
- Task 6.4: `ParentPolicy` and `is_insecure_parent` (`cfg_attr(not(test), …)`);
- Task 6.8: `ReloadTrigger::Admin` keeps the safety floor's existing allowance; `ReloadReport::to_wire`, `Reloader::attach_board`, `ReloadWaitError`, `request_reload_and_wait`, `ADMIN_ENQUEUE_TIMEOUT`, `ADMIN_REPLY_TIMEOUT` and `ITEM_TLS_CERTIFICATES` get `cfg_attr(not(test), …)` (Task 6.8's tests use each of them).

Task 6.14 deletes every one of these, plus PR 1's `#[cfg_attr(not(test), allow(dead_code))]` on `impl AdminCell` and `impl ReloadCell` (status/board.rs), PR 2's allowance on `DrainControl::{request_pause, request_resume, request_requeue}` and PR 3's on `socket::refuse_if_live`, then runs `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` and `cargo clippy --all-targets -- -D warnings` clean. With these allowances every intermediate commit passes the per-task `cargo clippy -p weir-server --all-targets --all-features -- -D warnings`.

**Tasks:** 6.1 protocol types (weir-status) · 6.2 config keys · 6.3 metrics · 6.4 private-parent bind · 6.5 peer predicate and identity · 6.6 admin bind · 6.7 framing · 6.8 reload loop admin end · 6.9 `status`/`reload` · 6.10 `drain.pause`/`drain.resume` · 6.11 `drain.retry_stranded` · 6.12 dispatcher · 6.13 accept loop · 6.14 startup/exit wiring · 6.15 system tests 6 and 7 · 6.16 TLS admin reload · 6.17 docs and security review.

**Per-task loop** (same as the safety floor): failing test, see it fail, implement, see it pass, then the crate's full test suite, `cargo clippy -p <crate> --all-targets --all-features -- -D warnings`, `cargo fmt`, commit. Commit messages: the exact subject given, an explanatory body, **no trailers**, never the CI-skip token.

---

### Task 6.1: `weir_status::admin` — protocol v1 types and the derived socket path

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Create: `crates/weir-status/src/admin.rs`
- Modify: `crates/weir-status/src/lib.rs` (add `pub mod admin;`; PR 1 created the crate)
- Modify: `crates/weir-status/Cargo.toml` (move `serde_json = "1"` from PR 1's `[dev-dependencies]` to `[dependencies]`: the envelope's `id`, `args` and `result` are `serde_json::Value`. Spec §5.1 allows exactly serde and serde_json; serde_json is already in the lock tree, so no new crate enters the root lock; fuzz/Cargo.lock gains one edge (weir-status → serde_json))
- Modify: `fuzz/Cargo.lock` (regenerated: fuzz depends on weir-server by path, and its lock records only normal dependencies, so weir-status's entry gains `"serde_json"`)
- Test: `crates/weir-status/src/admin.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: the `weir-status` crate from PR 1 (`serde` with derive as a dependency, `serde_json` as a dev-dependency until this task moves it) and PR 1's `weir_status::{ADMIN_PROTOCOL, ADMIN_VERBS}` (the status document's `admin.protocol` / `admin.verbs`), which this module's `PROTOCOL_VERSION` and `Verb::names()` must equal.
- Produces (all `pub` in `weir_status::admin`):
  ```rust
  pub const PROTOCOL_VERSION: u32 = crate::ADMIN_PROTOCOL; // 1; one constant for the document and the envelope
  pub const SUPPORTED_VERSIONS: &[u32] = &[1];
  pub const ENV_SOCKET_PATH: &str = "WEIR_ADMIN_SOCKET_PATH";
  pub const SOCKET_SUFFIX: &str = ".admin.sock";
  pub const MAX_SOCKET_PATH_BYTES: usize = 103;
  pub const MAX_LINE_BYTES: usize = 64 * 1024;
  pub const MAX_WAIT_MS: u64 = 30_000;
  pub const DEFAULT_PAUSE_WAIT_MS: u64 = 5_000;
  pub const DEFAULT_RETRY_WAIT_MS: u64 = 10_000;
  pub const PAUSE_NOTE: &str;  pub const RETRY_NOTE: &str;  pub const WARN_UNBOUNDED_WAB: &str;
  pub enum Verb { Status, Reload, DrainPause, DrainResume, DrainRetryStranded }
  impl Verb { pub const ALL: [Verb; 5]; pub fn as_str(self) -> &'static str;
              pub fn parse(s: &str) -> Option<Verb>; pub fn is_mutating(self) -> bool;
              pub fn names() -> Vec<&'static str>; }
  pub struct InstanceId { pub pid: u32, pub started_at_ms: u64, pub wab_dir: String,
                          pub socket_path: String, pub wab_dir_dev: u64, pub wab_dir_ino: u64 }
  pub struct Request { pub v: u32, pub id: serde_json::Value, pub verb: String, pub args: serde_json::Value }
  impl Request { pub fn new(id: serde_json::Value, verb: Verb, args: serde_json::Value) -> Request }
  pub struct Response { pub v: u32, pub id: serde_json::Value, pub instance: Option<InstanceId>,
                        pub ok: bool, pub result: Option<serde_json::Value>, pub error: Option<AdminError> }
  impl Response { pub fn success(id: Value, instance: InstanceId, result: Value) -> Response;
                  pub fn failure(id: Value, instance: Option<InstanceId>, error: AdminError) -> Response }
  pub struct AdminError { pub code: ErrorCode, pub message: String, pub retryable: bool, pub supported: Option<Vec<u32>> }
  impl AdminError { pub fn new(code: ErrorCode, message: impl Into<String>) -> AdminError;
                    pub fn unsupported_version(message: impl Into<String>) -> AdminError }
  pub enum ErrorCode { BadRequest, UnsupportedVersion, UnknownVerb, RequestTooLarge, Forbidden,
                       Busy, ShuttingDown, Unavailable, Internal, Unknown }
  impl ErrorCode { pub fn as_str(self) -> &'static str; pub fn retryable(self) -> bool }
  pub struct HeldSegment { pub segment: String, pub processed_records: u64 }
  pub struct PauseResult { pub generation: u64, pub pause_requested: bool, pub paused: bool, pub held: bool,
                           pub paused_since_ms: Option<u64>, pub held_segment: Option<HeldSegment>,
                           pub warnings: Vec<String>, pub note: String }
  pub struct ResumeResult { pub generation: u64, pub pause_requested: bool, pub was_paused: bool,
                            pub paused_for_secs: Option<f64>, pub resumed: bool }
  pub struct RetryStrandedResult { pub ticket: u64, pub completed: bool, pub coalesced: bool,
                                   pub requeued: Option<u64>, pub already_queued: Option<u64>,
                                   pub backoff_cut: bool, pub scan_error: Option<String>,
                                   pub paused: bool, pub note: String }
  pub enum ReloadOutcomeWire { Applied, Noop, Failed, Unknown }            // twin of PR 1's crate::ReloadOutcomeCode
  pub enum ReloadItemOutcomeWire { Reloaded, Failed, NotConfigured, Unknown } // twin of PR 1's crate::ReloadItemOutcome
  pub struct ReloadItemWire { pub item: String, pub outcome: ReloadItemOutcomeWire, pub detail: Option<String> }
  pub struct NotReloaded { pub item: String, pub reason_code: String, pub reason: String }
  pub struct ReloadResult { pub trigger: String, pub at_unix_ms: u64, pub outcome: ReloadOutcomeWire,
                            pub items: Vec<ReloadItemWire>, pub not_reloaded: Vec<NotReloaded> }
  pub fn default_socket_path(wab_dir: &std::path::Path) -> Option<std::path::PathBuf>;
  pub fn lexical_normalize(p: &std::path::Path) -> std::path::PathBuf;
  ```
  The wire-only reload types carry a `Wire` suffix so they never share a name with PR 1's crate-root `ReloadItem` (an enum), `ReloadItemOutcome` and `ReloadOutcomeCode`: a consumer that glob-imports both `weir_status::*` and `weir_status::admin::*` (weir-ctl, PR 7) gets no ambiguity. They keep a `#[serde(other)] Unknown` that PR 1's closed sets do not have; a test pins their wire names to PR 1's.

- [ ] **Step 1: Write the failing test**

  In `crates/weir-status/src/lib.rs`, add `pub mod admin;` now, so the test module below is compiled. Then append to the new `crates/weir-status/src/admin.rs` (the file holds only the test module at this point, plus `use super::*;` targets that do not exist yet):

  ```rust
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
              vec!["status", "reload", "drain.pause", "drain.resume", "drain.retry_stranded"]
          );
          assert_eq!(
              Verb::names(),
              crate::ADMIN_VERBS.to_vec(),
              "the status document's admin.verbs (PR 1) and the protocol name the same verbs"
          );
          assert_eq!(Verb::parse("drain_pause"), None, "the wire spelling is dotted");
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
          assert_eq!(default_socket_path(Path::new("wab")), None, "relative: no parent");
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
          assert_eq!(s, r#"{"v":1,"id":"a","verb":"drain.pause","args":{"wait_ms":0}}"#);
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
          assert!(v["items"][0].get("detail").is_none(), "None detail is omitted");
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
              assert_eq!(serde_json::to_value(r).unwrap(), serde_json::to_value(twin).unwrap(), "{r:?}");
          }
          for &r in crate::ReloadOutcomeCode::ALL {
              let twin = match r {
                  crate::ReloadOutcomeCode::Applied => ReloadOutcomeWire::Applied,
                  crate::ReloadOutcomeCode::Noop => ReloadOutcomeWire::Noop,
                  crate::ReloadOutcomeCode::Failed => ReloadOutcomeWire::Failed,
              };
              assert_eq!(serde_json::to_value(r).unwrap(), serde_json::to_value(twin).unwrap(), "{r:?}");
          }
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-status admin`
  Expected: compile failure, `error[E0412]: cannot find type `InstanceId` in this scope` (and the other items).

- [ ] **Step 3: Implement**

  `crates/weir-status/src/admin.rs` (above the test module):

  ```rust
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
  pub const WARN_UNBOUNDED_WAB: &str =
      "wab_max_bytes is 0: the WAB grows without bound while paused";

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
          Request { v: PROTOCOL_VERSION, id, verb: verb.as_str().to_string(), args }
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
          Response { v: PROTOCOL_VERSION, id, instance, ok: false, result: None, error: Some(error) }
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
          AdminError { code, message: message.into(), retryable: code.retryable(), supported: None }
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
          matches!(self, ErrorCode::Busy | ErrorCode::Unavailable | ErrorCode::Internal)
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
      p.components().filter(|c| !matches!(c, Component::CurDir)).collect()
  }
  ```

  `crates/weir-status/Cargo.toml`: move `serde_json = "1"` from `[dev-dependencies]` to `[dependencies]` (the non-test code above names `serde_json::Value`).

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-status admin`
  Expected: `test result: ok. 11 passed`.

- [ ] **Step 5: Refresh the fuzz lock and check the chaos lock**

  Run: `(cd fuzz && cargo metadata --format-version 1 >/dev/null) && git diff fuzz/Cargo.lock`
  Expected: weir-status's `dependencies` gains `"serde_json"` and nothing else changes.

  Run: `(cd chaos && cargo metadata --format-version 1 >/dev/null) && git diff --exit-code chaos/Cargo.lock`
  Expected: exit 0, no diff (chaos depends only on weir-client and weir-core).

- [ ] **Step 6: Commit**

  `git add crates/weir-status fuzz/Cargo.lock && git commit -m "feat(status): admin socket protocol v1 types and the derived socket path"`

---

### Task 6.2: config keys `admin_socket_enabled` and `admin_socket_path`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/config/mod.rs` — `PartialConfig` (lines ~221-299: two fields), `Config` (lines ~359-640: two fields), `Config::from_layers` (after the `socket_path`/`wab_dir` block at ~739-744: merge + validate; the `Ok(Config { .. })` literal at ~1220: two fields), new `AdminSocketSetting`, `AdminPathProblem`, `admin_path_problem`, `Config::admin_socket`, tests.
- Modify: `crates/weir-server/src/config/env.rs` (the `PartialConfig { .. }` literal at lines 6-70)
- Modify: `crates/weir-server/src/config/cli.rs` (`HELP` at line 5 onward, the `PartialConfig` literal at ~161)
- Modify: `crates/weir-server/src/config/file.rs` (`RawServer` at 14-95, `BASE_SERVER_KEYS` at 99+, the mapping at ~225)
- Modify: `crates/weir-server/src/status/config_view.rs` (PR 1's exhaustive destructure of `Config`)
- Test: `crates/weir-server/src/config/mod.rs`, `config/file.rs`, `config/cli.rs` test modules

**Interfaces:**
- Consumes: `weir_status::admin::{default_socket_path, lexical_normalize, MAX_SOCKET_PATH_BYTES}` (Task 6.1); PR 1's `ConfigView::from(&Config)` exhaustive destructure and its `Admin` classification.
- Produces:
  ```rust
  // config/mod.rs
  pub struct Config { /* … */ pub admin_socket_enabled: bool, pub admin_socket_path: Option<PathBuf>, /* … */ }
  pub(crate) struct PartialConfig { /* … */ pub admin_socket_enabled: Option<bool>, pub admin_socket_path: Option<PathBuf> }
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) enum AdminSocketSetting {
      Disabled,
      Explicit(PathBuf),
      Derived(PathBuf),
      DerivedUnusable { reason_code: &'static str, path: Option<PathBuf> },
  }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum AdminPathProblem { TooLong, UnderWabDir, SameAsSocketPath }
  impl AdminPathProblem { pub(crate) fn reason_code(self) -> &'static str }
  pub(crate) fn admin_path_problem(p: &Path, wab_dir: &Path, socket_path: &Path) -> Option<AdminPathProblem>
  impl Config { pub(crate) fn admin_socket(&self) -> AdminSocketSetting }
  ```
  Env `WEIR_ADMIN_SOCKET_ENABLED`, `WEIR_ADMIN_SOCKET_PATH`; CLI `--admin-socket-path` (no CLI for the bool, spec §13); TOML `admin_socket_enabled`, `admin_socket_path`.

- [ ] **Step 1: Write the failing test**

  In `config/mod.rs` `mod tests` (uses the existing `tmp_dir` and `layers_with_wab` helpers):

  ```rust
  #[test]
  fn admin_socket_defaults_to_enabled_and_derived() {
      let dir = tmp_dir("admin_default");
      let c = layers_with_wab(dir.clone()).unwrap();
      assert!(c.admin_socket_enabled);
      assert_eq!(c.admin_socket_path, None);
      let want = weir_status::admin::default_socket_path(&c.wab_dir).unwrap();
      assert_eq!(c.admin_socket(), AdminSocketSetting::Derived(want));
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn admin_socket_can_be_disabled_from_the_file() {
      let dir = tmp_dir("admin_disabled");
      let c = Config::from_layers(
          PartialConfig::empty(),
          PartialConfig::empty(),
          PartialConfig {
              wab_dir: Some(dir.clone()),
              admin_socket_enabled: Some(false),
              admin_socket_path: Some(PathBuf::from("/run/weir-admin/a.sock")),
              ..PartialConfig::empty()
          },
      )
      .unwrap();
      assert_eq!(c.admin_socket(), AdminSocketSetting::Disabled);
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn an_explicit_admin_path_is_used_verbatim() {
      let dir = tmp_dir("admin_explicit");
      let c = Config::from_layers(
          PartialConfig {
              admin_socket_path: Some(PathBuf::from("/run/weir-admin/a.sock")),
              ..PartialConfig::empty()
          },
          PartialConfig::empty(),
          PartialConfig { wab_dir: Some(dir.clone()), ..PartialConfig::empty() },
      )
      .unwrap();
      assert_eq!(
          c.admin_socket(),
          AdminSocketSetting::Explicit(PathBuf::from("/run/weir-admin/a.sock"))
      );
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn invalid_explicit_admin_paths_are_rejected() {
      let dir = tmp_dir("admin_invalid");
      // wab_dir is canonicalized by `validate_path` (/var -> /private/var on
      // macOS), so the "inside wab_dir" case must use the canonical form.
      let under_wab = std::fs::canonicalize(&dir).unwrap().join("admin.sock");
      let long = PathBuf::from(format!("/{}", "a".repeat(103)));
      let cases: Vec<(PathBuf, &str)> = vec![
          (PathBuf::from("relative.sock"), "not absolute"),
          (PathBuf::from("/run/../weir/a.sock"), "'..'"),
          (long, "103"),
          (under_wab, "inside wab_dir"),
          (PathBuf::from("/run/weir//./weir.sock"), "same path as socket_path"),
      ];
      for (p, needle) in cases {
          let err = Config::from_layers(
              PartialConfig {
                  admin_socket_path: Some(p.clone()),
                  ..PartialConfig::empty()
              },
              PartialConfig::empty(),
              PartialConfig { wab_dir: Some(dir.clone()), ..PartialConfig::empty() },
          )
          .expect_err(&format!("{} must be rejected", p.display()))
          .to_string();
          assert!(err.contains("admin_socket_path"), "{err}");
          assert!(err.contains(needle), "{}: {err}", p.display());
      }
      fs::remove_dir_all(dir).ok();
  }

  /// A derived default that breaks a static rule disables admin; it never
  /// fails startup (spec §6 step 7).
  #[test]
  fn a_derived_default_that_collides_is_unusable_not_fatal() {
      let dir = tmp_dir("adm_dc"); // short: the derived path must stay <= 103 bytes on macOS
      let canonical = std::fs::canonicalize(&dir).unwrap();
      let derived = weir_status::admin::default_socket_path(&canonical).unwrap();
      let c = Config::from_layers(
          PartialConfig { socket_path: Some(derived.clone()), ..PartialConfig::empty() },
          PartialConfig::empty(),
          PartialConfig { wab_dir: Some(dir.clone()), ..PartialConfig::empty() },
      )
      .expect("the producer socket_path itself is valid");
      assert_eq!(
          c.admin_socket(),
          AdminSocketSetting::DerivedUnusable {
              reason_code: "collides_with_socket_path",
              path: Some(derived),
          }
      );
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn admin_path_problem_table() {
      let wab = Path::new("/var/lib/weir/wab");
      let sock = Path::new("/run/weir/weir.sock");
      assert_eq!(admin_path_problem(Path::new("/var/lib/weir/wab.admin.sock"), wab, sock), None);
      assert_eq!(
          admin_path_problem(Path::new("/var/lib/weir/wab/x.sock"), wab, sock),
          Some(AdminPathProblem::UnderWabDir)
      );
      assert_eq!(
          admin_path_problem(Path::new("/var/lib/weir/wab2.sock"), wab, sock),
          None,
          "component-wise, not string prefix"
      );
      assert_eq!(
          admin_path_problem(Path::new("/run/weir/./weir.sock"), wab, sock),
          Some(AdminPathProblem::SameAsSocketPath)
      );
      assert_eq!(AdminPathProblem::TooLong.reason_code(), "path_too_long");
  }
  ```

  In `config/file.rs` `mod tests`:

  ```rust
  #[test]
  fn admin_socket_keys_are_known() {
      let dir = crate::testutil::scratch_dir("filecfg_admin");
      crate::testutil::mkdir_p(&dir);
      let path = dir.join("weir.toml");
      std::fs::write(
          &path,
          "[server]\nadmin_socket_enabled = false\nadmin_socket_path = \"/run/weir-admin/a.sock\"\n",
      )
      .unwrap();
      let (partial, warnings) = read(&path).unwrap().expect("the file exists");
      assert_eq!(partial.admin_socket_enabled, Some(false));
      assert_eq!(partial.admin_socket_path, Some(PathBuf::from("/run/weir-admin/a.sock")));
      assert!(
          warnings.iter().all(|w| !w.contains("admin_socket")),
          "admin keys must not be reported as unknown: {warnings:?}"
      );
      std::fs::remove_file(&path).ok();
  }
  ```

  In `config/cli.rs` `mod tests`:

  ```rust
  #[test]
  fn admin_socket_path_flag_is_parsed() {
      let (partial, _) = parse_from(args(&["--admin-socket-path", "/run/weir-admin/a.sock"])).unwrap();
      assert_eq!(partial.admin_socket_path, Some(PathBuf::from("/run/weir-admin/a.sock")));
      assert!(HELP.contains("--admin-socket-path"), "the flag must be in --help");
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server config:: -- --test-threads=1`
  Expected: compile failure, `struct `PartialConfig` has no field named `admin_socket_enabled``, `no method named `admin_socket` found for struct `Config``.

- [ ] **Step 3: Implement**

  `config/mod.rs`, `PartialConfig` (after `peer_uid_check`):
  ```rust
      pub admin_socket_enabled: Option<bool>,
      pub admin_socket_path: Option<PathBuf>,
  ```

  `Config` (after `peer_uid_check`):
  ```rust
      /// Serve the admin Unix socket (Foundations spec §11). Default `true`.
      pub admin_socket_enabled: bool,
      /// The operator-set admin socket path, already validated. `None` means
      /// the path derived from `wab_dir`; resolve with [`Config::admin_socket`].
      pub admin_socket_path: Option<PathBuf>,
  ```

  New items above `impl Config`:
  ```rust
  /// Where (and whether) the admin socket binds. Resolved once at startup.
  // Unconditional: the tests compare it only through the derived PartialEq,
  // which dead-code analysis ignores, so its fields are unread in the test
  // target too until `admin::start` destructures it.
  #[allow(dead_code, reason = "admin::start (Task 6.14) is the caller; Task 6.14 removes this")]
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) enum AdminSocketSetting {
      /// `admin_socket_enabled = false`.
      Disabled,
      /// `admin_socket_path` was set: any bind failure is fatal.
      Explicit(PathBuf),
      /// `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`: a bind failure
      /// disables admin with `admin_unavailable` instead.
      Derived(PathBuf),
      /// The derived default breaks a static rule (`reason_code` is one of
      /// `no_parent`, `path_too_long`, `under_wab_dir`,
      /// `collides_with_socket_path`).
      DerivedUnusable { reason_code: &'static str, path: Option<PathBuf> },
  }

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum AdminPathProblem {
      TooLong,
      UnderWabDir,
      SameAsSocketPath,
  }

  impl AdminPathProblem {
      /// Public-safe reason code (spec §9.1: codes, never free text).
      #[cfg_attr(not(test), allow(dead_code, reason = "admin::start (Task 6.14) is the caller"))]
      pub(crate) fn reason_code(self) -> &'static str {
          match self {
              AdminPathProblem::TooLong => "path_too_long",
              AdminPathProblem::UnderWabDir => "under_wab_dir",
              AdminPathProblem::SameAsSocketPath => "collides_with_socket_path",
          }
      }

      fn message(self, p: &Path) -> String {
          match self {
              AdminPathProblem::TooLong => format!(
                  "'{}' is longer than {} bytes, the longest Unix socket path every \
                   supported platform can bind",
                  p.display(),
                  weir_status::admin::MAX_SOCKET_PATH_BYTES
              ),
              AdminPathProblem::UnderWabDir => format!(
                  "'{}' is inside wab_dir; put the admin socket outside the WAB",
                  p.display()
              ),
              AdminPathProblem::SameAsSocketPath => format!(
                  "'{}' is the same path as socket_path; the admin socket needs its own path",
                  p.display()
              ),
          }
      }
  }

  /// The static (no-I/O) rules every admin socket path must pass, explicit or
  /// derived (spec §11). The (dev, ino) collision check is done at bind time.
  pub(crate) fn admin_path_problem(
      p: &Path,
      wab_dir: &Path,
      socket_path: &Path,
  ) -> Option<AdminPathProblem> {
      use weir_status::admin::{MAX_SOCKET_PATH_BYTES, lexical_normalize};
      if p.as_os_str().as_encoded_bytes().len() > MAX_SOCKET_PATH_BYTES {
          return Some(AdminPathProblem::TooLong);
      }
      let n = lexical_normalize(p);
      if n.starts_with(lexical_normalize(wab_dir)) {
          return Some(AdminPathProblem::UnderWabDir);
      }
      if n == lexical_normalize(socket_path) {
          return Some(AdminPathProblem::SameAsSocketPath);
      }
      None
  }
  ```

  In `impl Config`:
  ```rust
      #[cfg_attr(not(test), allow(dead_code, reason = "admin::start (Task 6.14) is the caller"))]
      pub(crate) fn admin_socket(&self) -> AdminSocketSetting {
          if !self.admin_socket_enabled {
              return AdminSocketSetting::Disabled;
          }
          if let Some(p) = &self.admin_socket_path {
              return AdminSocketSetting::Explicit(p.clone());
          }
          let Some(p) = weir_status::admin::default_socket_path(&self.wab_dir) else {
              return AdminSocketSetting::DerivedUnusable { reason_code: "no_parent", path: None };
          };
          match admin_path_problem(&p, &self.wab_dir, &self.socket_path) {
              None => AdminSocketSetting::Derived(p),
              Some(problem) => AdminSocketSetting::DerivedUnusable {
                  reason_code: problem.reason_code(),
                  path: Some(p),
              },
          }
      }
  ```

  In `from_layers`, right after `let wab_dir = validate_path("wab_dir", &wab_dir_raw)?;`:
  ```rust
          let admin_socket_enabled = merge!(admin_socket_enabled).unwrap_or(true);
          let admin_socket_path = merge!(admin_socket_path);
          if let Some(p) = &admin_socket_path {
              validate_path_format("admin_socket_path", p)?;
              if let Some(problem) = admin_path_problem(p, &wab_dir, &socket_path) {
                  return Err(ConfigError::InvalidValue {
                      field: "admin_socket_path",
                      reason: problem.message(p),
                  });
              }
          }
  ```
  and `admin_socket_enabled, admin_socket_path,` in the `Ok(Config { .. })` literal after `peer_uid_check,`. The `validate_path_format` error text already says "is not absolute" and "contains '..' components" (config/mod.rs `validate_path_format_inner`), which the test needles match.

  `config/env.rs`, after `peer_uid_check`:
  ```rust
          admin_socket_enabled: env_bool("WEIR_ADMIN_SOCKET_ENABLED")?,
          admin_socket_path: env_path("WEIR_ADMIN_SOCKET_PATH")?,
  ```

  `config/cli.rs`: in the `PartialConfig` literal after `peer_uid_check`:
  ```rust
          admin_socket_enabled: None, // env/TOML only (spec §13)
          admin_socket_path: pargs
              .opt_value_from_str("--admin-socket-path")
              .map_err(pico_err)?,
  ```
  and in `HELP`, after the `--peer-uid-check` entry:
  ```text
      --admin-socket-path <path>               Admin Unix socket path (ABSOLUTE, <= 103 bytes)
                                                 [default: <wab_dir parent>/<wab_dir name>.admin.sock]
                                                 [env: WEIR_ADMIN_SOCKET_PATH]
  ```

  `config/file.rs`: `RawServer` gains `admin_socket_enabled: Option<bool>, admin_socket_path: Option<String>,`; `BASE_SERVER_KEYS` gains `"admin_socket_enabled", "admin_socket_path",`; the mapping gains `admin_socket_enabled: s.admin_socket_enabled, admin_socket_path: s.admin_socket_path.map(PathBuf::from),`.

  `status/config_view.rs` (PR 1): add `admin_socket_enabled, admin_socket_path,` to the `let Config { .. } = cfg;` destructure (it has no `..`, so the build fails until this is done) and classify both **`Admin`** with PR 1's `admin(..)` helper, as for `socket_path` (an absolute path is admin-only, spec §9.1; neither key joins the fixed public subset):
  ```rust
          v.admin("admin_socket_enabled", b(*admin_socket_enabled));
          v.admin("admin_socket_path", opt_s(admin_socket_path.as_ref().map(|p| p.display())));
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server config:: -- --test-threads=1`
  Expected: all config tests pass, including the 8 new ones (6 in `config/mod.rs`, 1 in `file.rs`, 1 in `cli.rs`); no other test changes.

- [ ] **Step 5: Commit**

  `git commit -am "feat(config): admin_socket_enabled and admin_socket_path"`

---

### Task 6.3: metrics `weir_admin_requests_total{verb,outcome}` and `weir_admin_socket_up`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs` (label types after `ConfigReloadOutcome` at ~296; two `Metrics` fields after `config_reloads` at ~489; two `reg!` calls after the `weir_config_reloads` registration at ~858-866 plus pre-creation; the struct literal at ~1004; `all_metric_names_present_in_output` at ~1166; the `weir_config_reloads` help text, which still says "admin is reserved")
- Modify: `docs/monitoring.md` (metric reference: two new rows; the `weir_config_reloads_total` row at line 751 no longer says `admin` is reserved)
- Modify: `crates/weir-server/tests/system.rs` — `metrics_all_families_registered` (line ~795): add `"weir_admin_requests"` and `"weir_admin_socket_up"` to its `expected` list (it also asserts the count of `# HELP weir_` lines equals the list length, so it fails without them)
- Test: `crates/weir-server/src/metrics/mod.rs` tests; `crates/weir-server/tests/docs_drift.rs::every_registered_metric_is_in_the_monitoring_reference` (existing, unchanged); `crates/weir-server/tests/system.rs::metrics_all_families_registered`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  ```rust
  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct AdminRequestLabel { pub verb: AdminVerbLabel, pub outcome: AdminOutcomeLabel }
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
  pub enum AdminVerbLabel { Status, Reload, DrainPause, DrainResume, DrainRetryStranded, Connect, Invalid }
  impl AdminVerbLabel { pub fn as_str(self) -> &'static str }
  impl EncodeLabelValue for AdminVerbLabel
  #[allow(non_camel_case_types)] #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum AdminOutcomeLabel { ok, error, refused, busy }
  // Metrics fields
  pub admin_requests: Family<AdminRequestLabel, Counter<u64, AtomicU64>>,
  pub admin_socket_up: Gauge<f64, AtomicU64>,
  ```
  **Ownership (resolved by the integrator):** PR 1 Task 1.18 does not register these two families; it assigns them to this PR. This task registers them, with their monitoring.md rows.

- [ ] **Step 1: Write the failing test**

  In `metrics/mod.rs` tests:
  ```rust
  /// spec §11/§13: the verb label is bounded and spelled exactly as on the
  /// wire, and every meaningful series exists from the first scrape.
  #[test]
  fn admin_series_are_pre_created_with_wire_verb_names() {
      let (m, reg) = Metrics::new();
      let out = encode_to_string(&reg);
      for verb in ["status", "reload", "drain.pause", "drain.resume", "drain.retry_stranded"] {
          for outcome in ["ok", "error"] {
              let s = format!("weir_admin_requests_total{{verb=\"{verb}\",outcome=\"{outcome}\"}} 0");
              assert!(out.contains(&s), "missing {s}\n{out}");
          }
      }
      for s in [
          "weir_admin_requests_total{verb=\"_connect\",outcome=\"refused\"} 0",
          "weir_admin_requests_total{verb=\"_connect\",outcome=\"busy\"} 0",
          "weir_admin_requests_total{verb=\"_invalid\",outcome=\"error\"} 0",
          "weir_admin_socket_up 0",
      ] {
          assert!(out.contains(s), "missing {s}\n{out}");
      }
      m.admin_socket_up.set(1.0);
      assert!(encode_to_string(&reg).contains("weir_admin_socket_up 1"));
      let ok_status = AdminRequestLabel { verb: AdminVerbLabel::Status, outcome: AdminOutcomeLabel::ok };
      m.admin_requests.get_or_create(&ok_status).inc();
      assert!(encode_to_string(&reg).contains("weir_admin_requests_total{verb=\"status\",outcome=\"ok\"} 1"));
  }
  ```
  and add `"weir_admin_requests"` and `"weir_admin_socket_up"` to the `expected` array of `all_metric_names_present_in_output`.

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server metrics:: -- --test-threads=1`
  Expected: compile failure, `no field `admin_socket_up` on type `Metrics``.

- [ ] **Step 3: Implement**

  Label types (after `ConfigReloadOutcome`):
  ```rust
  /// Labels for `weir_admin_requests_total` (Foundations spec §11).
  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct AdminRequestLabel {
      pub verb: AdminVerbLabel,
      pub outcome: AdminOutcomeLabel,
  }

  /// The admin verb, spelled exactly as on the wire, plus `_connect` (a
  /// connection refused before any request) and `_invalid` (a line that is
  /// not a valid request). Hand-encoded because the wire spellings contain
  /// dots, which a derived `EncodeLabelValue` cannot produce.
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
  pub enum AdminVerbLabel {
      Status,
      Reload,
      DrainPause,
      DrainResume,
      DrainRetryStranded,
      Connect,
      Invalid,
  }

  impl AdminVerbLabel {
      pub fn as_str(self) -> &'static str {
          match self {
              AdminVerbLabel::Status => "status",
              AdminVerbLabel::Reload => "reload",
              AdminVerbLabel::DrainPause => "drain.pause",
              AdminVerbLabel::DrainResume => "drain.resume",
              AdminVerbLabel::DrainRetryStranded => "drain.retry_stranded",
              AdminVerbLabel::Connect => "_connect",
              AdminVerbLabel::Invalid => "_invalid",
          }
      }
  }

  impl prometheus_client::encoding::EncodeLabelValue for AdminVerbLabel {
      fn encode(
          &self,
          encoder: &mut prometheus_client::encoding::LabelValueEncoder,
      ) -> Result<(), std::fmt::Error> {
          use std::fmt::Write;
          encoder.write_str(self.as_str())
      }
  }

  /// `ok` (the verb answered `ok:true`), `error` (an error response),
  /// `refused` (`_connect`: the peer is neither the daemon's uid nor root, or
  /// has no credentials), `busy` (`_connect`: the 4-connection cap).
  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum AdminOutcomeLabel {
      ok,
      error,
      refused,
      busy,
  }
  ```
  (prometheus-client 0.22.3 implements `std::fmt::Write` for `LabelValueEncoder` (encoding.rs:459), which is what the `write_str` relies on.)

  Fields (after `config_reloads`):
  ```rust
      // ── Admin socket (Foundations spec §11) ───────────────────────────────
      /// `weir_admin_requests_total{verb,outcome}`.
      #[cfg_attr(not(test), allow(dead_code, reason = "read by the admin module (Tasks 6.12/6.13); Task 6.14 removes this"))]
      pub admin_requests: Family<AdminRequestLabel, Counter<u64, AtomicU64>>,
      /// 1 while the admin socket is bound and served, else 0.
      #[cfg_attr(not(test), allow(dead_code, reason = "read by the admin module (Tasks 6.12/6.13); Task 6.14 removes this"))]
      pub admin_socket_up: Gauge<f64, AtomicU64>,
  ```

  Registration (after `config_reloads`), with pre-creation of the 13 meaningful series:
  ```rust
          let admin_requests = reg!(
              Family::<AdminRequestLabel, Counter<u64, AtomicU64>>::default(),
              "weir_admin_requests",
              "Admin socket requests, by verb (status, reload, drain.pause, drain.resume, \
               drain.retry_stranded; _connect for a connection refused before any request; \
               _invalid for a line that is not a valid request) and outcome (ok, error, \
               refused, busy)"
          );
          for verb in [
              AdminVerbLabel::Status,
              AdminVerbLabel::Reload,
              AdminVerbLabel::DrainPause,
              AdminVerbLabel::DrainResume,
              AdminVerbLabel::DrainRetryStranded,
          ] {
              for outcome in [AdminOutcomeLabel::ok, AdminOutcomeLabel::error] {
                  admin_requests.get_or_create(&AdminRequestLabel { verb, outcome });
              }
          }
          for outcome in [AdminOutcomeLabel::refused, AdminOutcomeLabel::busy] {
              admin_requests.get_or_create(&AdminRequestLabel { verb: AdminVerbLabel::Connect, outcome });
          }
          admin_requests.get_or_create(&AdminRequestLabel {
              verb: AdminVerbLabel::Invalid,
              outcome: AdminOutcomeLabel::error,
          });
          let admin_socket_up = reg!(
              Gauge::<f64, AtomicU64>::default(),
              "weir_admin_socket_up",
              "1 while the admin Unix socket is bound and served; 0 when it is disabled, \
               unavailable, or closed at exit"
          );
  ```
  Add both to the `Metrics { .. }` literal. Change the `weir_config_reloads` help from "(sighup; admin is reserved)" to "(sighup, or admin for the admin socket's reload verb)".

  `docs/monitoring.md`, metric reference, after the `weir_config_reloads_total` row (and in that row replace "`admin` is reserved; nothing sends it yet" with "or `admin` (the admin socket's `reload` verb, which runs the same reload)"):
  ```markdown
  | `weir_admin_requests_total{verb,outcome}` | counter | Requests on the admin Unix socket. `verb` is the wire verb (`status`, `reload`, `drain.pause`, `drain.resume`, `drain.retry_stranded`), `_connect` for a connection refused before any request, or `_invalid` for a line that is not a valid request. `outcome` is `ok`, `error` (the exact error code is in the daemon log), `refused` (the peer is neither the daemon's uid nor root — **investigate every one**: the socket is mode 0600 in a private directory, so a refusal means something loosened that) or `busy` (more than 4 concurrent admin connections). |
  | `weir_admin_socket_up` | gauge | 1 while the admin socket is bound and served. 0 when it is disabled (`admin_socket_enabled = false`), unavailable (the derived default path could not be bound; the `admin_unavailable` condition and an ERROR at startup say why), or closed during exit. |
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server metrics:: -- --test-threads=1`, then `cargo test -p weir-server --test docs_drift`, then `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1`
  Expected: all green; `every_registered_metric_is_in_the_monitoring_reference` passes with the two new rows, and the system families test counts two more `# HELP` lines.

- [ ] **Step 5: Commit**

  `git commit -am "feat(metrics): admin request counter and admin socket gauge"`

---

### Task 6.4: `bind_hardened_with(path, ParentPolicy::RequirePrivate)`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs` — `mod peer;` (line 20) becomes `pub(crate) mod peer;`; `ACCEPT_BACKOFF_ON_EXHAUSTION` (line ~320) and `is_accept_resource_exhaustion` (~327) become `pub(crate)`; `bind_hardened` (unix, ~421-531) splits into `bind_hardened` + `bind_hardened_with`; new `ParentPolicy`, `ParentProblem`, `parent_private_verdict`, `refuse_unless_parent_private`, `InsecureParent`, `is_insecure_parent` next to `warn_if_parent_world_writable` (~1103).
- Test: `crates/weir-server/src/socket/mod.rs` `mod tests`

**Interfaces:**
- Consumes: the safety floor's `bind_hardened`, `split_parent_basename`, `open_parent_dirfd`, `OwnedDirFd`, `is_group_or_other_writable`, `remove_stale_socket_at_startup` (unchanged).
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum ParentPolicy { Warn, RequirePrivate }
  pub(crate) fn bind_hardened_with(path: &Path, policy: ParentPolicy) -> io::Result<BoundSocket>   // #[cfg(unix)]
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum ParentProblem { GroupOrOtherWritable, ForeignOwner }
  pub(crate) fn parent_private_verdict(mode: libc::mode_t, owner: libc::uid_t, euid: libc::uid_t) -> Result<(), ParentProblem>
  pub(crate) fn is_insecure_parent(e: &io::Error) -> bool
  pub(crate) mod peer;                       // peer_uid_of now reachable from crate::admin
  pub(crate) const ACCEPT_BACKOFF_ON_EXHAUSTION: Duration;
  pub(crate) fn is_accept_resource_exhaustion(e: &io::Error) -> bool;
  ```
  `bind_hardened(path)` keeps its signature and behaviour (it is `bind_hardened_with(path, ParentPolicy::Warn)`).

- [ ] **Step 1: Write the failing test**

  ```rust
  /// Foundations §11: the admin bind REFUSES a parent others can write to
  /// (the producer bind keeps warning), and leaves nothing behind.
  /// A tokio test: the binds go through tokio's `UnixListener::bind`, which
  /// panics outside a runtime with IO enabled.
  #[tokio::test]
  async fn require_private_refuses_a_group_or_world_writable_parent() {
      use std::os::unix::fs::PermissionsExt;
      let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
      let root = crate::testutil::scratch_dir("bind_private");
      for mode in [0o770, 0o707, 0o1777] {
          let dir = root.join(format!("m{mode:o}"));
          crate::testutil::mkdir_p(&dir);
          std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode)).unwrap();
          let path = dir.join("a.sock");
          let err = bind_hardened_with(&path, ParentPolicy::RequirePrivate)
              .expect_err("a shared parent must be refused");
          assert!(is_insecure_parent(&err), "{mode:o}: {err}");
          assert!(err.to_string().contains("group- or world-writable"), "{err}");
          assert!(!path.exists(), "a refused bind creates nothing");
          // The producer policy is unchanged: it warns and binds.
          let bound = bind_hardened(&path).expect("Warn policy still binds");
          remove_own_socket(&path, &bound);
      }
      let private = root.join("private");
      crate::testutil::mkdir_p(&private);
      let path = private.join("a.sock");
      let bound = bind_hardened_with(&path, ParentPolicy::RequirePrivate).expect("0700 parent binds");
      use std::os::unix::fs::MetadataExt;
      assert_eq!(std::fs::symlink_metadata(&path).unwrap().mode() & 0o777, 0o600);
      remove_own_socket(&path, &bound);
      let _ = std::fs::remove_dir_all(&root);
  }

  #[test]
  fn parent_private_verdict_table() {
      assert_eq!(parent_private_verdict(0o700, 1000, 1000), Ok(()));
      assert_eq!(parent_private_verdict(0o755, 1000, 1000), Ok(()));
      assert_eq!(parent_private_verdict(0o755, 0, 1000), Ok(()), "root-owned is fine");
      assert_eq!(
          parent_private_verdict(0o755, 1234, 1000),
          Err(ParentProblem::ForeignOwner),
          "another user can create entries in a directory they own"
      );
      assert_eq!(
          parent_private_verdict(0o775, 1000, 1000),
          Err(ParentProblem::GroupOrOtherWritable)
      );
      assert_eq!(
          parent_private_verdict(0o1777, 0, 1000),
          Err(ParentProblem::GroupOrOtherWritable),
          "the sticky bit does not stop a squatter creating the name"
      );
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server socket::tests::require_private -- --test-threads=1`
  Expected: compile failure, `cannot find function `bind_hardened_with` in this scope`.

- [ ] **Step 3: Implement**

  ```rust
  /// What a hardened bind does about a parent directory others can write to.
  #[cfg_attr(not(test), allow(dead_code, reason = "admin::bind_admin (Task 6.6) constructs RequirePrivate; Task 6.14 removes this"))]
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum ParentPolicy {
      /// The producer socket: WARN and bind. Operators legitimately put
      /// development sockets under `/tmp`.
      Warn,
      /// The admin socket (Foundations spec §11): refuse. Anyone who can
      /// create entries in the parent could squat the name and impersonate
      /// the control plane.
      RequirePrivate,
  }

  #[cfg(unix)]
  pub(crate) fn bind_hardened(path: &Path) -> io::Result<BoundSocket> {
      bind_hardened_with(path, ParentPolicy::Warn)
  }

  #[cfg(unix)]
  pub(crate) fn bind_hardened_with(path: &Path, policy: ParentPolicy) -> io::Result<BoundSocket> {
      let (parent, basename) = split_parent_basename(path)?;
      let dirfd = open_parent_dirfd(parent)?;
      let _dirfd_guard = OwnedDirFd(dirfd);
      match policy {
          ParentPolicy::Warn => warn_if_parent_world_writable(dirfd, parent),
          ParentPolicy::RequirePrivate => refuse_unless_parent_private(dirfd, parent)?,
      }
      // … the rest of today's body, unchanged, from
      // `remove_stale_socket_at_startup(dirfd, basename, path)?;` to the
      // `Ok(BoundSocket { .. })`.
  }

  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum ParentProblem {
      GroupOrOtherWritable,
      ForeignOwner,
  }

  /// A parent is private when only the daemon user (or root) can create
  /// entries in it: not group- or world-writable, and owned by the daemon's
  /// euid or by root.
  #[cfg(unix)]
  pub(crate) fn parent_private_verdict(
      mode: libc::mode_t,
      owner: libc::uid_t,
      euid: libc::uid_t,
  ) -> Result<(), ParentProblem> {
      if is_group_or_other_writable(mode) {
          return Err(ParentProblem::GroupOrOtherWritable);
      }
      if owner != euid && owner != 0 {
          return Err(ParentProblem::ForeignOwner);
      }
      Ok(())
  }

  /// The refusal's marker, so a caller can tell it from an `EACCES` on open.
  #[derive(Debug)]
  pub(crate) struct InsecureParent(String);

  impl std::fmt::Display for InsecureParent {
      fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
          f.write_str(&self.0)
      }
  }

  impl std::error::Error for InsecureParent {}

  #[cfg_attr(not(test), allow(dead_code, reason = "admin::bind_admin (Task 6.6) is the caller; Task 6.14 removes this"))]
  pub(crate) fn is_insecure_parent(e: &io::Error) -> bool {
      e.get_ref().is_some_and(|inner| inner.is::<InsecureParent>())
  }

  /// `fstat`s the pinned parent dirfd (no re-resolution by path, as in
  /// `warn_if_parent_world_writable`) and refuses a non-private parent.
  #[cfg(unix)]
  fn refuse_unless_parent_private(dirfd: libc::c_int, parent: &Path) -> io::Result<()> {
      let mut st: libc::stat = unsafe { std::mem::zeroed() };
      // Safety: dirfd is a valid directory fd held by the caller; &mut st is a
      // writable libc::stat.
      if unsafe { libc::fstat(dirfd, &mut st) } != 0 {
          return Err(io::Error::last_os_error());
      }
      // Safety: geteuid has no preconditions.
      let euid = unsafe { libc::geteuid() };
      parent_private_verdict(st.st_mode, st.st_uid, euid).map_err(|why| {
          let what = match why {
              ParentProblem::GroupOrOtherWritable => "it is group- or world-writable",
              ParentProblem::ForeignOwner => "it is owned by another non-root user",
          };
          io::Error::new(
              io::ErrorKind::PermissionDenied,
              InsecureParent(format!(
                  "refusing to bind a control socket in '{}' (mode {:#o}, owner uid {}): {what}. \
                   Anyone who can create entries there could put their own socket at this path \
                   and impersonate the daemon's admin interface. Use a directory writable only \
                   by the daemon user.",
                  parent.display(),
                  st.st_mode & 0o7777,
                  st.st_uid
              )),
          )
      })
  }
  ```
  Visibility edits: `pub(crate) mod peer;`, `pub(crate) const ACCEPT_BACKOFF_ON_EXHAUSTION`, `pub(crate) fn is_accept_resource_exhaustion`. The `#[cfg(not(unix))] bind_hardened` stub is unchanged (admin is `#[cfg(unix)]`).

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1`
  Expected: every existing socket test plus the two new ones pass (the producer bind path is byte-for-byte the old one under `ParentPolicy::Warn`).

- [ ] **Step 5: Commit**

  `git commit -am "feat(socket): a bind that refuses a parent directory others can write to"`

---

### Task 6.5: `admin/identity.rs` — the peer predicate and the instance identity

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Create: `crates/weir-server/src/admin/mod.rs` (module skeleton: `pub(crate) mod identity;` only, grows in later tasks)
- Create: `crates/weir-server/src/admin/identity.rs`
- Modify: `crates/weir-server/src/main.rs` (add `#[cfg(unix)] mod admin;` beside `mod socket;`)
- Test: `crates/weir-server/src/admin/identity.rs`

**Interfaces:**
- Consumes: `weir_status::admin::InstanceId` (Task 6.1).
- Produces:
  ```rust
  pub(crate) fn peer_allowed(peer_uid: u32, daemon_euid: u32) -> bool
  #[derive(Debug, PartialEq, Eq)] pub(crate) enum PeerRefusal { WrongUid(u32), NoCredentials }
  pub(crate) fn authorize_peer(lookup: std::io::Result<u32>, daemon_euid: u32) -> Result<u32, PeerRefusal>
  pub(crate) fn instance_id(wab_dir: &Path, socket_path: &Path, started_at_ms: u64) -> std::io::Result<InstanceId>
  ```

- [ ] **Step 1: Write the failing test**

  Declare the module first, so the test is compiled: `main.rs` gains `#[cfg(unix)] mod admin;` directly under `mod socket;` (with the same cfg comment), and the new `admin/mod.rs` is:
  ```rust
  //! The admin Unix socket (Foundations spec §5.9, §11): a JSON-lines control
  //! channel for the daemon's own uid and root, separate from the producer
  //! socket and never on the producer-shared directory.

  // Unreachable from `main` until Task 6.14 wires it in. Unconditional, so it
  // also covers the test target that `--all-targets` lints; Task 6.14 deletes
  // this line and clippy must then pass without it.
  #![allow(dead_code, reason = "wired into main by Task 6.14")]

  pub(crate) mod identity;
  ```
  Then `admin/identity.rs` holds only this test module:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      /// Owner decision (spec §20): daemon euid or root, nobody else. This is
      /// independent of `peer_uid_check`, which governs producers only.
      #[test]
      fn the_peer_predicate_is_daemon_uid_or_root() {
          assert!(peer_allowed(1000, 1000));
          assert!(peer_allowed(0, 1000));
          assert!(!peer_allowed(1001, 1000));
          assert!(!peer_allowed(10001, 1000), "a producer uid is not an admin");
          assert!(peer_allowed(0, 0));
      }

      #[test]
      fn authorization_fails_closed() {
          assert_eq!(authorize_peer(Ok(1000), 1000), Ok(1000));
          assert_eq!(authorize_peer(Ok(0), 1000), Ok(0));
          assert_eq!(authorize_peer(Ok(7), 1000), Err(PeerRefusal::WrongUid(7)));
          let no_creds = Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "x"));
          assert_eq!(authorize_peer(no_creds, 1000), Err(PeerRefusal::NoCredentials));
      }

      #[test]
      fn instance_carries_the_wab_dir_inode() {
          use std::os::unix::fs::MetadataExt;
          let dir = crate::testutil::scratch_dir("admin_identity");
          let id = instance_id(&dir, Path::new("/run/weir/weir.sock"), 123).unwrap();
          let m = std::fs::metadata(&dir).unwrap();
          assert_eq!((id.wab_dir_dev, id.wab_dir_ino), (m.dev(), m.ino()));
          assert_eq!(id.pid, std::process::id());
          assert_eq!(id.started_at_ms, 123);
          assert_eq!(id.wab_dir, dir.display().to_string());
          assert_eq!(id.socket_path, "/run/weir/weir.sock");
          let _ = std::fs::remove_dir_all(&dir);
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::identity -- --test-threads=1`
  Expected: compile failure, `error[E0425]: cannot find function `peer_allowed` in this scope` (and `authorize_peer`, `instance_id`).

- [ ] **Step 3: Implement**

  `admin/identity.rs` (above the test module):
  ```rust
  //! Who may use the admin socket, and who the daemon says it is.

  use std::{io, path::Path};

  use weir_status::admin::InstanceId;

  /// The admin peer predicate (owner decision, spec §20): the daemon's own
  /// effective uid, or root. A deliberate divergence from the producer rule
  /// (root is not implicitly a producer), documented in threat-model.md.
  pub(crate) fn peer_allowed(peer_uid: u32, daemon_euid: u32) -> bool {
      peer_uid == daemon_euid || peer_uid == 0
  }

  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum PeerRefusal {
      WrongUid(u32),
      /// The platform or socket gave no peer credentials. Fails closed.
      NoCredentials,
  }

  /// Applies [`peer_allowed`] to a `peer_uid_of` result. Always on; does not
  /// read `peer_uid_check`.
  pub(crate) fn authorize_peer(lookup: io::Result<u32>, daemon_euid: u32) -> Result<u32, PeerRefusal> {
      match lookup {
          Ok(uid) if peer_allowed(uid, daemon_euid) => Ok(uid),
          Ok(uid) => Err(PeerRefusal::WrongUid(uid)),
          Err(_) => Err(PeerRefusal::NoCredentials),
      }
  }

  /// The identity stamped on every response to an authorized peer, so a
  /// client can check it acted on the daemon it meant (weir-ctl compares
  /// `wab_dir_dev/ino` with `stat(--wab-dir)`, spec §11).
  pub(crate) fn instance_id(
      wab_dir: &Path,
      socket_path: &Path,
      started_at_ms: u64,
  ) -> io::Result<InstanceId> {
      use std::os::unix::fs::MetadataExt;
      let m = std::fs::metadata(wab_dir)?;
      Ok(InstanceId {
          pid: std::process::id(),
          started_at_ms,
          wab_dir: wab_dir.display().to_string(),
          socket_path: socket_path.display().to_string(),
          wab_dir_dev: m.dev(),
          wab_dir_ino: m.ino(),
      })
  }
  ```
- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin::identity -- --test-threads=1`
  Expected: 3 passed.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): the peer predicate and the instance identity"`

---

### Task 6.6: `admin::bind_admin` — collision, parent and live-socket refusals with reason codes

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/mod.rs`
- Test: `crates/weir-server/src/admin/mod.rs` `mod tests`

**Interfaces:**
- Consumes: `socket::{bind_hardened_with, ParentPolicy, is_insecure_parent, validate_socket_path, BoundSocket, remove_own_socket}` (Task 6.4 + safety floor), PR 3's `socket::refuse_if_live(path: &Path, what: &str) -> io::Result<()>` (spec §5.9 "reuse `refuse_if_live`"), `weir_status::admin::lexical_normalize`.
- Produces:
  ```rust
  pub(crate) mod reason {
      pub(crate) const PARENT_INSECURE: &str = "parent_insecure";
      pub(crate) const IN_USE: &str = "in_use";
      pub(crate) const BIND_FAILED: &str = "bind_failed";
      pub(crate) const COLLIDES_WITH_SOCKET_PATH: &str = "collides_with_socket_path";
  }
  pub(crate) struct BoundAdmin { pub(crate) path: PathBuf, pub(crate) socket: BoundSocket }
  #[derive(Debug)] pub(crate) struct AdminBindError { pub(crate) reason_code: &'static str, pub(crate) source: io::Error }
  pub(crate) fn bind_admin(path: &Path, socket_path: &Path) -> Result<BoundAdmin, AdminBindError>
  pub(crate) fn names_same_file(admin: &Path, socket_path: &Path) -> bool
  ```
  Reason codes are `&'static str` constants from code only (log fields and error messages). The board carries PR 1's closed `weir_status::AdminReason` instead, mapped from these codes plus Task 6.2's `no_parent`, `path_too_long`, `under_wab_dir` by Task 6.14's `admin_reason`.

- [ ] **Step 1: Write the failing test**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use std::os::unix::fs::{MetadataExt, PermissionsExt};

      fn private_dir(label: &str) -> PathBuf {
          crate::testutil::scratch_dir(label)
      }

      // Every test here is a tokio test: `bind_admin` reaches tokio's
      // `UnixListener::bind`, which panics outside a runtime with IO enabled.
      // No umask lock: `umask_test_lock` is private to socket::tests, and the
      // bin tests run with --test-threads=1.
      #[tokio::test]
      async fn binds_mode_0600_in_a_private_parent() {
          let dir = private_dir("admin_bind_ok");
          let path = dir.join("wab.admin.sock");
          let b = bind_admin(&path, Path::new("/run/weir/weir.sock")).expect("binds");
          assert_eq!(std::fs::symlink_metadata(&path).unwrap().mode() & 0o777, 0o600);
          assert_eq!(b.path, path);
          socket::remove_own_socket(&b.path, &b.socket);
          let _ = std::fs::remove_dir_all(&dir);
      }

      #[tokio::test]
      async fn refuses_a_group_writable_parent() {
          let dir = private_dir("admin_bind_grp");
          std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o770)).unwrap();
          let path = dir.join("wab.admin.sock");
          let e = bind_admin(&path, Path::new("/run/weir/weir.sock")).err().expect("refused");
          assert_eq!(e.reason_code, reason::PARENT_INSECURE);
          assert!(!path.exists());
          let _ = std::fs::remove_dir_all(&dir);
      }

      /// A second daemon (or anything) already listening keeps its socket.
      #[tokio::test]
      async fn refuses_when_a_live_socket_listens() {
          let dir = private_dir("admin_bind_live");
          let path = dir.join("wab.admin.sock");
          let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
          let e = bind_admin(&path, Path::new("/run/weir/weir.sock")).err().expect("refused");
          assert_eq!(e.reason_code, reason::IN_USE, "{}", e.source);
          std::os::unix::net::UnixStream::connect(&path).expect("the live socket is untouched");
          drop(live);
          let _ = std::fs::remove_dir_all(&dir);
      }

      #[tokio::test]
      async fn replaces_a_stale_socket() {
          let dir = private_dir("admin_bind_stale");
          let path = dir.join("wab.admin.sock");
          drop(std::os::unix::net::UnixListener::bind(&path).unwrap()); // file stays, nobody listens
          let b = bind_admin(&path, Path::new("/run/weir/weir.sock")).expect("stale is replaced");
          socket::remove_own_socket(&b.path, &b.socket);
          let _ = std::fs::remove_dir_all(&dir);
      }

      /// `/var/run` → `/run`: different strings, same file (spec §11).
      #[tokio::test]
      async fn collides_with_socket_path_through_a_symlinked_parent() {
          let dir = private_dir("admin_bind_alias");
          let real = dir.join("real");
          crate::testutil::mkdir_p(&real);
          let alias = dir.join("alias");
          std::os::unix::fs::symlink(&real, &alias).unwrap();
          let admin = alias.join("x.sock");
          let producer = real.join("x.sock");
          assert!(names_same_file(&admin, &producer));
          assert!(names_same_file(Path::new("/run/weir//x.sock"), Path::new("/run/weir/x.sock")));
          assert!(!names_same_file(&alias.join("y.sock"), &producer));
          let e = bind_admin(&admin, &producer).err().expect("refused");
          assert_eq!(e.reason_code, reason::COLLIDES_WITH_SOCKET_PATH);
          let _ = std::fs::remove_dir_all(&dir);
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::tests -- --test-threads=1`
  Expected: compile failure, `cannot find function `bind_admin` in this scope`.

- [ ] **Step 3: Implement** (append to `admin/mod.rs`)

  ```rust
  use std::{
      fmt, io,
      path::{Path, PathBuf},
  };

  use crate::socket::{self, BoundSocket, ParentPolicy};

  /// Why the admin socket is unavailable. Codes only: rendered on the public
  /// status document (spec §9.1).
  pub(crate) mod reason {
      pub(crate) const PARENT_INSECURE: &str = "parent_insecure";
      pub(crate) const IN_USE: &str = "in_use";
      pub(crate) const BIND_FAILED: &str = "bind_failed";
      pub(crate) const COLLIDES_WITH_SOCKET_PATH: &str = "collides_with_socket_path";
  }

  pub(crate) struct BoundAdmin {
      pub(crate) path: PathBuf,
      pub(crate) socket: BoundSocket,
  }

  #[derive(Debug)]
  pub(crate) struct AdminBindError {
      pub(crate) reason_code: &'static str,
      pub(crate) source: io::Error,
  }

  impl fmt::Display for AdminBindError {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          write!(f, "{} ({})", self.source, self.reason_code)
      }
  }

  /// Binds the admin socket (spec §11 "Bind"): format check, the (dev, ino)
  /// collision check against `socket_path`, PR 3's `refuse_if_live` (a live
  /// socket is refused with the admin socket's own wording, spec §5.7/§5.9),
  /// then `bind_hardened_with` under `RequirePrivate`, whose stale-socket step
  /// probes again harmlessly (connect-only) and whose `umask(0o177)` bind
  /// creates the inode at 0600 with an inode re-check.
  ///
  /// The collision check runs BEFORE the bind, not after both binds as the
  /// spec words it: the admin socket binds at startup step 7, before the
  /// producer socket (step 11), and a colliding producer bind would otherwise
  /// be refused by the admin socket's live listener with a message that names
  /// the wrong cause.
  pub(crate) fn bind_admin(path: &Path, socket_path: &Path) -> Result<BoundAdmin, AdminBindError> {
      socket::validate_socket_path(path)
          .map_err(|source| AdminBindError { reason_code: reason::BIND_FAILED, source })?;
      if names_same_file(path, socket_path) {
          return Err(AdminBindError {
              reason_code: reason::COLLIDES_WITH_SOCKET_PATH,
              source: io::Error::new(
                  io::ErrorKind::InvalidInput,
                  format!(
                      "admin socket path '{}' names the same file as socket_path '{}'",
                      path.display(),
                      socket_path.display()
                  ),
              ),
          });
      }
      // spec §5.9: refuse a live socket with the admin socket's own wording
      // before touching the path (PR 3's helper over the safety floor's
      // connect-only probe; `bind_hardened_with` probes again, harmlessly).
      socket::refuse_if_live(path, "admin socket")
          .map_err(|source| AdminBindError { reason_code: classify_bind_error(&source), source })?;
      let socket = socket::bind_hardened_with(path, ParentPolicy::RequirePrivate)
          .map_err(|source| AdminBindError { reason_code: classify_bind_error(&source), source })?;
      Ok(BoundAdmin { path: path.to_path_buf(), socket })
  }

  fn classify_bind_error(e: &io::Error) -> &'static str {
      if socket::is_insecure_parent(e) {
          reason::PARENT_INSECURE
      } else if e.kind() == io::ErrorKind::AddrInUse {
          // `live_socket_refusal`, or EADDRINUSE from bind(2) itself.
          reason::IN_USE
      } else {
          reason::BIND_FAILED
      }
  }

  /// True when the two paths are the same lexically, or have the same
  /// basename in the same parent directory by (dev, ino) — which catches a
  /// symlinked parent such as `/var/run` → `/run`.
  pub(crate) fn names_same_file(admin: &Path, socket_path: &Path) -> bool {
      use std::os::unix::fs::MetadataExt;
      use weir_status::admin::lexical_normalize;
      if lexical_normalize(admin) == lexical_normalize(socket_path) {
          return true;
      }
      if admin.file_name() != socket_path.file_name() {
          return false;
      }
      match (
          admin.parent().map(std::fs::metadata),
          socket_path.parent().map(std::fs::metadata),
      ) {
          (Some(Ok(a)), Some(Ok(b))) => (a.dev(), a.ino()) == (b.dev(), b.ino()),
          _ => false,
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin:: -- --test-threads=1`
  Expected: 5 new + 3 identity tests pass. Verified against 2491883: probe_socket (socket/mod.rs:810-823) settles a non-Linux ECONNREFUSED with a datagram probe; both refuse_if_live and bind_hardened_with go through it, so these tests behave the same on Linux and macOS.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): bind the admin socket with path, collision and live-socket refusals"`

---
### Task 6.7: `admin/framing.rs` — bounded JSON lines, deadlines, request limits

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Create: `crates/weir-server/src/admin/framing.rs`
- Modify: `crates/weir-server/src/admin/mod.rs` (add `pub(crate) mod framing;`)
- Test: `crates/weir-server/src/admin/framing.rs`

**Interfaces:**
- Consumes: `weir_status::admin::{Response, AdminError, ErrorCode, InstanceId, Verb, MAX_LINE_BYTES, MAX_WAIT_MS}`; tokio `io-util` (already enabled); `serde_json` as a normal weir-server dependency (PR 1 Task 1.11 moved it).
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug)]
  pub(crate) struct Limits { pub(crate) max_line_bytes: usize, pub(crate) request_deadline: Duration,
                             pub(crate) idle_timeout: Duration, pub(crate) write_timeout: Duration,
                             pub(crate) max_requests: usize }
  impl Limits { pub(crate) const PRODUCTION: Limits }   // 64 KiB, 5 s, 10 s, 5 s, 64
  #[derive(Debug, PartialEq, Eq)] pub(crate) enum LineRead { Line, Eof, TooLarge }
  pub(crate) async fn read_line_bounded<R: AsyncBufRead + Unpin>(r: &mut R, line: &mut Vec<u8>, max: usize) -> io::Result<LineRead>
  pub(crate) trait Dispatch {
      fn dispatch(&self, line: &[u8], peer_uid: u32) -> impl Future<Output = Response> + Send;
      fn instance(&self) -> Option<InstanceId>;
      fn on_framing_error(&self, _code: ErrorCode) {}
  }
  pub(crate) async fn serve_connection<D: Dispatch + Sync>(stream: UnixStream, peer_uid: u32, d: &D, limits: &Limits)
  pub(crate) async fn write_response<W: AsyncWrite + Unpin>(w: &mut W, r: &Response, t: Duration) -> io::Result<()>
  pub(crate) struct ParsedRequest { pub(crate) id: Value, pub(crate) verb: Verb, pub(crate) args: Map<String, Value> }
  pub(crate) fn parse_request(line: &[u8]) -> Result<ParsedRequest, (Value, AdminError)>
  pub(crate) fn wait_arg(args: &Map<String, Value>, default_ms: u64) -> Result<Duration, AdminError>
  ```

- [ ] **Step 1: Write the failing test**

  Add `pub(crate) mod framing;` to `admin/mod.rs` now, so the test module is compiled. The new `admin/framing.rs` holds only:

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use serde_json::json;
      use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
      use tokio::net::unix::OwnedReadHalf;

      fn inst() -> InstanceId {
          InstanceId {
              pid: 1,
              started_at_ms: 2,
              wab_dir: "/w".into(),
              socket_path: "/s".into(),
              wab_dir_dev: 3,
              wab_dir_ino: 4,
          }
      }

      /// A stub dispatcher, so framing is tested without any daemon state.
      struct Echo;
      impl Dispatch for Echo {
          async fn dispatch(&self, line: &[u8], peer_uid: u32) -> Response {
              match parse_request(line) {
                  Ok(r) => Response::success(
                      r.id,
                      inst(),
                      json!({"verb": r.verb.as_str(), "peer_uid": peer_uid}),
                  ),
                  Err((id, e)) => Response::failure(id, Some(inst()), e),
              }
          }
          fn instance(&self) -> Option<InstanceId> {
              Some(inst())
          }
      }

      fn fast() -> Limits {
          Limits {
              request_deadline: Duration::from_millis(200),
              idle_timeout: Duration::from_millis(300),
              ..Limits::PRODUCTION
          }
      }

      fn serve(limits: Limits) -> (BufReader<OwnedReadHalf>, tokio::net::unix::OwnedWriteHalf) {
          let (client, server) = UnixStream::pair().unwrap();
          tokio::spawn(async move { serve_connection(server, 1000, &Echo, &limits).await });
          let (rd, wr) = client.into_split();
          (BufReader::new(rd), wr)
      }

      /// `Some(json)` for a line, `None` once the server closed (EOF or reset).
      async fn next(rd: &mut BufReader<OwnedReadHalf>) -> Option<Value> {
          let mut s = String::new();
          let n = tokio::time::timeout(Duration::from_secs(2), rd.read_line(&mut s))
              .await
              .expect("the server answers or closes within 2 s");
          match n {
              Ok(0) | Err(_) => None,
              Ok(_) => Some(serde_json::from_str(&s).expect("one JSON object per line")),
          }
      }

      #[tokio::test]
      async fn requests_are_answered_in_order_one_line_each() {
          let (mut rd, mut wr) = serve(Limits::PRODUCTION);
          wr.write_all(b"{\"v\":1,\"id\":1,\"verb\":\"status\"}\n{\"v\":1,\"id\":2,\"verb\":\"reload\",\"extra\":true}\n")
              .await
              .unwrap();
          let a = next(&mut rd).await.unwrap();
          let b = next(&mut rd).await.unwrap();
          assert_eq!((a["id"].clone(), a["result"]["verb"].clone()), (json!(1), json!("status")));
          assert_eq!((b["id"].clone(), b["result"]["verb"].clone()), (json!(2), json!("reload")));
          assert_eq!(a["result"]["peer_uid"], 1000);
          assert_eq!(a["instance"]["pid"], 1, "identity on every response");
      }

      #[tokio::test]
      async fn an_oversized_line_is_refused_then_closed() {
          let (mut rd, mut wr) = serve(Limits::PRODUCTION);
          // The server stops reading at 64 KiB, so the writer may be cut off.
          tokio::spawn(async move {
              let _ = wr.write_all(&vec![b'a'; 70 * 1024]).await;
          });
          let r = next(&mut rd).await.expect("one error line");
          assert_eq!(r["error"]["code"], "request_too_large");
          assert!(next(&mut rd).await.is_none(), "then the connection closes");
      }

      /// The bound holds while reading: nothing past the limit is buffered.
      #[tokio::test]
      async fn read_line_bounded_never_buffers_past_the_limit() {
          let data = vec![b'a'; 70 * 1024];
          let mut r = &data[..];
          let mut line = Vec::new();
          assert_eq!(
              read_line_bounded(&mut r, &mut line, MAX_LINE_BYTES).await.unwrap(),
              LineRead::TooLarge
          );
          assert!(line.len() <= MAX_LINE_BYTES, "buffered {}", line.len());

          let mut exact = vec![b'b'; MAX_LINE_BYTES];
          exact.push(b'\n');
          let mut r = &exact[..];
          line.clear();
          assert_eq!(read_line_bounded(&mut r, &mut line, MAX_LINE_BYTES).await.unwrap(), LineRead::Line);
          assert_eq!(line.len(), MAX_LINE_BYTES);

          // A line split over many small reads.
          let mut r = tokio::io::BufReader::with_capacity(4, &b"{\"v\":1}\nrest"[..]);
          line.clear();
          assert_eq!(read_line_bounded(&mut r, &mut line, 64).await.unwrap(), LineRead::Line);
          assert_eq!(line, b"{\"v\":1}");
      }

      #[tokio::test]
      async fn a_request_that_never_ends_hits_the_deadline() {
          let (mut rd, mut wr) = serve(fast());
          wr.write_all(b"{\"v\":1").await.unwrap(); // no newline
          let r = next(&mut rd).await.expect("an error line");
          assert_eq!(r["error"]["code"], "bad_request");
          assert!(r["error"]["message"].as_str().unwrap().contains("within"), "{r}");
          assert!(next(&mut rd).await.is_none());
      }

      #[tokio::test]
      async fn an_idle_connection_is_closed() {
          let (mut rd, _wr) = serve(fast());
          let t0 = std::time::Instant::now();
          assert!(next(&mut rd).await.is_none());
          assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
      }

      #[tokio::test]
      async fn at_most_max_requests_per_connection() {
          let (mut rd, mut wr) = serve(Limits { max_requests: 2, ..Limits::PRODUCTION });
          for i in 0..3 {
              let _ = wr
                  .write_all(format!("{{\"v\":1,\"id\":{i},\"verb\":\"status\"}}\n").as_bytes())
                  .await;
          }
          assert_eq!(next(&mut rd).await.unwrap()["id"], 0);
          assert_eq!(next(&mut rd).await.unwrap()["id"], 1);
          assert!(next(&mut rd).await.is_none(), "the third is never answered");
      }

      #[test]
      fn parse_request_table() {
          let e = |line: &str| parse_request(line.as_bytes()).err().expect(line);
          let (id, err) = e("not json");
          assert_eq!((id, err.code), (Value::Null, ErrorCode::BadRequest));
          let (id, err) = e(r#"[1,2]"#);
          assert_eq!((id, err.code), (Value::Null, ErrorCode::BadRequest));
          let (id, err) = e(r#"{"v":2,"id":"a","verb":"status"}"#);
          assert_eq!((id, err.code), (json!("a"), ErrorCode::UnsupportedVersion));
          assert_eq!(err.supported, Some(vec![1]));
          let (_, err) = e(r#"{"id":1,"verb":"status"}"#);
          assert_eq!(err.code, ErrorCode::BadRequest, "v is required");
          let (_, err) = e(r#"{"v":1,"verb":"drain.explode"}"#);
          assert_eq!(err.code, ErrorCode::UnknownVerb);
          assert!(!err.message.contains("explode"), "the request text is not echoed");
          let (_, err) = e(r#"{"v":1,"verb":"status","args":[1]}"#);
          assert_eq!(err.code, ErrorCode::BadRequest);
          let ok = parse_request(br#"{"v":1,"verb":"drain.pause","args":{"wait_ms":5},"x":1}"#).unwrap();
          assert_eq!(ok.verb, Verb::DrainPause);
          assert_eq!(ok.id, Value::Null);
          assert_eq!(ok.args.get("wait_ms"), Some(&json!(5)));
      }

      #[test]
      fn wait_arg_bounds() {
          let m = |v: Value| v.as_object().unwrap().clone();
          assert_eq!(wait_arg(&m(json!({})), 5000).unwrap(), Duration::from_millis(5000));
          assert_eq!(wait_arg(&m(json!({"wait_ms": null})), 10).unwrap(), Duration::from_millis(10));
          assert_eq!(wait_arg(&m(json!({"wait_ms": 0})), 5000).unwrap(), Duration::ZERO);
          assert_eq!(wait_arg(&m(json!({"wait_ms": 30000})), 0).unwrap(), Duration::from_secs(30));
          for bad in [json!({"wait_ms": 30001}), json!({"wait_ms": -1}), json!({"wait_ms": "5"})] {
              assert_eq!(wait_arg(&m(bad), 0).unwrap_err().code, ErrorCode::BadRequest);
          }
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::framing -- --test-threads=1`
  Expected: compile failure, `error[E0412]: cannot find type `InstanceId` in this scope` (and `Limits`, `Dispatch`, `Response`).

- [ ] **Step 3: Implement** (`admin/framing.rs`, above the test module)

  ```rust
  //! JSON-lines framing for the admin socket (spec §11 "Framing"): one UTF-8
  //! JSON object per `\n` each way, answered in order. The 64 KiB line limit
  //! is enforced WHILE reading, so an oversized line is refused without being
  //! buffered. Deadlines: 10 s idle before a request's first byte, 5 s from
  //! that byte to the newline, 5 s per write. At most 64 requests per
  //! connection.

  use std::{future::Future, io, time::Duration};

  use serde_json::{Map, Value};
  use tokio::{
      io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
      net::UnixStream,
      time::timeout,
  };
  use weir_status::admin::{
      AdminError, ErrorCode, InstanceId, MAX_LINE_BYTES, MAX_WAIT_MS, Response, Verb,
  };

  #[derive(Clone, Copy, Debug)]
  pub(crate) struct Limits {
      pub(crate) max_line_bytes: usize,
      pub(crate) request_deadline: Duration,
      pub(crate) idle_timeout: Duration,
      pub(crate) write_timeout: Duration,
      pub(crate) max_requests: usize,
  }

  impl Limits {
      pub(crate) const PRODUCTION: Limits = Limits {
          max_line_bytes: MAX_LINE_BYTES,
          request_deadline: Duration::from_secs(5),
          idle_timeout: Duration::from_secs(10),
          write_timeout: Duration::from_secs(5),
          max_requests: 64,
      };
  }

  /// How long a refused oversized connection's input is drained after the
  /// error is written, so the close is a clean EOF rather than a reset.
  const DRAIN_ON_CLOSE: Duration = Duration::from_millis(100);

  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum LineRead {
      Line,
      Eof,
      TooLarge,
  }

  /// Appends bytes up to (not including) the next `\n` to `line`, consuming
  /// the `\n`. Returns `TooLarge` as soon as the line would exceed `max`,
  /// having buffered at most `max` bytes.
  pub(crate) async fn read_line_bounded<R: AsyncBufRead + Unpin>(
      r: &mut R,
      line: &mut Vec<u8>,
      max: usize,
  ) -> io::Result<LineRead> {
      loop {
          let buf = r.fill_buf().await?;
          if buf.is_empty() {
              return Ok(LineRead::Eof);
          }
          match buf.iter().position(|&b| b == b'\n') {
              Some(i) => {
                  if line.len() + i > max {
                      return Ok(LineRead::TooLarge);
                  }
                  line.extend_from_slice(&buf[..i]);
                  r.consume(i + 1);
                  return Ok(LineRead::Line);
              }
              None => {
                  let n = buf.len();
                  if line.len() + n > max {
                      return Ok(LineRead::TooLarge);
                  }
                  line.extend_from_slice(buf);
                  r.consume(n);
              }
          }
      }
  }

  /// What a connection hands each complete request line to.
  pub(crate) trait Dispatch {
      /// Answers one line. Never fails: every problem is an error `Response`.
      fn dispatch(&self, line: &[u8], peer_uid: u32) -> impl Future<Output = Response> + Send;
      /// Identity for framing-level errors (deadline, oversized line).
      fn instance(&self) -> Option<InstanceId>;
      /// Called once per framing-level error, for metrics.
      fn on_framing_error(&self, _code: ErrorCode) {}
  }

  pub(crate) async fn serve_connection<D: Dispatch + Sync>(
      stream: UnixStream,
      peer_uid: u32,
      d: &D,
      limits: &Limits,
  ) {
      let (rd, mut wr) = stream.into_split();
      let mut rd = BufReader::with_capacity(8 * 1024, rd);
      let mut line = Vec::with_capacity(1024);
      for _ in 0..limits.max_requests {
          // Idle: wait for the next request's first byte.
          match timeout(limits.idle_timeout, rd.fill_buf()).await {
              Ok(Ok(b)) if !b.is_empty() => {}
              _ => return, // idle timeout, EOF or a read error
          }
          line.clear();
          let read = timeout(
              limits.request_deadline,
              read_line_bounded(&mut rd, &mut line, limits.max_line_bytes),
          )
          .await;
          let resp = match read {
              Ok(Ok(LineRead::Line)) => d.dispatch(&line, peer_uid).await,
              Ok(Ok(LineRead::Eof)) | Ok(Err(_)) => return,
              Ok(Ok(LineRead::TooLarge)) => {
                  let r = framing_error(
                      d,
                      ErrorCode::RequestTooLarge,
                      format!("a request line may be at most {} bytes", limits.max_line_bytes),
                  );
                  let _ = write_response(&mut wr, &r, limits.write_timeout).await;
                  let _ = wr.shutdown().await;
                  // Discard (never buffer) what is still arriving, briefly.
                  let mut sink = [0u8; 8 * 1024];
                  let _ = timeout(DRAIN_ON_CLOSE, async {
                      while matches!(rd.read(&mut sink).await, Ok(n) if n > 0) {}
                  })
                  .await;
                  return;
              }
              Err(_elapsed) => {
                  let r = framing_error(
                      d,
                      ErrorCode::BadRequest,
                      format!(
                          "request not completed within {} ms of its first byte",
                          limits.request_deadline.as_millis()
                      ),
                  );
                  let _ = write_response(&mut wr, &r, limits.write_timeout).await;
                  return;
              }
          };
          if write_response(&mut wr, &resp, limits.write_timeout).await.is_err() {
              return;
          }
      }
  }

  fn framing_error<D: Dispatch>(d: &D, code: ErrorCode, message: String) -> Response {
      d.on_framing_error(code);
      Response::failure(Value::Null, d.instance(), AdminError::new(code, message))
  }

  pub(crate) async fn write_response<W: AsyncWrite + Unpin>(
      w: &mut W,
      r: &Response,
      t: Duration,
  ) -> io::Result<()> {
      let mut bytes = serde_json::to_vec(r).map_err(io::Error::other)?;
      bytes.push(b'\n');
      match timeout(t, w.write_all(&bytes)).await {
          Ok(res) => res,
          Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "admin write timed out")),
      }
  }

  pub(crate) struct ParsedRequest {
      pub(crate) id: Value,
      pub(crate) verb: Verb,
      pub(crate) args: Map<String, Value>,
  }

  fn bad_request(m: impl Into<String>) -> AdminError {
      AdminError::new(ErrorCode::BadRequest, m)
  }

  /// Validates the v1 envelope. Unknown fields are ignored (spec §11). The
  /// `id` is echoed on errors whenever it could be read.
  pub(crate) fn parse_request(line: &[u8]) -> Result<ParsedRequest, (Value, AdminError)> {
      let v: Value = serde_json::from_slice(line)
          .map_err(|e| (Value::Null, bad_request(format!("not valid JSON: {e}"))))?;
      let Value::Object(mut obj) = v else {
          return Err((Value::Null, bad_request("a request is one JSON object")));
      };
      let id = obj.remove("id").unwrap_or(Value::Null);
      match obj.get("v").and_then(Value::as_u64) {
          Some(1) => {}
          Some(other) => {
              return Err((
                  id,
                  AdminError::unsupported_version(format!("protocol version {other} is not supported")),
              ));
          }
          None => return Err((id, bad_request("missing or non-integer \"v\""))),
      }
      let Some(verb_s) = obj.get("verb").and_then(Value::as_str) else {
          return Err((id, bad_request("missing \"verb\"")));
      };
      let Some(verb) = Verb::parse(verb_s) else {
          return Err((
              id,
              AdminError::new(
                  ErrorCode::UnknownVerb,
                  format!("unknown verb; this daemon knows {:?}", Verb::names()),
              ),
          ));
      };
      let args = match obj.remove("args") {
          None | Some(Value::Null) => Map::new(),
          Some(Value::Object(m)) => m,
          Some(_) => return Err((id, bad_request("\"args\" must be an object"))),
      };
      Ok(ParsedRequest { id, verb, args })
  }

  /// `args.wait_ms`: an integer `0..=30000`, defaulting per verb.
  pub(crate) fn wait_arg(args: &Map<String, Value>, default_ms: u64) -> Result<Duration, AdminError> {
      match args.get("wait_ms") {
          None | Some(Value::Null) => Ok(Duration::from_millis(default_ms)),
          Some(v) => match v.as_u64() {
              Some(n) if n <= MAX_WAIT_MS => Ok(Duration::from_millis(n)),
              _ => Err(bad_request(format!("wait_ms must be an integer 0..={MAX_WAIT_MS}"))),
          },
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin::framing -- --test-threads=1`
  Expected: 8 passed.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): bounded JSON-lines framing with deadlines and request limits"`

---

### Task 6.8: the admin end of the reload loop — `at_unix_ms`, `to_wire`, board publication, `request_reload_and_wait`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/reload.rs` — module doc (lines 1-21: the admin verb now exists); `ReloadTrigger::Admin` (lines 51-54: the doc's "Reserved" sentence goes; the `#[cfg_attr(not(test), allow(dead_code))]` stays until Task 6.14, because nothing outside tests constructs `Admin` before the admin module is wired); `ReloadReport` (97-109: add `at_unix_ms`); `Reloader` (123-142: add `board`); `Reloader::reload` (158-189: stamp, publish); `request_reload` (283-305: share `count_dropped`); new `ReloadReport::to_wire`, `Reloader::attach_board`, `ReloadWaitError`, `request_reload_and_wait`, `ADMIN_ENQUEUE_TIMEOUT`, `ADMIN_REPLY_TIMEOUT`, `ITEM_TLS_CERTIFICATES`.
- Modify: `crates/weir-server/src/metrics/mod.rs` (`ConfigReloadTrigger::admin` doc comment at ~283: no longer reserved)
- Modify: `crates/weir-server/src/status/board.rs` (test only: PR 1's `the_reload_cell_is_sticky_on_a_tls_failure_until_a_success` builds `ReloadReport { trigger, outcome, changed, rejected, note: None }`; add `at_unix_ms: 0` to that literal, or it stops compiling with the new field)
- Test: `crates/weir-server/src/reload.rs` tests (and its `tls` submodule)

**Interfaces:**
- Consumes: `crate::status::StatusBoard` with field `reload: ReloadCell` and `ReloadCell::{publish(&self, report: ReloadReport, now_ms: u64), evidence(&self) -> Option<ReloadEvidence>}` (PR 1 Task 1.16; `ReloadEvidence { trigger: ReloadTriggerCode, outcome: ReloadOutcomeCode, .. }`) and the test constructor `StatusBoard::for_test() -> Arc<StatusBoard>` (PR 1); `weir_status::admin::{ReloadResult, ReloadItemWire, ReloadItemOutcomeWire, ReloadOutcomeWire, NotReloaded}` (Task 6.1); PR 1's `crate::status::unix_ms_now()` for the timestamp.
- Produces:
  ```rust
  pub(crate) struct ReloadReport { /* existing */ pub at_unix_ms: u64 }
  impl ReloadReport { pub(crate) fn to_wire(&self) -> weir_status::admin::ReloadResult }
  impl Reloader { pub(crate) fn attach_board(&self, board: Arc<StatusBoard>) }
  pub(crate) const ITEM_TLS_CERTIFICATES: &str = "tls_certificates";
  pub(crate) const ADMIN_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(1);
  pub(crate) const ADMIN_REPLY_TIMEOUT: Duration = Duration::from_secs(10);
  #[derive(Debug, PartialEq, Eq)] pub(crate) enum ReloadWaitError { QueueFull, LoopGone, NoAnswer, TimedOut }
  pub(crate) async fn request_reload_and_wait(tx: &mpsc::Sender<ReloadRequest>, trigger: ReloadTrigger,
      metrics: &Metrics, enqueue_timeout: Duration, reply_timeout: Duration) -> Result<ReloadReport, ReloadWaitError>
  ```
  Unchanged and still the only reload path: `Reloader::reload`, `reload_loop`, `spawn_reload_loop`, `request_reload`, `spawn_sighup_task`. (Decision Q4: the safety floor's `spawn_sighup_task` stays the only SIGHUP handler; Lifecycle handles TERM/INT only.)

  Until Task 6.14 wires `main`, `to_wire`, `attach_board`, `ReloadWaitError`, `request_reload_and_wait`, `ADMIN_ENQUEUE_TIMEOUT`, `ADMIN_REPLY_TIMEOUT` and `ITEM_TLS_CERTIFICATES` have no caller outside tests, so each carries `#[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]`, which Task 6.14 removes. The tests below use every one of them, so the test target needs no allowance.

- [ ] **Step 1: Write the failing test**

  In `reload.rs` `mod tests`:
  ```rust
  use weir_status::admin::{ReloadItemOutcomeWire, ReloadOutcomeWire};

  #[test]
  fn reports_carry_a_time_and_map_to_the_wire_shape() {
      let r = Reloader::new(metrics(), false);
      let report = r.reload(ReloadTrigger::Admin);
      assert!(report.at_unix_ms > 1_600_000_000_000, "{}", report.at_unix_ms);
      let w = report.to_wire();
      assert_eq!(w.trigger, "admin");
      assert_eq!(w.at_unix_ms, report.at_unix_ms);
      assert_eq!(w.outcome, ReloadOutcomeWire::Noop);
      assert_eq!(w.items.len(), 1);
      assert_eq!(w.items[0].item, ITEM_TLS_CERTIFICATES);
      assert_eq!(w.items[0].outcome, ReloadItemOutcomeWire::NotConfigured);
      assert_eq!(w.items[0].detail.as_deref(), Some(NOTE_NOTHING_RELOADABLE));
      assert_eq!(w.not_reloaded.len(), 1);
      assert_eq!(w.not_reloaded[0].item, "config_file");
      assert_eq!(w.not_reloaded[0].reason_code, "restart_required");
  }

  /// spec §5.2: the board's reload cell is written by the Reloader only.
  #[test]
  fn the_last_report_is_published_to_the_board() {
      let board = crate::status::StatusBoard::for_test();
      let r = Reloader::new(metrics(), false);
      r.attach_board(Arc::clone(&board));
      assert!(board.reload.evidence().is_none());
      r.reload(ReloadTrigger::Sighup);
      let last = board.reload.evidence().expect("published");
      assert_eq!(last.trigger, weir_status::ReloadTriggerCode::Sighup);
      assert_eq!(last.outcome, weir_status::ReloadOutcomeCode::Noop);
  }

  #[tokio::test]
  async fn an_admin_reload_waits_for_its_own_report() {
      let m = metrics();
      let tx = spawn_reload_loop(Reloader::new(Arc::clone(&m), false));
      // The production timeouts the admin verb uses (1 s enqueue, 10 s reply).
      let report = request_reload_and_wait(
          &tx,
          ReloadTrigger::Admin,
          &m,
          ADMIN_ENQUEUE_TIMEOUT,
          ADMIN_REPLY_TIMEOUT,
      )
      .await
      .unwrap();
      assert_eq!(report.trigger, ReloadTrigger::Admin);
      assert_eq!(report.outcome, ReloadOutcome::Noop);
      assert_eq!(reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::noop), 1);
  }

  #[tokio::test]
  async fn an_admin_reload_on_a_full_queue_is_refused_and_counted() {
      let m = metrics();
      let (tx, _stalled) = reload_channel();
      for _ in 0..RELOAD_QUEUE_DEPTH {
          assert!(request_reload(&tx, ReloadTrigger::Sighup, &m));
      }
      let e = request_reload_and_wait(
          &tx,
          ReloadTrigger::Admin,
          &m,
          std::time::Duration::from_millis(50),
          std::time::Duration::from_millis(50),
      )
      .await
      .unwrap_err();
      assert_eq!(e, ReloadWaitError::QueueFull);
      assert_eq!(reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::dropped), 1);
  }

  #[tokio::test]
  async fn an_admin_reload_with_no_loop_reports_it() {
      let m = metrics();
      let (tx, rx) = reload_channel();
      drop(rx);
      let e = request_reload_and_wait(
          &tx,
          ReloadTrigger::Admin,
          &m,
          std::time::Duration::from_millis(50),
          std::time::Duration::from_millis(50),
      )
      .await
      .unwrap_err();
      assert_eq!(e, ReloadWaitError::LoopGone);
  }
  ```
  In the existing `mod tls` (feature `tls`), extend `registered_tls_reloads_and_fails_safe` after each reload:
  ```rust
  // after the successful reload:
  let w = report.to_wire();
  assert_eq!(w.outcome, weir_status::admin::ReloadOutcomeWire::Applied);
  assert_eq!(w.items[0].outcome, weir_status::admin::ReloadItemOutcomeWire::Reloaded);
  // after the failed reload:
  let w = report.to_wire();
  assert_eq!(w.outcome, weir_status::admin::ReloadOutcomeWire::Failed);
  assert_eq!(w.items[0].outcome, weir_status::admin::ReloadItemOutcomeWire::Failed);
  assert!(w.items[0].detail.is_some(), "the rejection reason is admin detail");
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server reload:: -- --test-threads=1`
  Expected: compile failure, `no field `at_unix_ms` on type `ReloadReport``, `no method named `to_wire``.

- [ ] **Step 3: Implement**

  ```rust
  use std::sync::{Arc, OnceLock};          // OnceLock is now unconditional
  use std::time::Duration;
  use crate::status::{StatusBoard, unix_ms_now};

  #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
  pub(crate) const ITEM_TLS_CERTIFICATES: &str = "tls_certificates";
  /// How long the admin verb waits for room in the 8-deep reload queue.
  #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
  pub(crate) const ADMIN_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(1);
  /// How long the admin verb waits for the loop's answer.
  #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
  pub(crate) const ADMIN_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

  pub(crate) struct ReloadReport {
      // … existing fields …
      /// When the reload finished (unix ms).
      pub at_unix_ms: u64,
  }

  impl ReloadReport {
      /// The admin wire shape (spec §5.10/§11). One item today: the TLS
      /// material. The config file is reported as not reloaded.
      #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
      pub(crate) fn to_wire(&self) -> weir_status::admin::ReloadResult {
          use weir_status::admin::{
              NotReloaded, ReloadItemOutcomeWire as Item, ReloadItemWire as ReloadItem,
              ReloadOutcomeWire as Out, ReloadResult,
          };
          let outcome = match self.outcome {
              ReloadOutcome::Ok => Out::Applied,
              ReloadOutcome::Noop => Out::Noop,
              ReloadOutcome::Failed => Out::Failed,
          };
          let tls = if self.changed.contains(&"tls") {
              ReloadItem { item: ITEM_TLS_CERTIFICATES.into(), outcome: Item::Reloaded, detail: None }
          } else if let Some((_, why)) = self.rejected.iter().find(|(k, _)| *k == "tls") {
              ReloadItem { item: ITEM_TLS_CERTIFICATES.into(), outcome: Item::Failed, detail: Some(why.clone()) }
          } else {
              ReloadItem {
                  item: ITEM_TLS_CERTIFICATES.into(),
                  outcome: Item::NotConfigured,
                  detail: self.note.map(str::to_string),
              }
          };
          ReloadResult {
              trigger: match self.trigger {
                  ReloadTrigger::Sighup => "sighup",
                  ReloadTrigger::Admin => "admin",
              }
              .into(),
              at_unix_ms: self.at_unix_ms,
              outcome,
              items: vec![tls],
              not_reloaded: vec![NotReloaded {
                  item: "config_file".into(),
                  reason_code: "restart_required".into(),
                  reason: "the config file is read once at startup; edits to weir.toml or the \
                           environment need a restart"
                      .into(),
              }],
          }
      }
  }

  pub(crate) struct Reloader {
      // … existing fields …
      /// Where the last report is published (spec §5.2 `reload` cell). Set
      /// once at startup; unset in unit tests that do not need it.
      board: OnceLock<Arc<StatusBoard>>,
  }
  // Reloader::new gains `board: OnceLock::new(),`.

  impl Reloader {
      #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
      pub(crate) fn attach_board(&self, board: Arc<StatusBoard>) {
          if self.board.set(board).is_err() {
              warn!("status board attached to the reloader twice; keeping the first");
          }
      }

      pub(crate) fn reload(&self, t: ReloadTrigger) -> ReloadReport {
          let mut report = ReloadReport {
              trigger: t,
              outcome: ReloadOutcome::Noop,
              changed: Vec::new(),
              rejected: Vec::new(),
              note: None,
              at_unix_ms: 0,
          };
          #[cfg(all(unix, feature = "tls"))]
          if let Some(tls) = self.tls.get() {
              self.reload_tls(tls, &mut report);
              return self.finish(report);
          }
          // … the existing tls_configured / nothing-reloadable branches, unchanged,
          // ending in `self.finish(report)` instead of `self.count(&report); report` …
      }

      /// Stamps, publishes and counts one report.
      fn finish(&self, mut report: ReloadReport) -> ReloadReport {
          report.at_unix_ms = unix_ms_now();
          // publish first: anyone who sees the counter move also sees the report
          if let Some(board) = self.board.get() {
              board.reload.publish(report.clone(), report.at_unix_ms);
          }
          self.count(&report);
          report
      }
  }

  #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum ReloadWaitError {
      /// No room in the queue within the enqueue timeout (counted `dropped`).
      QueueFull,
      /// The loop has stopped (runtime shutting down; counted `dropped`).
      LoopGone,
      /// The loop dropped the reply: the reload panicked. Previous config stays.
      NoAnswer,
      /// No answer within the reply timeout. The reload may still complete.
      TimedOut,
  }

  fn count_dropped(metrics: &Metrics, trigger: ReloadTrigger) {
      metrics
          .config_reloads
          .get_or_create(&ConfigReloadLabel { trigger: trigger.label(), outcome: ConfigReloadOutcome::dropped })
          .inc();
  }

  /// The admin `reload` verb's path: the SAME queue and loop SIGHUP uses, with
  /// a reply channel, so both triggers run one reload function serialized.
  #[cfg_attr(not(test), allow(dead_code, reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"))]
  pub(crate) async fn request_reload_and_wait(
      tx: &mpsc::Sender<ReloadRequest>,
      trigger: ReloadTrigger,
      metrics: &Metrics,
      enqueue_timeout: Duration,
      reply_timeout: Duration,
  ) -> Result<ReloadReport, ReloadWaitError> {
      let (reply, answer) = oneshot::channel();
      match tokio::time::timeout(enqueue_timeout, tx.send(ReloadRequest { trigger, reply: Some(reply) })).await {
          Ok(Ok(())) => {}
          Ok(Err(_closed)) => {
              count_dropped(metrics, trigger);
              return Err(ReloadWaitError::LoopGone);
          }
          Err(_elapsed) => {
              count_dropped(metrics, trigger);
              return Err(ReloadWaitError::QueueFull);
          }
      }
      match tokio::time::timeout(reply_timeout, answer).await {
          Ok(Ok(report)) => Ok(report),
          Ok(Err(_)) => Err(ReloadWaitError::NoAnswer),
          Err(_) => Err(ReloadWaitError::TimedOut),
      }
  }
  ```
  `request_reload` keeps its body but replaces its inline `metrics.config_reloads…dropped…inc()` with `count_dropped(metrics, trigger)`. On `ReloadTrigger::Admin`, delete the "Reserved" sentence but keep the `#[cfg_attr(not(test), allow(dead_code))]` (Task 6.14 removes it); update the module doc's "the admin `reload` verb (Foundations) will enqueue" to the present tense. In `metrics/mod.rs`, the `ConfigReloadTrigger::admin` doc becomes "The admin socket's `reload` verb, which goes through the same `reload::Reloader`."

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server reload:: -- --test-threads=1` and `cargo test -p weir-server --features tls --bin weir-server reload:: -- --test-threads=1`
  Expected: the 5 new tests and every existing reload test pass on both feature sets.

- [ ] **Step 5: Commit**

  `git commit -am "feat(reload): admin reloads through the one reload loop, with a wire report"`

---

### Task 6.9: `AdminState` with the `status` and `reload` verbs

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Create: `crates/weir-server/src/admin/verbs.rs`
- Modify: `crates/weir-server/src/admin/mod.rs` (add `pub(crate) mod verbs;` and `#[cfg(test)] pub(crate) mod test_support`)
- Test: `crates/weir-server/src/admin/verbs.rs`

**Interfaces:**
- Consumes:
  - PR 1: `crate::status::StatusBoard` (+ `StatusBoard::for_test() -> Arc<StatusBoard>`), `weir_status::{Thresholds, LivezCheck}` with `Thresholds: Default`.
  - PR 1 Task 1.17: `crate::status::render::render_admin_doc(board: &StatusBoard, thresholds: &Thresholds, livez_checks: &[LivezCheck], now: SystemTime) -> weir_status::AdminStatusDoc` — the same function PR 5's `/status` renders through before `weir_status::public` (spec §11 "same model as /status").
  - PR 2: `crate::drain::control::DrainControl` (`Clone + Send + Sync`, `DrainControl::detached()`).
  - PR 4: `crate::lifecycle::ShutdownLevel { Running, Graceful, Forced }` deriving `PartialEq`.
  - Tasks 6.3, 6.5, 6.8.
- Produces:
  ```rust
  pub(crate) struct AdminState {
      pub(crate) board: Arc<StatusBoard>,
      pub(crate) drain_ctl: DrainControl,
      pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
      pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
      pub(crate) thresholds: Arc<Thresholds>,
      pub(crate) livez_checks: Arc<[LivezCheck]>,
      pub(crate) instance: InstanceId,
      pub(crate) metrics: Arc<Metrics>,
      pub(crate) wab_max_bytes: u64,
      pub(crate) daemon_euid: u32,
      pub(crate) mutate: tokio::sync::Mutex<()>,
  }
  impl AdminState { fn status(&self) -> Result<Value, AdminError>; async fn reload(&self) -> Result<Value, AdminError> }
  pub(crate) fn verb_label(v: Verb) -> AdminVerbLabel
  // wall-clock ms comes from PR 1's crate::status::unix_ms_now(); no local copy
  // test only: admin::test_support::{ReloadMode, Harness, harness, requests, args, uid}
  ```

- [ ] **Step 1: Write the failing test**

  `admin/mod.rs`: add `pub(crate) mod verbs;` now, so the tests below are compiled, and this test-only module:
  ```rust
  #[cfg(test)]
  pub(crate) mod test_support {
      use std::{path::Path, sync::Arc};

      use serde_json::{Map, Value};
      use tokio::sync::{Mutex, mpsc, watch};

      use super::{identity, verbs::AdminState};
      use crate::{
          drain::control::DrainControl,
          lifecycle::ShutdownLevel,
          metrics::{AdminOutcomeLabel, AdminRequestLabel, AdminVerbLabel, Metrics},
          reload::{self, ReloadRequest, Reloader},
          status::StatusBoard,
      };

      pub(crate) enum ReloadMode {
          /// A real reload loop (no TLS: every reload is a `noop`).
          Live,
          /// A queue nobody serves.
          Stalled,
      }

      pub(crate) struct Harness {
          pub(crate) st: Arc<AdminState>,
          pub(crate) level_tx: watch::Sender<ShutdownLevel>,
          pub(crate) _stalled: Option<mpsc::Receiver<ReloadRequest>>,
      }

      /// Must run inside a tokio runtime (the live loop is spawned).
      pub(crate) fn harness(mode: ReloadMode) -> Harness {
          let metrics = Arc::new(Metrics::new().0);
          let (level_tx, level_rx) = watch::channel(ShutdownLevel::Running);
          let (reload_tx, stalled) = match mode {
              ReloadMode::Live => (
                  reload::spawn_reload_loop(Reloader::new(Arc::clone(&metrics), false)),
                  None,
              ),
              ReloadMode::Stalled => {
                  let (tx, rx) = reload::reload_channel();
                  (tx, Some(rx))
              }
          };
          let wab = crate::testutil::scratch_dir("admin_harness");
          let st = Arc::new(AdminState {
              board: StatusBoard::for_test(),
              drain_ctl: DrainControl::detached(),
              reload_tx,
              level_rx,
              thresholds: Arc::new(weir_status::Thresholds::default()),
              livez_checks: Arc::from(Vec::<weir_status::LivezCheck>::new()),
              instance: identity::instance_id(&wab, Path::new("/run/weir/weir.sock"), 1).unwrap(),
              metrics,
              wab_max_bytes: 0,
              daemon_euid: uid(),
              mutate: Mutex::new(()),
          });
          Harness { st, level_tx, _stalled: stalled }
      }

      pub(crate) fn uid() -> u32 {
          // Safety: geteuid has no preconditions.
          unsafe { libc::geteuid() }
      }

      pub(crate) fn args(v: Value) -> Map<String, Value> {
          v.as_object().cloned().unwrap_or_default()
      }

      pub(crate) fn requests(m: &Metrics, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) -> u64 {
          m.admin_requests.get_or_create(&AdminRequestLabel { verb, outcome }).get()
      }
  }
  ```

  The new `admin/verbs.rs` holds only these tests:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::admin::test_support::{ReloadMode, harness};
      use crate::metrics::{ConfigReloadLabel, ConfigReloadOutcome, ConfigReloadTrigger};

      #[tokio::test]
      async fn status_is_the_admin_view_of_the_status_document() {
          let h = harness(ReloadMode::Live);
          let doc = h.st.status().unwrap();
          assert_eq!(doc["schema"], "weir.status/v1");
          assert_eq!(doc["view"], "admin");
      }

      #[tokio::test]
      async fn reload_goes_through_the_one_reload_loop() {
          let h = harness(ReloadMode::Live);
          let res = h.st.reload().await.unwrap();
          assert_eq!(res["trigger"], "admin");
          assert_eq!(res["outcome"], "noop");
          assert_eq!(res["items"][0]["item"], "tls_certificates");
          assert_eq!(res["items"][0]["outcome"], "not_configured");
          assert_eq!(res["not_reloaded"][0]["item"], "config_file");
          let n = h
              .st
              .metrics
              .config_reloads
              .get_or_create(&ConfigReloadLabel {
                  trigger: ConfigReloadTrigger::admin,
                  outcome: ConfigReloadOutcome::noop,
              })
              .get();
          assert_eq!(n, 1, "counted once, under trigger=admin");
      }

      #[tokio::test]
      async fn reload_on_a_stalled_loop_is_busy_and_retryable() {
          let h = harness(ReloadMode::Stalled);
          for _ in 0..reload::RELOAD_QUEUE_DEPTH {
              assert!(reload::request_reload(&h.st.reload_tx, ReloadTrigger::Sighup, &h.st.metrics));
          }
          let e = h.st.reload().await.unwrap_err(); // waits ADMIN_ENQUEUE_TIMEOUT (1 s)
          assert_eq!(e.code, ErrorCode::Busy);
          assert!(e.retryable);
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: compile failure, `error[E0432]: unresolved import `super::verbs::AdminState`` (test_support), and the tests cannot find `reload` / `ReloadTrigger` / `ErrorCode`.

- [ ] **Step 3: Implement** — `admin/verbs.rs`, above the test module:

  ```rust
  //! The admin verbs (spec §11) and the state they act on. `status` reads the
  //! board; `reload` goes through `reload.rs`'s single loop; the drain verbs
  //! store intent in `DrainControl` and wait for the drain's observation on
  //! the board. Neither the drain nor the Lifecycle ever awaits a client.

  use std::{sync::Arc, time::SystemTime};

  use serde_json::Value;
  use tokio::sync::{Mutex, mpsc, watch};
  use weir_status::{
      LivezCheck, Thresholds,
      admin::{AdminError, ErrorCode, InstanceId, Verb},
  };

  use crate::{
      drain::control::DrainControl,
      lifecycle::ShutdownLevel,
      metrics::{AdminVerbLabel, Metrics},
      reload::{self, ReloadRequest, ReloadTrigger, ReloadWaitError},
      status::{StatusBoard, unix_ms_now},
  };

  pub(crate) struct AdminState {
      pub(crate) board: Arc<StatusBoard>,
      pub(crate) drain_ctl: DrainControl,
      /// A clone of the reload queue SIGHUP feeds (spec §5.10: one path).
      pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
      pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
      pub(crate) thresholds: Arc<Thresholds>,
      pub(crate) livez_checks: Arc<[LivezCheck]>,
      pub(crate) instance: InstanceId,
      pub(crate) metrics: Arc<Metrics>,
      /// For the pause warning (spec §11): 0 means the WAB is unbounded.
      pub(crate) wab_max_bytes: u64,
      pub(crate) daemon_euid: u32,
      /// The admin mutation guard (spec §8): held only for the intent store,
      /// released before any observation wait.
      pub(crate) mutate: Mutex<()>,
  }

  pub(crate) fn verb_label(v: Verb) -> AdminVerbLabel {
      match v {
          Verb::Status => AdminVerbLabel::Status,
          Verb::Reload => AdminVerbLabel::Reload,
          Verb::DrainPause => AdminVerbLabel::DrainPause,
          Verb::DrainResume => AdminVerbLabel::DrainResume,
          Verb::DrainRetryStranded => AdminVerbLabel::DrainRetryStranded,
      }
  }

  fn encode<T: serde::Serialize>(v: &T) -> Result<Value, AdminError> {
      serde_json::to_value(v)
          .map_err(|e| AdminError::new(ErrorCode::Internal, format!("could not encode the result: {e}")))
  }

  impl AdminState {
      /// Same model and evaluator as `/status` (PR 1's renderer, PR 5's route), admin view.
      /// Loads only: never blocks the drain, a flusher or an ack.
      fn status(&self) -> Result<Value, AdminError> {
          let doc = crate::status::render::render_admin_doc(
              &self.board,
              &self.thresholds,
              &self.livez_checks,
              SystemTime::now(),
          );
          encode(&doc)
      }

      async fn reload(&self) -> Result<Value, AdminError> {
          let report = reload::request_reload_and_wait(
              &self.reload_tx,
              ReloadTrigger::Admin,
              &self.metrics,
              reload::ADMIN_ENQUEUE_TIMEOUT,
              reload::ADMIN_REPLY_TIMEOUT,
          )
          .await
          .map_err(|e| match e {
              ReloadWaitError::QueueFull => AdminError::new(
                  ErrorCode::Busy,
                  "the reload queue is full (a reload burst is in progress); try again",
              ),
              ReloadWaitError::LoopGone => {
                  AdminError::new(ErrorCode::Unavailable, "the reload loop has stopped")
              }
              ReloadWaitError::NoAnswer => AdminError::new(
                  ErrorCode::Internal,
                  "the reload did not complete; the previous configuration stays in force",
              ),
              ReloadWaitError::TimedOut => AdminError::new(
                  ErrorCode::Internal,
                  "no reload result within 10 s; it may still complete, see `status`",
              ),
          })?;
          // A failed TLS reload is ok:true with outcome "failed" (spec §11).
          encode(&report.to_wire())
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: 3 passed.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): status and reload verbs"`

---

### Task 6.10: `drain.pause` and `drain.resume`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/verbs.rs` (`OBSERVE_POLL`, `wait_until`, `DrainObs`, `drain_obs`, `refuse_mutation`, `drain_pause`, `drain_resume`)

*(Integrator reconciliation: the draft added `AdminCell.paused_by_uid` and a `NO_UID` constant. PR 1 already carries the pause intent and its uid on the drain cell: `DrainCell::set_pause_requested(&self, requested: bool, by_uid: Option<u32>)` writes `pause_requested` and `paused_by_uid: AtomicU64` (PR 1's private `NO_UID = u64::MAX` when none), and PR 1's renderer already shows `drain.paused_by_uid` in the admin view only. This task writes through that setter and edits no `status/` file. It also answers PR 2's open question about `pause_requested` on the board: the admin server mirrors it there.)*
- Test: `crates/weir-server/src/admin/verbs.rs`

**Interfaces:**
- Consumes (PR 2's publication into PR 1's `DrainCell`, names per spec §5.2/§5.4):
  - `DrainControl::{request_pause(&self) -> u64, request_resume(&self) -> u64, pause_requested(&self) -> bool}`;
  - `board.drain.paused_since_ms: AtomicU64` (unix ms, 0 = not paused), `board.drain.held: AtomicBool`, `board.drain.generation: AtomicU64`, `board.drain.observed_requeue: AtomicU64`, `board.drain.in_flight: ArcSwapOption<crate::status::InFlight>` with `InFlight { segment: String, processed: u64, .. }`, `board.drain.last_requeue: ArcSwapOption<crate::status::RequeueResult>`, `board.drain.exit: ArcSwapOption<crate::status::DrainExitInfo>` with `DrainExitInfo { reason: crate::status::DrainExitReason, abandoned, pending_left, backlog_left, last_panic, at_ms }` (no `Default`), `DrainExitReason::GaveUp`;
  - PR 1: `DrainCell::set_pause_requested(&self, requested: bool, by_uid: Option<u32>)` and `DrainCell.paused_by_uid: AtomicU64` (`u64::MAX` = none).
  - **Every board field the drain verbs read is read in exactly one function, `drain_obs`.** If PR 1/PR 2 settled on other names, that function is the only edit.
- Produces:
  ```rust
  pub(crate) const OBSERVE_POLL: Duration = Duration::from_millis(20);
  pub(crate) async fn wait_until(wait: Duration, done: impl FnMut() -> bool) -> bool
  fn refuse_mutation(&self) -> Result<(), AdminError>
  async fn drain_pause(&self, args: &Map<String, Value>, peer_uid: u32) -> Result<Value, AdminError>
  async fn drain_resume(&self, args: &Map<String, Value>) -> Result<Value, AdminError>
  ```

- [ ] **Step 1: Write the failing test** (`admin/verbs.rs` `mod tests`)

  ```rust
  use crate::admin::test_support::{Harness, args, uid};
  use serde_json::json;
  use std::sync::atomic::Ordering;
  use std::time::Duration;

  /// Stands in for the drain thread: observes pause intent on the board the
  /// way PR 2's Draining head does, every 5 ms.
  fn fake_drain(h: &Harness) -> tokio::task::JoinHandle<()> {
      let st = Arc::clone(&h.st);
      tokio::spawn(async move {
          loop {
              let d = &st.board.drain;
              if st.drain_ctl.pause_requested() {
                  if d.paused_since_ms.load(Ordering::Acquire) == 0 {
                      d.paused_since_ms.store(unix_ms_now(), Ordering::Release);
                  }
              } else {
                  d.paused_since_ms.store(0, Ordering::Release);
              }
              tokio::time::sleep(Duration::from_millis(5)).await;
          }
      })
  }

  #[tokio::test]
  async fn pause_is_observed_and_reported_with_its_caveats() {
      let h = harness(ReloadMode::Live);
      let drain = fake_drain(&h);
      let res = h.st.drain_pause(&args(json!({"wait_ms": 2000})), uid()).await.unwrap();
      assert_eq!(res["pause_requested"], true);
      assert_eq!(res["paused"], true);
      assert!(res["paused_since_ms"].as_u64().unwrap() > 0);
      assert_eq!(res["held"], false);
      assert_eq!(res["held_segment"], Value::Null);
      assert!(res["note"].as_str().unwrap().contains("in-memory only"));
      assert_eq!(
          res["warnings"],
          json!([weir_status::admin::WARN_UNBOUNDED_WAB]),
          "wab_max_bytes = 0 in the harness"
      );
      assert_eq!(h.st.board.drain.paused_by_uid.load(Ordering::Acquire), u64::from(uid()));
      assert!(h.st.board.drain.pause_requested.load(Ordering::Acquire), "the intent is mirrored on the board");
      drain.abort();
  }

  #[tokio::test]
  async fn an_unobserved_pause_returns_at_its_deadline() {
      let h = harness(ReloadMode::Live); // no drain: nothing observes
      let t0 = std::time::Instant::now();
      let res = h.st.drain_pause(&args(json!({"wait_ms": 0})), uid()).await.unwrap();
      assert!(t0.elapsed() < Duration::from_millis(100), "{:?}", t0.elapsed());
      assert_eq!(res["pause_requested"], true);
      assert_eq!(res["paused"], false, "requested, not yet effective (weir-ctl exits 4)");
      assert!(h.st.drain_ctl.pause_requested(), "the intent stays set");
  }

  /// spec §8: the guard is released before the observation wait, so a
  /// long-waiting pause never delays a resume.
  #[tokio::test]
  async fn a_waiting_pause_does_not_delay_resume() {
      let h = harness(ReloadMode::Live);
      let st = Arc::clone(&h.st);
      let pause = tokio::spawn(async move {
          st.drain_pause(&args(json!({"wait_ms": 30000})), uid()).await
      });
      tokio::time::sleep(Duration::from_millis(50)).await;
      let t0 = std::time::Instant::now();
      let res = h.st.drain_resume(&args(json!({"wait_ms": 0}))).await.unwrap();
      assert!(t0.elapsed() < Duration::from_millis(200), "resume took {:?}", t0.elapsed());
      assert_eq!(res["was_paused"], true);
      assert!(!h.st.drain_ctl.pause_requested(), "last writer wins");
      pause.abort();
  }

  #[tokio::test]
  async fn resume_reports_how_long_it_was_paused() {
      let h = harness(ReloadMode::Live);
      let drain = fake_drain(&h);
      h.st.drain_pause(&args(json!({"wait_ms": 2000})), uid()).await.unwrap();
      tokio::time::sleep(Duration::from_millis(100)).await;
      let res = h.st.drain_resume(&args(json!({"wait_ms": 2000}))).await.unwrap();
      assert_eq!(res["pause_requested"], false);
      assert_eq!(res["was_paused"], true);
      assert_eq!(res["resumed"], true);
      assert!(res["paused_for_secs"].as_f64().unwrap() >= 0.1, "{res}");
      assert_eq!(h.st.board.drain.paused_by_uid.load(Ordering::Acquire), u64::MAX, "PR 1's NO_UID: no pause in force");
      drain.abort();
  }

  /// spec §7 (f) / §8: after T0 the drain verbs refuse, storing no intent.
  #[tokio::test]
  async fn pause_and_resume_are_refused_after_t0() {
      let h = harness(ReloadMode::Live);
      h.level_tx.send(ShutdownLevel::Graceful).unwrap();
      let e = h.st.drain_pause(&args(json!({"wait_ms": 0})), uid()).await.unwrap_err();
      assert_eq!(e.code, ErrorCode::ShuttingDown);
      assert!(!h.st.drain_ctl.pause_requested(), "a refused pause stores no intent");
      let e = h.st.drain_resume(&args(json!({"wait_ms": 0}))).await.unwrap_err();
      assert_eq!(e.code, ErrorCode::ShuttingDown);
  }

  #[tokio::test]
  async fn drain_verbs_are_unavailable_once_the_drain_gave_up() {
      let h = harness(ReloadMode::Live);
      h.st.board.drain.exit.store(Some(Arc::new(crate::status::DrainExitInfo {
          reason: crate::status::DrainExitReason::GaveUp,
          abandoned: None,
          pending_left: 0,
          backlog_left: 0,
          last_panic: None,
          at_ms: unix_ms_now(),
      })));
      let e = h.st.drain_pause(&args(json!({"wait_ms": 0})), uid()).await.unwrap_err();
      assert_eq!(e.code, ErrorCode::Unavailable);
      assert!(e.retryable);
  }

  #[tokio::test]
  async fn wait_ms_out_of_range_is_a_bad_request_and_stores_nothing() {
      let h = harness(ReloadMode::Live);
      let e = h.st.drain_pause(&args(json!({"wait_ms": 30001})), uid()).await.unwrap_err();
      assert_eq!(e.code, ErrorCode::BadRequest);
      assert!(!h.st.drain_ctl.pause_requested());
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: compile failure, `no method named `drain_pause` found for struct `Arc<AdminState>`` (and `drain_resume`).

- [ ] **Step 3: Implement**

  `admin/verbs.rs`:
  ```rust
  use std::{sync::atomic::Ordering, time::Duration};
  use serde_json::Map;
  use weir_status::admin::{
      DEFAULT_PAUSE_WAIT_MS, HeldSegment, PAUSE_NOTE, PauseResult, ResumeResult, WARN_UNBOUNDED_WAB,
  };
  use super::framing::wait_arg;

  /// How often a mutating verb re-reads the board while it waits (spec §11).
  pub(crate) const OBSERVE_POLL: Duration = Duration::from_millis(20);

  /// Polls `done` every 20 ms until it holds or `wait` passes. Checks once
  /// even when `wait` is zero.
  pub(crate) async fn wait_until(wait: Duration, mut done: impl FnMut() -> bool) -> bool {
      let deadline = tokio::time::Instant::now() + wait;
      loop {
          if done() {
              return true;
          }
          let now = tokio::time::Instant::now();
          if now >= deadline {
              return false;
          }
          tokio::time::sleep((deadline - now).min(OBSERVE_POLL)).await;
      }
  }

  /// Everything the drain verbs read from the board. The ONLY code in the
  /// admin module that names DrainCell fields (PR 1 / PR 2).
  struct DrainObs {
      generation: u64,
      paused_since_ms: u64,
      held: bool,
      held_segment: Option<HeldSegment>,
      observed_requeue: u64,
      last_requeue: Option<Arc<crate::status::RequeueResult>>,
      gave_up: bool,
  }

  impl DrainObs {
      /// Paused at the Draining head, or held mid-segment (spec §8).
      fn paused(&self) -> bool {
          self.paused_since_ms != 0 || self.held
      }
  }

  fn drain_obs(board: &StatusBoard) -> DrainObs {
      let d = &board.drain;
      let held = d.held.load(Ordering::Acquire);
      DrainObs {
          generation: d.generation.load(Ordering::Acquire),
          paused_since_ms: d.paused_since_ms.load(Ordering::Acquire),
          held,
          held_segment: if held {
              d.in_flight.load_full().map(|f| HeldSegment {
                  segment: f.segment.clone(),
                  processed_records: f.processed,
              })
          } else {
              None
          },
          observed_requeue: d.observed_requeue.load(Ordering::Acquire),
          last_requeue: d.last_requeue.load_full(),
          gave_up: d
              .exit
              .load_full()
              .is_some_and(|x| x.reason == crate::status::DrainExitReason::GaveUp),
      }
  }

  impl AdminState {
      /// spec §8 "Refusals": after T0 → `shutting_down`; the drain gave up →
      /// `unavailable`. Checked before any intent is stored.
      fn refuse_mutation(&self) -> Result<(), AdminError> {
          if *self.level_rx.borrow() != ShutdownLevel::Running {
              return Err(AdminError::new(
                  ErrorCode::ShuttingDown,
                  "the daemon is shutting down; drain verbs are refused (status and reload still work)",
              ));
          }
          if drain_obs(&self.board).gave_up {
              return Err(AdminError::new(
                  ErrorCode::Unavailable,
                  "the drain stopped after exhausting its restart budget; see `status`",
              ));
          }
          Ok(())
      }

      async fn drain_pause(&self, args: &Map<String, Value>, peer_uid: u32) -> Result<Value, AdminError> {
          let wait = wait_arg(args, DEFAULT_PAUSE_WAIT_MS)?;
          self.refuse_mutation()?;
          {
              let _g = self.mutate.lock().await;
              self.drain_ctl.request_pause();
              // PR 1's mirror of the intent and its uid (admin view only).
              self.board.drain.set_pause_requested(true, Some(peer_uid));
          } // released BEFORE the wait (spec §8)
          wait_until(wait, || drain_obs(&self.board).paused()).await;
          let o = drain_obs(&self.board);
          let mut warnings = Vec::new();
          if self.wab_max_bytes == 0 {
              warnings.push(WARN_UNBOUNDED_WAB.to_string());
          }
          encode(&PauseResult {
              generation: o.generation,
              pause_requested: self.drain_ctl.pause_requested(),
              paused: o.paused(),
              held: o.held,
              paused_since_ms: (o.paused_since_ms != 0).then_some(o.paused_since_ms),
              held_segment: o.held_segment,
              warnings,
              note: PAUSE_NOTE.to_string(),
          })
      }

      async fn drain_resume(&self, args: &Map<String, Value>) -> Result<Value, AdminError> {
          let wait = wait_arg(args, DEFAULT_PAUSE_WAIT_MS)?;
          self.refuse_mutation()?;
          let (was_paused, since) = {
              let _g = self.mutate.lock().await;
              let was = self.drain_ctl.pause_requested();
              let since = drain_obs(&self.board).paused_since_ms;
              self.drain_ctl.request_resume();
              self.board.drain.set_pause_requested(false, None);
              (was, since)
          };
          let resumed = wait_until(wait, || !drain_obs(&self.board).paused()).await;
          let o = drain_obs(&self.board);
          encode(&ResumeResult {
              generation: o.generation,
              pause_requested: self.drain_ctl.pause_requested(),
              was_paused,
              paused_for_secs: (since != 0).then(|| unix_ms_now().saturating_sub(since) as f64 / 1000.0),
              resumed,
          })
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: 7 new + 3 earlier pass.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): drain.pause and drain.resume"`

---

### Task 6.11: `drain.retry_stranded`

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/verbs.rs`
- Test: `crates/weir-server/src/admin/verbs.rs`

**Interfaces:**
- Consumes (PR 2): `DrainControl::{request_requeue(&self) -> u64, requeue_requested(&self) -> u64}` (a request arriving while a ticket is unobserved folds into it and gets that ticket, spec §5.4 "Coalescing"); `crate::status::RequeueResult { ticket: u64, requeued: u64, already_queued: u64, scan_error: Option<String>, backoff_cut: bool, coalesced: bool, at_ms: u64 }` (PR 1's type, which PR 2 publishes); `drain_obs` (Task 6.10).
- Produces: `async fn drain_retry_stranded(&self, args: &Map<String, Value>) -> Result<Value, AdminError>` on `AdminState`.

- [ ] **Step 1: Write the failing test**

  ```rust
  /// Stands in for the drain's requeue observation (PR 2): publishes a
  /// RequeueResult for each new ticket.
  fn fake_requeue_drain(h: &Harness) -> tokio::task::JoinHandle<()> {
      let st = Arc::clone(&h.st);
      tokio::spawn(async move {
          let mut observed = 0u64;
          loop {
              let t = st.drain_ctl.requeue_requested();
              if t > observed {
                  st.board.drain.last_requeue.store(Some(Arc::new(crate::status::RequeueResult {
                      ticket: t,
                      requeued: 3,
                      already_queued: 1,
                      scan_error: None,
                      backoff_cut: true,
                      coalesced: false,
                      at_ms: unix_ms_now(),
                  })));
                  st.board.drain.observed_requeue.store(t, Ordering::Release);
                  observed = t;
              }
              tokio::time::sleep(Duration::from_millis(5)).await;
          }
      })
  }

  #[tokio::test]
  async fn retry_stranded_reports_the_observed_requeue() {
      let h = harness(ReloadMode::Live);
      let drain = fake_requeue_drain(&h);
      let res = h.st.drain_retry_stranded(&args(json!({}))).await.unwrap();
      assert!(res["ticket"].as_u64().unwrap() >= 1);
      assert_eq!(res["completed"], true);
      assert_eq!(res["requeued"], 3);
      assert_eq!(res["already_queued"], 1);
      assert_eq!(res["backoff_cut"], true);
      assert_eq!(res["coalesced"], false);
      assert_eq!(res["scan_error"], Value::Null);
      assert_eq!(res["paused"], false);
      assert!(res["note"].as_str().unwrap().contains("restart from record 0"));
      drain.abort();
  }

  #[tokio::test]
  async fn an_unobserved_retry_returns_its_ticket_without_counts() {
      let h = harness(ReloadMode::Live);
      let res = h.st.drain_retry_stranded(&args(json!({"wait_ms": 0}))).await.unwrap();
      assert!(res["ticket"].as_u64().unwrap() >= 1);
      assert_eq!(res["completed"], false);
      assert_eq!(res["requeued"], Value::Null);
      assert_eq!(res["already_queued"], Value::Null);
  }

  /// A request answered by a later scan reports itself coalesced, and the
  /// scan error (admin-only free text) is passed through.
  #[tokio::test]
  async fn a_retry_answered_by_a_newer_ticket_is_coalesced() {
      let h = harness(ReloadMode::Live);
      let first = h.st.drain_ctl.request_requeue();
      h.st.board.drain.last_requeue.store(Some(Arc::new(crate::status::RequeueResult {
          ticket: first + 1,
          requeued: 0,
          already_queued: 2,
          scan_error: Some("permission denied on shard-0".into()),
          backoff_cut: false,
          coalesced: true,
          at_ms: unix_ms_now(),
      })));
      h.st.board.drain.observed_requeue.store(first + 1, Ordering::Release);
      let res = h.st.drain_retry_stranded(&args(json!({"wait_ms": 500}))).await.unwrap();
      assert_eq!(res["completed"], true);
      assert_eq!(res["coalesced"], true);
      assert_eq!(res["scan_error"], "permission denied on shard-0");
  }

  #[tokio::test]
  async fn retry_stranded_is_refused_after_t0() {
      let h = harness(ReloadMode::Live);
      h.level_tx.send(ShutdownLevel::Graceful).unwrap();
      let e = h.st.drain_retry_stranded(&args(json!({"wait_ms": 0}))).await.unwrap_err();
      assert_eq!(e.code, ErrorCode::ShuttingDown);
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: compile failure, `no method named `drain_retry_stranded``.

- [ ] **Step 3: Implement**

  ```rust
  use weir_status::admin::{DEFAULT_RETRY_WAIT_MS, RETRY_NOTE, RetryStrandedResult};

  impl AdminState {
      async fn drain_retry_stranded(&self, args: &Map<String, Value>) -> Result<Value, AdminError> {
          let wait = wait_arg(args, DEFAULT_RETRY_WAIT_MS)?;
          self.refuse_mutation()?;
          let ticket = {
              let _g = self.mutate.lock().await;
              self.drain_ctl.request_requeue()
          };
          let completed = wait_until(wait, || drain_obs(&self.board).observed_requeue >= ticket).await;
          let o = drain_obs(&self.board);
          // The result that answered this ticket: its own, or a later scan's
          // (coalesced). An unobserved ticket reports no counts.
          let r = if completed { o.last_requeue.filter(|r| r.ticket >= ticket) } else { None };
          encode(&RetryStrandedResult {
              ticket,
              completed,
              coalesced: r.as_ref().is_some_and(|r| r.coalesced || r.ticket > ticket),
              requeued: r.as_ref().map(|r| r.requeued),
              already_queued: r.as_ref().map(|r| r.already_queued),
              backoff_cut: r.as_ref().is_some_and(|r| r.backoff_cut),
              scan_error: r.as_ref().and_then(|r| r.scan_error.clone()),
              paused: self.drain_ctl.pause_requested(),
              note: RETRY_NOTE.to_string(),
          })
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: 4 new + 10 earlier pass.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): drain.retry_stranded"`

---

### Task 6.12: the dispatcher — envelope, counting, logging, identity on every reply

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/verbs.rs` (`impl Dispatch for AdminState`, `AdminState::count`)
- Modify: `crates/weir-server/src/admin/mod.rs` (`test_support::call`)
- Test: `crates/weir-server/src/admin/verbs.rs`

**Interfaces:**
- Consumes: Task 6.7 (`Dispatch`, `parse_request`), Tasks 6.9-6.11.
- Produces: `impl Dispatch for AdminState` (`dispatch`, `instance`, `on_framing_error`); test-only `test_support::call(st: &AdminState, line: &str) -> Response`.

- [ ] **Step 1: Write the failing test**

  `admin/mod.rs` `test_support`:
  ```rust
  pub(crate) async fn call(st: &AdminState, line: &str) -> weir_status::admin::Response {
      use super::framing::Dispatch;
      st.dispatch(line.as_bytes(), uid()).await
  }
  ```
  `admin/verbs.rs` tests:
  ```rust
  use crate::admin::test_support::{call, requests};
  use crate::metrics::AdminOutcomeLabel;

  #[tokio::test]
  async fn every_verb_answers_through_the_envelope_with_identity() {
      let h = harness(ReloadMode::Live);
      for (verb, extra) in [
          ("status", ""),
          ("reload", ""),
          ("drain.pause", r#","args":{"wait_ms":0}"#),
          ("drain.resume", r#","args":{"wait_ms":0}"#),
          ("drain.retry_stranded", r#","args":{"wait_ms":0}"#),
      ] {
          let r = call(&h.st, &format!(r#"{{"v":1,"id":"{verb}","verb":"{verb}"{extra}}}"#)).await;
          assert!(r.ok, "{verb}: {r:?}");
          assert_eq!(r.id, json!(verb), "id echoed");
          assert_eq!(r.instance.as_ref().unwrap().pid, std::process::id(), "{verb}");
      }
      for verb in [
          AdminVerbLabel::Status,
          AdminVerbLabel::Reload,
          AdminVerbLabel::DrainPause,
          AdminVerbLabel::DrainResume,
          AdminVerbLabel::DrainRetryStranded,
      ] {
          assert_eq!(requests(&h.st.metrics, verb, AdminOutcomeLabel::ok), 1, "{verb:?}");
      }
  }

  #[tokio::test]
  async fn errors_carry_identity_and_are_counted() {
      let h = harness(ReloadMode::Live);
      for line in ["not json", r#"{"v":2,"verb":"status"}"#, r#"{"v":1,"verb":"nope"}"#] {
          let r = call(&h.st, line).await;
          assert!(!r.ok, "{line}");
          assert!(r.instance.is_some(), "identity on every response: {line}");
      }
      assert_eq!(requests(&h.st.metrics, AdminVerbLabel::Invalid, AdminOutcomeLabel::error), 3);

      h.level_tx.send(ShutdownLevel::Graceful).unwrap();
      let r = call(&h.st, r#"{"v":1,"id":5,"verb":"drain.pause","args":{"wait_ms":0}}"#).await;
      assert_eq!(r.id, json!(5));
      assert_eq!(r.error.unwrap().code, ErrorCode::ShuttingDown);
      assert!(r.instance.is_some());
      assert_eq!(requests(&h.st.metrics, AdminVerbLabel::DrainPause, AdminOutcomeLabel::error), 1);
      assert!(call(&h.st, r#"{"v":1,"verb":"status"}"#).await.ok, "status still works after T0");
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::verbs -- --test-threads=1`
  Expected: compile failure, `the trait bound `AdminState: Dispatch` is not satisfied`.

- [ ] **Step 3: Implement** (`admin/verbs.rs`)

  ```rust
  use tracing::{debug, info};
  use weir_status::admin::Response;
  use super::framing::{Dispatch, parse_request};
  use crate::metrics::{AdminOutcomeLabel, AdminRequestLabel};

  impl AdminState {
      fn count(&self, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) {
          self.metrics.admin_requests.get_or_create(&AdminRequestLabel { verb, outcome }).inc();
      }
  }

  impl Dispatch for AdminState {
      async fn dispatch(&self, line: &[u8], peer_uid: u32) -> Response {
          let req = match parse_request(line) {
              Ok(r) => r,
              Err((id, e)) => {
                  self.count(AdminVerbLabel::Invalid, AdminOutcomeLabel::error);
                  debug!(peer_uid, code = e.code.as_str(), "admin: invalid request");
                  return Response::failure(id, Some(self.instance.clone()), e);
              }
          };
          let result = match req.verb {
              Verb::Status => self.status(),
              Verb::Reload => self.reload().await,
              Verb::DrainPause => self.drain_pause(&req.args, peer_uid).await,
              Verb::DrainResume => self.drain_resume(&req.args).await,
              Verb::DrainRetryStranded => self.drain_retry_stranded(&req.args).await,
          };
          let outcome = match &result {
              Ok(_) => "ok",
              Err(e) => e.code.as_str(),
          };
          self.count(
              verb_label(req.verb),
              if result.is_ok() { AdminOutcomeLabel::ok } else { AdminOutcomeLabel::error },
          );
          if req.verb == Verb::Status {
              debug!(peer_uid, outcome, "admin status");
          } else {
              // spec §11: every mutating verb is logged at INFO with
              // {verb, peer_uid, outcome}; reload is logged the same way.
              info!(verb = req.verb.as_str(), peer_uid, outcome, "admin request");
          }
          match result {
              Ok(v) => Response::success(req.id, self.instance.clone(), v),
              Err(e) => Response::failure(req.id, Some(self.instance.clone()), e),
          }
      }

      fn instance(&self) -> Option<InstanceId> {
          Some(self.instance.clone())
      }

      fn on_framing_error(&self, _code: ErrorCode) {
          self.count(AdminVerbLabel::Invalid, AdminOutcomeLabel::error);
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin:: -- --test-threads=1`
  Expected: all admin tests pass.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): one dispatcher with identity on every reply, counted and logged"`

---

### Task 6.13: accept loop — peer check, 4-connection cap, `AdminHandle` with own-socket removal

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/mod.rs` (`MAX_CONNECTIONS`, `REFUSAL_WRITE_TIMEOUT`, `accept_loop`, `refuse`, `spawn`, `AdminHandle`)
- Test: `crates/weir-server/src/admin/mod.rs` `mod tests`

**Interfaces:**
- Consumes: `socket::{peer::peer_uid_of, is_accept_resource_exhaustion, ACCEPT_BACKOFF_ON_EXHAUSTION, remove_own_socket, BoundSocket}` (Task 6.4 visibility), Tasks 6.5-6.12.
- Produces:
  ```rust
  pub(crate) const MAX_CONNECTIONS: usize = 4;
  pub(crate) struct AdminHandle { /* path, socket: Arc<BoundSocket>, task: JoinHandle<()>, metrics */ }
  impl AdminHandle { pub(crate) fn close(self) }
  pub(crate) fn spawn(bound: BoundAdmin, state: Arc<AdminState>) -> AdminHandle
  ```
  `close` is what main calls at T3 (spec §7 "unlink the admin socket (inode-checked)"). It is synchronous and safe after the runtime has shut down: it aborts the task (a no-op if the runtime already cancelled it), removes the socket file only if it is still the inode this daemon bound (`remove_own_socket`), and sets `weir_admin_socket_up` to 0. The `Arc<BoundSocket>` keeps the listener and the Linux `O_PATH` pin alive until then.

- [ ] **Step 1: Write the failing test**

  ```rust
  mod accept {
      use super::super::*;
      use crate::admin::test_support::{ReloadMode, harness, requests};
      use crate::metrics::{AdminOutcomeLabel, AdminVerbLabel};
      use std::io::{BufRead, BufReader, Write};
      use std::os::unix::net::UnixStream as StdStream;
      use std::time::Duration;

      fn roundtrip(s: &StdStream, line: &str) -> serde_json::Value {
          let mut w = s.try_clone().unwrap();
          w.write_all(line.as_bytes()).unwrap();
          w.write_all(b"\n").unwrap();
          let mut r = BufReader::new(s.try_clone().unwrap());
          let mut out = String::new();
          r.read_line(&mut out).unwrap();
          serde_json::from_str(&out).unwrap()
      }

      #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
      async fn serves_status_and_sheds_the_fifth_connection() {
          let h = harness(ReloadMode::Live);
          let dir = crate::testutil::scratch_dir("admin_accept_busy");
          let path = dir.join("a.sock");
          let bound = bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap();
          let handle = spawn(bound, Arc::clone(&h.st));
          assert_eq!(h.st.metrics.admin_socket_up.get(), 1.0);

          let held: Vec<StdStream> = (0..MAX_CONNECTIONS)
              .map(|_| {
                  let s = StdStream::connect(&path).unwrap();
                  s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                  // A full round trip proves the server accepted and holds a permit.
                  let r = roundtrip(&s, r#"{"v":1,"id":1,"verb":"status"}"#);
                  assert_eq!(r["ok"], true, "{r}");
                  assert_eq!(r["instance"]["pid"], std::process::id());
                  s
              })
              .collect();

          let fifth = StdStream::connect(&path).unwrap();
          fifth.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
          let mut line = String::new();
          BufReader::new(fifth.try_clone().unwrap()).read_line(&mut line).unwrap();
          let r: serde_json::Value = serde_json::from_str(&line).unwrap();
          assert_eq!(r["error"]["code"], "busy");
          assert_eq!(r["error"]["retryable"], true);
          assert_eq!(requests(&h.st.metrics, AdminVerbLabel::Connect, AdminOutcomeLabel::busy), 1);

          drop(held);
          handle.close();
          assert!(!path.exists(), "close removes our own socket");
          assert_eq!(h.st.metrics.admin_socket_up.get(), 0.0);
          let _ = std::fs::remove_dir_all(&dir);
      }

      /// A successor that replaced the file keeps it (own-inode-only removal).
      #[tokio::test]
      async fn close_leaves_a_socket_it_did_not_bind() {
          let h = harness(ReloadMode::Live);
          let dir = crate::testutil::scratch_dir("admin_accept_successor");
          let path = dir.join("a.sock");
          let handle = spawn(bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap(), Arc::clone(&h.st));
          std::fs::remove_file(&path).unwrap();
          let successor = std::os::unix::net::UnixListener::bind(&path).unwrap();
          handle.close();
          assert!(path.exists(), "the successor's socket stays");
          drop(successor);
          let _ = std::fs::remove_dir_all(&dir);
      }

      /// spec §11 + Q8: a peer that is neither the daemon's uid nor root gets
      /// one `forbidden` line with no `instance`, then EOF, and the refusal is
      /// counted. Driven in-process by handing `accept_loop` a daemon euid
      /// that is not the test's own.
      #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
      async fn a_foreign_uid_gets_one_forbidden_line_without_identity_then_eof() {
          let me = crate::admin::test_support::uid();
          if me == 0 {
              return; // root is always admitted: the branch is unreachable as root
          }
          let h = harness(ReloadMode::Live);
          let dir = crate::testutil::scratch_dir("admin_accept_forbidden");
          let path = dir.join("a.sock");
          let bound = bind_admin(&path, Path::new("/run/weir/weir.sock")).unwrap();
          let socket = Arc::new(bound.socket);
          let task = tokio::spawn(accept_loop(
              Arc::clone(&socket),
              Arc::clone(&h.st),
              me + 1,
              Arc::clone(&h.st.metrics),
              framing::Limits::PRODUCTION,
          ));
          let s = StdStream::connect(&path).unwrap();
          s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
          let mut r = BufReader::new(s);
          let mut line = String::new();
          r.read_line(&mut line).unwrap();
          let v: serde_json::Value = serde_json::from_str(&line).unwrap();
          assert_eq!(v["error"]["code"], "forbidden");
          assert!(v.get("instance").is_none(), "an unauthorized peer learns nothing: {v}");
          line.clear();
          assert_eq!(r.read_line(&mut line).unwrap(), 0, "then the connection closes");
          assert_eq!(requests(&h.st.metrics, AdminVerbLabel::Connect, AdminOutcomeLabel::refused), 1);
          task.abort();
          socket::remove_own_socket(&path, &socket);
          let _ = std::fs::remove_dir_all(&dir);
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --bin weir-server admin::tests::accept -- --test-threads=1`
  Expected: compile failure, `cannot find function `spawn` in module`, `cannot find value `MAX_CONNECTIONS``, `cannot find function `accept_loop``.

- [ ] **Step 3: Implement** (`admin/mod.rs`)

  ```rust
  use std::{sync::Arc, time::Duration};

  use serde_json::Value;
  use tokio::{io::AsyncWriteExt, net::UnixStream, sync::Semaphore, task::{JoinHandle, JoinSet}};
  use tracing::warn;
  use weir_status::admin::{AdminError, ErrorCode, Response};

  use crate::metrics::{AdminOutcomeLabel, AdminRequestLabel, AdminVerbLabel, Metrics};
  use framing::{Dispatch, Limits};
  use verbs::AdminState;

  /// spec §11: at most 4 concurrent admin connections.
  pub(crate) const MAX_CONNECTIONS: usize = 4;
  /// A refusal is one short line into a fresh socket; never let it hold the
  /// accept loop.
  const REFUSAL_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

  pub(crate) struct AdminHandle {
      path: PathBuf,
      socket: Arc<BoundSocket>,
      task: JoinHandle<()>,
      metrics: Arc<Metrics>,
  }

  impl AdminHandle {
      /// T3 (spec §7): stop serving and remove the socket file if it is still
      /// the inode this daemon bound. Callable after the runtime shut down.
      pub(crate) fn close(self) {
          self.task.abort();
          socket::remove_own_socket(&self.path, &self.socket);
          self.metrics.admin_socket_up.set(0.0);
      }
  }

  pub(crate) fn spawn(bound: BoundAdmin, state: Arc<AdminState>) -> AdminHandle {
      let socket = Arc::new(bound.socket);
      let metrics = Arc::clone(&state.metrics);
      let euid = state.daemon_euid;
      metrics.admin_socket_up.set(1.0);
      let task = tokio::spawn(accept_loop(
          Arc::clone(&socket),
          state,
          euid,
          Arc::clone(&metrics),
          Limits::PRODUCTION,
      ));
      AdminHandle { path: bound.path, socket, task, metrics }
  }

  fn count(m: &Metrics, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) {
      m.admin_requests.get_or_create(&AdminRequestLabel { verb, outcome }).inc();
  }

  /// Serves until aborted (T3) or the runtime shuts down. The admin socket
  /// stays up through shutdown so `status` and `reload` keep answering.
  async fn accept_loop<D>(
      socket: Arc<BoundSocket>,
      d: Arc<D>,
      daemon_euid: u32,
      metrics: Arc<Metrics>,
      limits: Limits,
  ) where
      D: Dispatch + Send + Sync + 'static,
  {
      let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
      let mut conns: JoinSet<()> = JoinSet::new();
      loop {
          let stream = match socket.listener.accept().await {
              Ok((s, _)) => s,
              Err(e) => {
                  warn!(error = %e, "admin socket: accept error");
                  if socket::is_accept_resource_exhaustion(&e) {
                      tokio::time::sleep(socket::ACCEPT_BACKOFF_ON_EXHAUSTION).await;
                  }
                  continue;
              }
          };
          while conns.try_join_next().is_some() {}
          // spec §11: on EVERY accept, independent of peer_uid_check.
          let peer_uid = match identity::authorize_peer(socket::peer::peer_uid_of(&stream), daemon_euid) {
              Ok(uid) => uid,
              Err(why) => {
                  warn!(
                      ?why,
                      daemon_euid,
                      "admin socket: refusing a peer that is neither the daemon's uid nor root"
                  );
                  count(&metrics, AdminVerbLabel::Connect, AdminOutcomeLabel::refused);
                  // No `instance`: an unauthorized peer learns nothing.
                  refuse(stream, Response::failure(Value::Null, None, AdminError::new(ErrorCode::Forbidden, "forbidden"))).await;
                  continue;
              }
          };
          let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
              count(&metrics, AdminVerbLabel::Connect, AdminOutcomeLabel::busy);
              refuse(
                  stream,
                  Response::failure(
                      Value::Null,
                      d.instance(),
                      AdminError::new(
                          ErrorCode::Busy,
                          format!("the admin socket serves at most {MAX_CONNECTIONS} connections at once"),
                      ),
                  ),
              )
              .await;
              continue;
          };
          let d = Arc::clone(&d);
          conns.spawn(async move {
              let _permit = permit;
              framing::serve_connection(stream, peer_uid, &*d, &limits).await;
          });
      }
  }

  async fn refuse(mut stream: UnixStream, r: Response) {
      let _ = framing::write_response(&mut stream, &r, REFUSAL_WRITE_TIMEOUT).await;
      let _ = stream.shutdown().await;
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --bin weir-server admin:: -- --test-threads=1`
  Expected: all admin tests pass.

- [ ] **Step 5: Commit**

  `git commit -am "feat(admin): accept loop with peer check, connection cap and own-socket removal"`

---
### Task 6.14: serve the admin socket from startup step 7 and close it at T3

PR 6 runs Tasks 6.1-6.13 in its worktree. Tasks 6.14-6.17 start only after PR 5 has merged and PR 6 is rebased onto main.

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-server/src/admin/mod.rs` (`StartDeps`, `start`; remove the `#![allow(dead_code, …)]` added in Task 6.5)
- Modify: `crates/weir-server/src/main.rs` — as restructured by PR 4: step 4 (where `reload::Reloader::new` / `spawn_reload_loop` run; today main.rs:1449-1452 (inside rt.block_on at :1440; PR 4 moves it to step 4 with a [PR 6 slot] comment)), step 7 (after the TLS load and `reloader.register_tls`, before `wab::recover`), and the T3 `DrainWait::Joined` arm (PR 4's pre-serving skip path falls through to the same T0-T3 sequence; the orphan arm deliberately leaves a stale socket)
- Modify (remove the dead-code allowances listed under **Dead code until wiring** at the top of PR 6): `crates/weir-server/src/config/mod.rs`, `metrics/mod.rs`, `socket/mod.rs`, `reload.rs` (PR 6's own); `status/board.rs` (PR 1's on `impl AdminCell` and `impl ReloadCell`); `drain/control.rs` (PR 2's on `request_pause`, `request_resume`, `request_requeue`); `socket/mod.rs` (PR 3's on `refuse_if_live`)
- Modify: `crates/weir-testkit/src/harness.rs` (`WeirServer::admin_socket_path`, `WeirServer::admin`)
- Test: `crates/weir-server/tests/system.rs` (new section "Admin socket (Foundations PR 6)")

**Interfaces:**
- Consumes: PR 4's `Lifecycle::subscribe() -> watch::Receiver<ShutdownLevel>`, the `board`, `drain_ctl`, `reloader`, `reload_tx` locals of the restructured `main`, and the `_rt_guard = rt.enter()` execution model (spec §6) that lets `tokio::spawn` and tokio's sync `UnixListener::bind` run from main; PR 5's `Config::livez_checks: Vec<LivezCheck>` (PR 5 merges first; `HttpState` builds its thresholds internally, so this task builds `thresholds: Arc<Thresholds>` and `livez_checks: Arc<[LivezCheck]>` in `main` itself); PR 1's `board.statik.started_at_ms: u64` and `AdminCell::{set_disabled(&self), set_bound(&self, path: String), set_unavailable(&self, weir_status::AdminReason)}` with `weir_status::AdminReason { ParentInsecure, PathTooLong, InUse, BindFailed }`; Tasks 6.2, 6.6, 6.8, 6.13.

  *(Integrator reconciliation: the draft wrote an `AdminCellState { Disabled, Bound { path }, Unavailable { reason_code } }` enum through `AdminCell::store`. PR 1's `AdminCell` takes a closed `AdminReason` (it renders on the public document), so `start` maps this PR's reason-code strings onto it with `admin_reason` below: `collides_with_socket_path` is an `in_use` (the producer socket holds that name), and `no_parent` / `under_wab_dir`, which a derived path cannot produce, fall to `bind_failed`. The four public values are the ones PR 8's alert and runbook name.)*
- Produces:
  ```rust
  pub(crate) struct StartDeps {
      pub(crate) board: Arc<StatusBoard>,
      pub(crate) drain_ctl: DrainControl,
      pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
      pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
      pub(crate) thresholds: Arc<Thresholds>,
      pub(crate) livez_checks: Arc<[LivezCheck]>,
      pub(crate) metrics: Arc<Metrics>,
      pub(crate) started_at_ms: u64,
  }
  pub(crate) fn start(cfg: &Config, deps: StartDeps) -> io::Result<Option<AdminHandle>>
  // weir-testkit
  impl WeirServer { pub fn admin_socket_path(&self) -> PathBuf; pub fn admin(&self, verb: &str, args_json: &str) -> String }
  ```

- [ ] **Step 1: Write the failing test**

  `crates/weir-server/tests/system.rs`, new section at the end:
  ```rust
  // ── Admin socket (Foundations PR 6, spec §5.9 / §11) ─────────────────────────

  fn admin_json(srv: &weir_testkit::WeirServer, verb: &str, args: &str) -> serde_json::Value {
      let line = srv.admin(verb, args);
      serde_json::from_str(&line).unwrap_or_else(|e| panic!("not one JSON line ({e}): {line:?}"))
  }

  /// A counter or gauge sample from a scrape; 0 if absent.
  fn scrape_value(srv: &weir_testkit::WeirServer, series: &str) -> f64 {
      srv.scrape_metrics()
          .lines()
          .find(|l| l.starts_with(series) && l[series.len()..].starts_with(' '))
          .and_then(|l| l.split_whitespace().next_back())
          .and_then(|v| v.parse().ok())
          .unwrap_or(0.0)
  }

  #[test]
  fn admin_socket_serves_status_at_the_derived_path_and_is_removed_at_exit() {
      use std::os::unix::fs::{FileTypeExt, MetadataExt};
      let mut srv = weir_server!("admin_status").start();
      let path = srv.admin_socket_path();
      let meta = fs::symlink_metadata(&path).expect("the admin socket exists at the derived path");
      assert!(meta.file_type().is_socket());
      assert_eq!(meta.mode() & 0o777, 0o600);

      let r = admin_json(&srv, "status", "{}");
      assert_eq!(r["ok"], true, "{r}");
      assert_eq!(r["instance"]["pid"], srv.child_pid());
      assert_eq!(
          r["instance"]["wab_dir"],
          fs::canonicalize(&srv.wab_dir).unwrap().display().to_string()
      );
      assert_eq!(r["result"]["schema"], "weir.status/v1");
      assert_eq!(r["result"]["view"], "admin");
      assert_eq!(scrape_value(&srv, "weir_admin_socket_up"), 1.0);

      srv.sigterm();
      assert!(!path.exists(), "a clean exit removes the admin socket (inode-checked)");
  }

  /// spec §5.10: SIGHUP and the admin verb are one reload path.
  #[test]
  fn admin_reload_and_sighup_share_one_reload_path() {
      let srv = weir_server!("admin_reload").start();
      let r = admin_json(&srv, "reload", "{}");
      assert_eq!(r["ok"], true, "{r}");
      assert_eq!(r["result"]["trigger"], "admin");
      assert_eq!(r["result"]["outcome"], "noop");
      assert_eq!(
          scrape_value(&srv, "weir_config_reloads_total{trigger=\"admin\",outcome=\"noop\"}"),
          1.0
      );
      send_sighup(srv.child_pid());
      assert_eq!(wait_for_sighup_reloads(&srv, "noop", 1), 1);
      let s = admin_json(&srv, "status", "{}");
      assert_eq!(s["result"]["reload"]["last"]["trigger"], "sighup", "{s}");
  }

  /// spec §6 step 7: an explicitly configured admin path that cannot be
  /// bound safely is fatal, before recovery.
  #[test]
  fn an_explicit_admin_path_in_a_shared_directory_refuses_to_start() {
      use std::os::unix::fs::PermissionsExt;
      let shared = std::env::temp_dir().join(format!("weir_admin_shared_{}", std::process::id()));
      fs::create_dir_all(&shared).unwrap();
      fs::set_permissions(&shared, fs::Permissions::from_mode(0o770)).unwrap();
      let sock = shared.join("a.sock");
      let (status, log) = weir_server!("admin_explicit_shared")
          .extra_config(format!("admin_socket_path = \"{}\"", sock.display()))
          .spawn_expecting_exit(Duration::from_secs(15));
      assert_eq!(status.code(), Some(1), "{log}");
      assert!(log.contains("group- or world-writable"), "{log}");
      assert!(!sock.exists(), "nothing was bound");
      let _ = fs::remove_dir_all(&shared);
  }

  /// spec §6 step 7: the derived default failing disables admin, not the
  /// daemon (and raises admin_unavailable).
  #[test]
  fn a_derived_admin_path_in_a_shared_parent_disables_admin_not_the_daemon() {
      use std::os::unix::fs::PermissionsExt;
      // Short label: <canonical temp dir>/…/wab.admin.sock must stay <= 103
      // bytes on macOS, or the derived path is path_too_long instead.
      let shared = std::env::temp_dir().join(format!("weir_adm_sh_{}", std::process::id()));
      let wab = shared.join("wab");
      fs::create_dir_all(&wab).unwrap();
      fs::set_permissions(&shared, fs::Permissions::from_mode(0o777)).unwrap();
      let srv = weir_server!("admin_derived_shared")
          .wab_dir(wab.clone())
          .log_level("info")
          .start();
      let mut c = srv.client();
      c.push(b"still-serving", Durability::Durable).expect("ingest is unaffected");
      drop(c);
      assert!(!shared.join("wab.admin.sock").exists());
      assert_eq!(scrape_value(&srv, "weir_admin_socket_up"), 0.0);
      let log = fs::read_to_string(srv.tmp_dir.join("weir.log")).unwrap_or_default();
      assert!(log.contains("admin socket unavailable"), "{log}");
      assert!(log.contains("parent_insecure"), "{log}");
      drop(srv);
      let _ = fs::remove_dir_all(&shared);
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --test system admin_ -- --test-threads=1`
  Expected: compile failure, `no method named `admin_socket_path` found for struct `WeirServer``; after the testkit helpers exist, `admin_socket_serves_status…` fails with "the admin socket exists at the derived path: … No such file or directory" (nothing binds it yet).

- [ ] **Step 3: Implement**

  `crates/weir-testkit/src/harness.rs`, `impl WeirServer`:
  ```rust
  /// The admin socket the daemon derives from its `wab_dir`
  /// (`<parent>/<name>.admin.sock`, Foundations spec §5.9). With the
  /// harness's default layout that is `tmp_dir/wab.admin.sock`.
  pub fn admin_socket_path(&self) -> PathBuf {
      let name = self.wab_dir.file_name().expect("wab_dir has a final component");
      let mut file = name.to_os_string();
      file.push(".admin.sock");
      self.wab_dir.parent().expect("wab_dir has a parent").join(file)
  }

  /// Sends one admin request `{"v":1,"id":1,"verb":<verb>,"args":<args_json>}`
  /// on a fresh connection and returns the response line (without `\n`).
  /// Waits up to 40 s, longer than the 30 s maximum `wait_ms`.
  pub fn admin(&self, verb: &str, args_json: &str) -> String {
      use std::io::{BufRead, BufReader, Write};
      let mut s = std::os::unix::net::UnixStream::connect(self.admin_socket_path())
          .expect("connect to the admin socket");
      s.set_read_timeout(Some(Duration::from_secs(40))).unwrap();
      writeln!(s, r#"{{"v":1,"id":1,"verb":"{verb}","args":{args_json}}}"#).unwrap();
      let mut line = String::new();
      BufReader::new(s).read_line(&mut line).expect("read the admin response");
      line.trim_end().to_string()
  }
  ```

  `admin/mod.rs`:
  ```rust
  use tokio::sync::{mpsc, watch};
  use tracing::{error, info};
  use weir_status::{LivezCheck, Thresholds};

  use crate::{
      config::{AdminSocketSetting, Config},
      drain::control::DrainControl,
      lifecycle::ShutdownLevel,
      reload::ReloadRequest,
      status::StatusBoard,
  };
  use weir_status::AdminReason;

  /// PR 1's closed `AdminReason` (it renders on the public `/status`) for one
  /// of this module's reason codes.
  fn admin_reason(code: &str) -> AdminReason {
      match code {
          reason::PARENT_INSECURE => AdminReason::ParentInsecure,
          "path_too_long" => AdminReason::PathTooLong,
          reason::IN_USE | reason::COLLIDES_WITH_SOCKET_PATH => AdminReason::InUse,
          _ => AdminReason::BindFailed,
      }
  }

  pub(crate) struct StartDeps {
      pub(crate) board: Arc<StatusBoard>,
      pub(crate) drain_ctl: DrainControl,
      pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
      pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
      pub(crate) thresholds: Arc<Thresholds>,
      pub(crate) livez_checks: Arc<[LivezCheck]>,
      pub(crate) metrics: Arc<Metrics>,
      pub(crate) started_at_ms: u64,
  }

  /// Startup step 7 (spec §6): resolve, bind and serve the admin socket.
  /// `Err` only for an explicitly configured path (fatal, before recovery);
  /// a derived default that cannot be used disables admin with an ERROR and
  /// `AdminCell::set_unavailable`, which raises `admin_unavailable`.
  pub(crate) fn start(cfg: &Config, deps: StartDeps) -> io::Result<Option<AdminHandle>> {
      let (path, explicit) = match cfg.admin_socket() {
          AdminSocketSetting::Disabled => {
              deps.board.admin.set_disabled();
              info!("admin socket disabled (admin_socket_enabled = false)");
              return Ok(None);
          }
          AdminSocketSetting::Explicit(p) => (p, true),
          AdminSocketSetting::Derived(p) => (p, false),
          AdminSocketSetting::DerivedUnusable { reason_code, path } => {
              error!(
                  reason_code,
                  path = ?path,
                  "admin socket unavailable: the default path derived from wab_dir cannot be \
                   used; set admin_socket_path to a file in a directory writable only by the \
                   daemon user, or admin_socket_enabled = false"
              );
              deps.board.admin.set_unavailable(admin_reason(reason_code));
              return Ok(None);
          }
      };
      let bound = match bind_admin(&path, &cfg.socket_path) {
          Ok(b) => b,
          Err(e) if explicit => {
              return Err(io::Error::new(
                  e.source.kind(),
                  format!("admin_socket_path '{}': {}", path.display(), e.source),
              ));
          }
          Err(e) => {
              error!(
                  path = %path.display(),
                  reason_code = e.reason_code,
                  error = %e.source,
                  "admin socket unavailable; the daemon runs without it. Set admin_socket_path to \
                   a file in a directory writable only by the daemon user, or \
                   admin_socket_enabled = false"
              );
              deps.board.admin.set_unavailable(admin_reason(e.reason_code));
              return Ok(None);
          }
      };
      let instance = identity::instance_id(&cfg.wab_dir, &cfg.socket_path, deps.started_at_ms)?;
      let state = Arc::new(AdminState {
          board: Arc::clone(&deps.board),
          drain_ctl: deps.drain_ctl,
          reload_tx: deps.reload_tx,
          level_rx: deps.level_rx,
          thresholds: deps.thresholds,
          livez_checks: deps.livez_checks,
          instance,
          metrics: deps.metrics,
          wab_max_bytes: cfg.wab_max_bytes,
          // Safety: geteuid has no preconditions.
          daemon_euid: unsafe { libc::geteuid() },
          mutate: tokio::sync::Mutex::new(()),
      });
      deps.board.admin.set_bound(bound.path.display().to_string());
      info!(path = %bound.path.display(), "admin socket listening");
      Ok(Some(spawn(bound, state)))
  }
  ```

  `main.rs` (PR 4's structure; the spec-step comments PR 4 adds are the anchors):
  ```rust
  // §6 step 4, where PR 4 builds the reload path:
  let reloader = reload::Reloader::new(Arc::clone(&metrics), tcp_configured);
  reloader.attach_board(Arc::clone(&board));
  let reload_tx = reload::spawn_reload_loop(Arc::clone(&reloader));
  // The admin verb's handle on the SAME queue SIGHUP feeds (spec §5.10).
  #[cfg(unix)]
  let admin_reload_tx = reload_tx.clone();
  // … reload_tx then moves into reload::spawn_sighup_task, unchanged (Q4) …

  // §6 step 7, after step 6 (TLS load + reloader.register_tls), before step 8.
  // The same thresholds and liveness checks the HTTP router (PR 5) uses.
  #[cfg(unix)]
  let thresholds = Arc::new(weir_status::Thresholds::DEFAULT);
  #[cfg(unix)]
  let livez_checks: Arc<[weir_status::LivezCheck]> = Arc::from(config.livez_checks.clone());
  #[cfg(unix)]
  let admin_handle = admin::start(
      &config,
      admin::StartDeps {
          board: Arc::clone(&board),
          drain_ctl: drain_ctl.clone(),
          reload_tx: admin_reload_tx,
          level_rx: lifecycle.subscribe(),
          thresholds: Arc::clone(&thresholds),
          livez_checks: Arc::clone(&livez_checks),
          metrics: Arc::clone(&metrics),
          started_at_ms: board.statik.started_at_ms,
      },
  )
  .inspect_err(|e| tracing::error!("{e}"))?;

  // §7 T3, the `DrainWait::Joined` arm (PR 4's [PR 6 slot]), after
  // rt.shutdown_timeout(1 s) and BEFORE dropping wab_lock (spec: "unlink the
  // admin socket (inode-checked)"). This is the only close site: PR 4's
  // pre-serving skip path falls through to the same T0-T3 sequence.
  #[cfg(unix)]
  if let Some(a) = admin_handle {
      a.close();
  }
  // §7 orphan arm (drain NOT joined, std::process::exit(0)): deliberately not
  // closed. The file is left as a stale socket, which the next start's
  // refuse_if_live / bind_hardened probes as Stale and replaces.
  ```
  Now that `main` reaches the admin module, remove every dead-code allowance listed under **Dead code until wiring** at the top of PR 6: the `#![allow(dead_code, …)]` from `admin/mod.rs`; the item allowances Tasks 6.2 (`AdminSocketSetting`, `Config::admin_socket`, `AdminPathProblem::reason_code`), 6.3 (`Metrics::{admin_requests, admin_socket_up}`), 6.4 (`ParentPolicy`, `is_insecure_parent`) and 6.8 (`ReloadTrigger::Admin`, `ReloadReport::to_wire`, `Reloader::attach_board`, `ReloadWaitError`, `request_reload_and_wait`, `ADMIN_ENQUEUE_TIMEOUT`, `ADMIN_REPLY_TIMEOUT`, `ITEM_TLS_CERTIFICATES`) added; PR 1's `#[cfg_attr(not(test), allow(dead_code))]` on `impl AdminCell` and `impl ReloadCell` (status/board.rs); PR 2's allowance on `DrainControl::{request_pause, request_resume, request_requeue}` (drain/control.rs; leave it on the items only PR 4 calls); and PR 3's `#[cfg_attr(not(test), allow(dead_code, …))]` on `socket::refuse_if_live` (Task 6.6's `bind_admin` is now its production caller). Every one of them is reachable now, and both `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` and `cargo clippy --all-targets -- -D warnings` must pass without them. Verified against 2491883: the lock-integrity thread starts at main.rs:997-1004, immediately after acquire_wab_dir_lock and before preflight and the runtime; step 7 adds nothing before it and does not touch it.

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --test system admin_ -- --test-threads=1` then the whole `cargo test -p weir-server --test system -- --test-threads=1`
  Expected: the 4 new tests pass, and every existing system test still passes (each daemon now also binds `tmp_dir/wab.admin.sock`; the harness's `tmp_dir` is created 0755 by `create_dir_all`, which passes the private-parent rule).

  Run: `cargo clippy -p weir-server --all-targets --all-features -- -D warnings && cargo clippy --all-targets -- -D warnings`
  Expected: no warnings, with every allowance above removed.

- [ ] **Step 5: Commit**

  `git commit -am "feat(server): serve the admin socket from startup and remove it at exit"`

---

### Task 6.15: system tests 6 and 7 — pause holds delivery while ingest acks; retry-stranded redelivers

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Modify: `crates/weir-testkit/src/recording_sink.rs` (`RecordingSink::start_on(port)`)
- Test: `crates/weir-server/tests/system.rs` (admin section)

**Interfaces:**
- Consumes: Task 6.14's `WeirServer::admin`, `admin_json`, `scrape_value`; the existing `RecordingSink`, `free_port`; PR 4 Task 4.3's `recording_sink::Mode { Answer { delay }, Hang }` and `start_mode(mode: Mode)`.
- Produces: `pub fn RecordingSink::start_on(port: u16) -> RecordingSink` (weir-testkit).

- [ ] **Step 1: Write the failing test**

  ```rust
  /// spec §16 system test 6: pause, then 1,000 pushes, then resume. Ingest
  /// keeps acking while delivery holds; everything arrives after resume.
  #[test]
  fn admin_pause_holds_delivery_while_ingest_acks_and_resume_delivers_all() {
      let sink = RecordingSink::start();
      let mut srv = weir_server!("admin_pause")
          .extra_config("sink_type = \"http\"")
          // Small segments, so sealed segments exist during the pushes and
          // an unpaused drain WOULD deliver them.
          .extra_config("wab_segment_max_bytes = 4096")
          .env("WEIR_SINK_URL", sink.url())
          .start();
      let r = admin_json(&srv, "drain.pause", r#"{"wait_ms":5000}"#);
      assert_eq!(r["ok"], true, "{r}");
      assert_eq!(r["result"]["paused"], true, "an idle drain observes a pause within a 50 ms slice: {r}");

      let before = sink.bodies().len();
      let payloads: Vec<String> = (0..1000).map(|i| format!("paused-{i:04}-{}", "x".repeat(48))).collect();
      let mut c = srv.client();
      for p in &payloads {
          c.push(p.as_bytes(), Durability::Durable).expect("ingest keeps acking while paused");
      }
      drop(c);
      thread::sleep(Duration::from_secs(1));
      assert_eq!(sink.bodies().len(), before, "nothing is delivered while paused");
      assert_eq!(admin_json(&srv, "status", "{}")["result"]["drain"]["paused"], true);

      let r = admin_json(&srv, "drain.resume", r#"{"wait_ms":5000}"#);
      assert_eq!(r["result"]["was_paused"], true, "{r}");
      assert_eq!(r["result"]["resumed"], true, "{r}");

      // SIGTERM seals the active segment; the drain budget (10 s default)
      // covers 1,000 local POSTs with room to spare.
      srv.sigterm();
      let delivered: std::collections::HashSet<String> = sink
          .bodies()
          .into_iter()
          .map(|b| String::from_utf8_lossy(&b).into_owned())
          .collect();
      let missing: Vec<&String> = payloads.iter().filter(|p| !delivered.contains(*p)).collect();
      assert!(missing.is_empty(), "{} of 1000 never delivered, e.g. {:?}", missing.len(), missing.first());
  }

  /// spec §16 system test 7: a stranded segment is redelivered by
  /// drain.retry_stranded, without waiting for the (here hour-long) health
  /// poll that would otherwise resume it.
  #[test]
  fn admin_retry_stranded_redelivers_without_waiting_for_the_health_poll() {
      let port = free_port(); // nothing listens yet: every delivery is refused
      let srv = weir_server!("admin_retry")
          .extra_config("sink_type = \"http\"")
          .extra_config("sink_max_retries = 0")
          .extra_config("health_poll_interval_secs = 3600")
          .extra_config("wab_segment_max_bytes = 4096")
          .env("WEIR_SINK_URL", format!("http://127.0.0.1:{port}/sink"))
          .start();
      let mut c = srv.client();
      for i in 0..40 {
          c.push(format!("stranded-{i:02}-{}", "y".repeat(200)).as_bytes(), Durability::Durable)
              .unwrap();
      }
      drop(c);
      let deadline = Instant::now() + Duration::from_secs(15);
      while scrape_value(&srv, "weir_drain_segments_stranded_total") < 1.0 && Instant::now() < deadline {
          thread::sleep(Duration::from_millis(50));
      }
      assert!(scrape_value(&srv, "weir_drain_segments_stranded_total") >= 1.0, "nothing stranded");

      let sink = RecordingSink::start_on(port);
      let r = admin_json(&srv, "drain.retry_stranded", r#"{"wait_ms":10000}"#);
      assert_eq!(r["ok"], true, "{r}");
      assert_eq!(r["result"]["completed"], true, "{r}");
      assert!(r["result"]["requeued"].as_u64().unwrap_or(0) >= 1, "{r}");

      let deadline = Instant::now() + Duration::from_secs(15);
      while sink.bodies().is_empty() && Instant::now() < deadline {
          thread::sleep(Duration::from_millis(50));
      }
      assert!(
          sink.bodies().iter().any(|b| b.starts_with(b"stranded-")),
          "the requeued segment reached the recovered sink"
      );
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --test system -- admin_pause admin_retry --test-threads=1`
  Expected: compile failure, `no function or associated item named `start_on` found for struct `RecordingSink``. (With `start_on` present and the PR 2 drain in place both pass; if PR 2's pause were not wired through the board, `admin_pause…` fails at `paused == true`.)

- [ ] **Step 3: Implement** (`recording_sink.rs`)

  PR 4 Task 4.3 already turned `start_mode(hang: bool)` into `start_mode(mode: Mode)` with `enum Mode { Answer { delay }, Hang }`, which `start`, `start_hanging` and `start_slow` call. This task keeps that and only moves the bind address into a parameter:

  ```rust
  /// Like [`start`](Self::start), on a fixed loopback port: for a test that
  /// points the daemon at a sink that is down at first and comes up later.
  /// Panics if the port was taken in between.
  pub fn start_on(port: u16) -> RecordingSink {
      Self::start_mode_at(("127.0.0.1", port), Mode::Answer { delay: std::time::Duration::ZERO })
  }

  fn start_mode(mode: Mode) -> RecordingSink {
      Self::start_mode_at(("127.0.0.1", 0), mode)
  }

  fn start_mode_at(addr: (&str, u16), mode: Mode) -> RecordingSink {
      let listener = TcpListener::bind(addr).expect("bind recording sink");
      // … the rest of PR 4's start_mode body, unchanged, matching on `mode`
      // (`Mode::Hang => hold(conn, &s)`, `Mode::Answer { delay } => serve(conn, &b, delay)`) …
  }
  ```

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --test system admin_ -- --test-threads=1`
  Expected: all 6 admin system tests pass.

- [ ] **Step 5: Commit**

  `git commit -am "test(system): pause holds delivery while ingest acks; retry-stranded redelivers"`

---

### Task 6.16: admin reload of TLS material (tls_listener.rs)

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Test: `crates/weir-server/tests/tls_listener.rs` (beside `sighup_reloads_tls_when_configured`, ~line 309)

**Interfaces:**
- Consumes: Task 6.14's `WeirServer::admin`; the file's existing `metric`, `acked`, `client_config_with_cert` helpers and `TlsFixture`.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

  ```rust
  /// spec §16 (Admin): reload with good certificates → applied; a corrupt key
  /// → failed, and the certificates in force still handshake.
  #[test]
  fn admin_reload_applies_rotated_certs_and_keeps_them_on_a_corrupt_key() {
      let fx = TlsFixture::generate("admin_rotate_old");
      let next = TlsFixture::generate("admin_rotate_new");
      // Short label: the derived <tmp>/weir_test_<label>_<pid>/wab.admin.sock
      // must stay <= 103 bytes on macOS, or admin is disabled (path_too_long).
      let srv = weir_server!("tls_adm_rl").tls(&fx).start();
      let addr = srv.tcp_addr();
      let new_client = client_config_with_cert(
          &next.ca_cert_path,
          &next.client_cert_path,
          &next.client_key_path,
      );
      for (from, to) in [
          (&next.server_cert_path, &fx.server_cert_path),
          (&next.server_key_path, &fx.server_key_path),
          (&next.ca_cert_path, &fx.ca_cert_path),
      ] {
          std::fs::copy(from, to).unwrap();
      }

      let r: serde_json::Value = serde_json::from_str(&srv.admin("reload", "{}")).unwrap();
      assert_eq!(r["ok"], true, "{r}");
      assert_eq!(r["result"]["outcome"], "applied", "{r}");
      assert_eq!(r["result"]["items"][0]["item"], "tls_certificates");
      assert_eq!(r["result"]["items"][0]["outcome"], "reloaded");
      assert_eq!(metric(&srv, "weir_config_reloads_total{trigger=\"admin\",outcome=\"ok\"}"), 1);
      assert!(acked(addr, Arc::clone(&new_client)), "the rotated identity is served");

      std::fs::write(&fx.server_key_path, "not a key").unwrap();
      let r: serde_json::Value = serde_json::from_str(&srv.admin("reload", "{}")).unwrap();
      assert_eq!(r["ok"], true, "a failed reload is ok:true with outcome failed: {r}");
      assert_eq!(r["result"]["outcome"], "failed");
      assert_eq!(r["result"]["items"][0]["outcome"], "failed");
      assert!(r["result"]["items"][0]["detail"].is_string(), "admin detail says why");
      assert_eq!(metric(&srv, "weir_config_reloads_total{trigger=\"admin\",outcome=\"failed\"}"), 1);
      assert!(acked(addr, new_client), "the certificates in force still handshake");
      srv.shutdown();
  }
  ```
  (`serde_json` is already a weir-server dev-dependency.)

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --features tls --test tls_listener admin_reload -- --test-threads=1`
  Expected: before Tasks 6.8/6.14 it fails to compile (`no method named `admin``); with them in place it passes on first run. Mutation check: temporarily make `ReloadReport::to_wire` map `ReloadOutcome::Ok` to `Noop`, see `assert_eq!(r["result"]["outcome"], "applied")` fail, restore.

- [ ] **Step 3: Implement**

  No production change: this pins the admin path end to end over a real TLS daemon. If it fails, the defect is in Task 6.8 (`to_wire` or the reload queue) or Task 6.14 (`admin_reload_tx` is not a clone of the queue the reloader serves).

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --features tls --test tls_listener -- --test-threads=1`
  Expected: every TLS listener test passes, including `sighup_reloads_tls_when_configured` and the new one.

- [ ] **Step 5: Commit**

  `git commit -am "test(tls): admin reload applies rotated certs and keeps them on a corrupt key"`

---

### Task 6.17: docs — threat model, socket and container docs, configuration; the admin config-key guard; security review

*PR-level merge gate: PR 6's **Merge gate** (top of the PR 6 section), run after rebasing onto PR 5; includes the security review in Task 6.17.*

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (new guard beside `the_wab_dir_lock_key_is_in_the_configuration_reference`, line ~796)
- Modify: `docs/operations/configuration.md` (two `####` key sections, after `wab_dir_lock` at line ~227)
- Modify: `deploy/docker/weir.toml.example` (two commented keys)
- Modify: `docs/security/threat-model.md` (trust-boundary subsection, two In-scope rows, actors, the root reconciliation, spec §17 items 2-4)
- Modify: `docs/security/socket-bind.md` (new `## The admin socket` section)
- Modify: `docs/security/container.md` (admin socket under `/var/lib/weir`, not the `/run/weir` VOLUME; `readOnlyRootFilesystem` note)

**Interfaces:**
- Consumes: Tasks 6.2 (`WEIR_ADMIN_SOCKET_ENABLED`, `WEIR_ADMIN_SOCKET_PATH`, `--admin-socket-path`), 6.4, 6.13.
- Produces: docs only. PR 8 owns the general config-key guard and the threat-model row guard (spec §16); this guard is the admin-key slice of it and PR 8 may fold it in.

- [ ] **Step 1: Write the failing test**

  ```rust
  /// The admin socket is on by default and refuses an insecure parent, so an
  /// operator who meets `admin_unavailable` must find both keys, under the
  /// spellings the daemon parses, in the reference and the example file.
  #[test]
  fn the_admin_socket_keys_are_in_the_configuration_reference() {
      assert!(CONFIG_ENV_SRC.contains("\"WEIR_ADMIN_SOCKET_ENABLED\""));
      assert!(CONFIG_ENV_SRC.contains("\"WEIR_ADMIN_SOCKET_PATH\""));
      assert!(CONFIG_CLI_SRC.contains("\"--admin-socket-path\""));
      for needle in [
          "#### `admin_socket_enabled`",
          "#### `admin_socket_path`",
          "`WEIR_ADMIN_SOCKET_ENABLED`",
          "`WEIR_ADMIN_SOCKET_PATH`",
          "`--admin-socket-path",
          ".admin.sock",
      ] {
          assert!(
              CONFIGURATION.contains(needle),
              "docs/operations/configuration.md must document the admin socket keys \n\
               (missing {needle:?})"
          );
      }
      for needle in ["admin_socket_enabled", "admin_socket_path", "WEIR_ADMIN_SOCKET_PATH"] {
          assert!(
              CONFIG_EXAMPLE.contains(needle),
              "deploy/docker/weir.toml.example claims to list every option; it is missing {needle:?}"
          );
      }
  }
  ```

- [ ] **Step 2: Run it to verify it fails**

  Run: `cargo test -p weir-server --test docs_drift the_admin_socket_keys`
  Expected: FAIL, `docs/operations/configuration.md must document the admin socket keys (missing "#### `admin_socket_enabled`")`.

- [ ] **Step 3: Implement**

  `docs/operations/configuration.md`, after the `wab_dir_lock` section:
  ```markdown
  #### `admin_socket_enabled`

  - **Type**: bool
  - **Default**: `true`
  - **CLI**: none
  - **Env**: `WEIR_ADMIN_SOCKET_ENABLED`
  - **TOML**: `admin_socket_enabled`

  Serve the admin Unix socket: `weir-ctl status`, `reload` and `drain pause|resume|retry-stranded` talk to it. Only the daemon's own uid and root may connect. Set `false` to run without it; nothing else changes.

  #### `admin_socket_path`

  - **Type**: absolute path, at most 103 bytes
  - **Default**: `<parent of wab_dir>/<name of wab_dir>.admin.sock` — `/var/lib/weir/wab.admin.sock` for the default `wab_dir`
  - **Validation**: absolute, no `..`, not inside `wab_dir`, not the same file as `socket_path` (compared after removing `.` and doubled `/`, and again by parent directory identity, so `/var/run/x` and `/run/x` collide)
  - **CLI**: `--admin-socket-path <path>`
  - **Env**: `WEIR_ADMIN_SOCKET_PATH` (weir-ctl reads the same variable)
  - **TOML**: `admin_socket_path`

  The default deliberately avoids the producer socket's directory: `/run/weir` is shared with producer containers by design, and a socket there would be reachable by every producer running as the daemon's uid. The parent directory must be writable only by the daemon user (not group- or world-writable, owned by the daemon's uid or root). An explicit path that fails this, or any other bind check, stops startup before recovery; the derived default failing only disables the admin socket, logs an ERROR naming the reason, and raises the `admin_unavailable` condition. Under Kubernetes with `readOnlyRootFilesystem`, set this to a private `emptyDir` that is not mounted into producer containers.
  ```

  `deploy/docker/weir.toml.example`, after `wab_dir_lock`:
  ```toml
  # Admin Unix socket for weir-ctl status / reload / drain (daemon uid and root only).
  # Default path: <parent of wab_dir>/<name of wab_dir>.admin.sock. The parent
  # directory must be writable only by the daemon user. Env: WEIR_ADMIN_SOCKET_ENABLED,
  # WEIR_ADMIN_SOCKET_PATH.
  # admin_socket_enabled = true
  # admin_socket_path = "/var/lib/weir/wab.admin.sock"
  ```

  `docs/security/threat-model.md`:
  - New subsection under `## Trust boundaries`, after `### Unix socket path`: `### Admin socket` — mode 0600 at a path derived from `wab_dir`, never on the producer volume; the bind refuses a parent that is group- or world-writable or owned by another non-root user; on every accept the peer euid must be the daemon's or 0 (always on, independent of `peer_uid_check`, fail-closed without peer credentials); framing bounds (64 KiB lines enforced while reading, 5 s per request, 10 s idle, 64 requests per connection, 4 connections); every reply to an authorized peer carries the daemon identity, and a refused peer gets one `forbidden` line with no identity; no secrets are reachable (the status document's admin view excludes bearer tokens, S3 secrets and URL passwords by type).
  - **Root reconciliation** (lines 24-30): after "Root is **not** implicitly trusted" for producers, add the bullet: "- The admin socket deliberately also admits root (euid 0) besides the daemon's own uid: `sudo weir-ctl drain pause` is an expected way to call it. Root can already read and rewrite the WAB and signal the daemon, so admitting it grants nothing new." (PR 8 Task 8.12's guard looks for the phrase "The admin socket deliberately also admits root".)
  - One `## In scope` row titled exactly `| Local process reaching the admin socket |` (the title PR 8 Task 8.12's guard pins), whose defense covers both threats: an unauthorized local process driving the socket (the 0600 mode, the private parent and the euid-or-root check, fail-closed, `weir_admin_requests_total{verb="_connect",outcome="refused"}` and a WARN; bounded framing; no secrets) and a squatter impersonating it (the bind refuses a non-private parent and a live socket; weir-ctl authenticates the server by `lstat` mode, parent mode, peer uid and `wab_dir` (dev, ino), spec §11).
  - `## Threat actors considered`: add the bullet "- **Producer or sidecar sharing the socket volume**: no admin access — the admin socket is not on that volume, and a same-uid producer in another container cannot reach `/var/lib/weir`." Qualify "**Compromised process running as the daemon uid**" by appending: "This does not extend to producer containers that share the daemon's uid only so that `peer_uid_check` admits them: they reach the producer socket, never the admin socket or the WAB, as long as the `wab_dir` parent is not mounted into them." (both phrases are what PR 8 Task 8.12's guard checks).
  - The "Stale socket files" row: add "The admin socket follows the same probe-then-unlink rule."

  `docs/security/socket-bind.md`, new section before `## What is tested`:
  ```markdown
  ## The admin socket

  The admin socket reuses `bind_hardened` with one difference: the parent-directory check refuses instead of warning (`ParentPolicy::RequirePrivate`). A parent that is group- or world-writable, or owned by a non-root user other than the daemon's, is a place where someone else could create the socket name first and impersonate the control plane, so the bind stops. The stale-socket probe, the `umask(0o177)` bind, the inode re-check and the own-inode-only removal at exit are the same code as the producer socket's. The bind happens at startup step 7, while the runtime serves `/metrics` and the probes but before recovery: the process-global umask window then overlaps only the runtime's own threads, which create no files, and the lock-integrity thread, whose only file creation (an O_EXCL re-create of `.weir.lock` at mode 0o600) is unchanged by umask 0o177.
  ```

  `docs/security/container.md`, new subsection after `### One daemon per WAB volume`: `### Admin socket` — in the image it lives at `/var/lib/weir/wab.admin.sock`, inside the image's `/var/lib/weir` (chowned to uid 10001, not a VOLUME), not in the `/run/weir` VOLUME producers mount, so a sidecar running as uid 10001 has no admin access; `docker exec <container> weir-ctl status` reaches it; with `readOnlyRootFilesystem` set `WEIR_ADMIN_SOCKET_PATH` to a private writable mount, or the daemon logs `admin socket unavailable` and carries on.

- [ ] **Step 4: Run to verify it passes**

  Run: `cargo test -p weir-server --test docs_drift`
  Expected: all docs_drift guards pass, including the new one. Mutation check: delete the `#### \`admin_socket_path\`` heading, see the guard fail, restore.

- [ ] **Step 5: Commit**

  `git commit -am "docs(security): the admin socket in the threat model, socket and container docs"`

- [ ] **Step 6: Merge gate — security review and the full gate** (no commit unless the review finds something)

  1. Run the `security-review` skill over `git diff main...HEAD`, scoped to `crates/weir-server/src/admin/`, `socket/mod.rs` (`bind_hardened_with`), `reload.rs` and `config/mod.rs` (admin keys). Questions it must answer, each with a file:line: (a) can any peer other than the daemon euid or root get any reply other than one `forbidden` line? (b) does the `forbidden` line disclose anything (it must not carry `instance`)? (c) can an oversized or slow request buffer more than 64 KiB per connection, and is a permit released once a peer stops sending (10 s idle, 5 s per request, 5 s per write, at most 64 requests, waits ≤ 30 s)? (d) can a log line carry attacker-chosen text (the unknown-verb error does not echo the verb; `peer_uid` is a number)? (e) can the admin bind ever unlink a socket it did not create, or bind under a non-private parent? (f) does any admin reply carry a bearer token, S3 secret or URL password (it must not: the status document's admin view is built from `ConfigView`, which classifies those `Never`)?
  2. Fix any finding with its own failing test first, then commit `fix(admin): <finding>`.
  3. Run every command in the PR's **Merge gate** list above, then each CI job with `act -W .github/workflows/ci.yml -j <job>` (container jobs included), and wait for every remote check (Windows too) before merging.
