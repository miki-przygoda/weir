# Operability Foundations — PR 5: HTTP router on the metrics listener: /livez /readyz /startupz /status, head deadline and split pools, public redacted status type, threat model

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 5: HTTP router on the metrics listener: /livez /readyz /startupz /status, head deadline and split pools, public redacted status type, threat model

**Scope (spec §16 PR table, row 5; size S-M).** The router on the existing metrics listener (§5.8, §10): an exact-path routing table, request-head parsing with a 1 s deadline and a 4096-byte bound, a 64-slot head pool, then a separate pool per route class (scrapes on `metrics_max_connections`, the four health routes on a pool of 4). The four routes `/livez`, `/readyz`, `/startupz` and `/status` with the readiness and liveness semantics of §12. `PublicStatusDoc` and `weir_status::public` (§5.1 rules, §9.1 public view, §9.1 redaction rules). The `livez_checks` config key (§13). The `weir_http_requests_dropped_total{reason}` counter (§13). threat-model.md rows and the actor for the HTTP listener (§17 items 1 and 3).

**Depends on.** PR 4 (spec §16 "PRs 5 and 6 after 4": `/readyz` and `/startupz` read lifecycle and listener state that only PR 4 writes). Transitively PR 1 (weir-status crate, `StatusBoard`, `StatusSnapshot`, `status::render::render_admin_doc`, evaluator, probe predicates, admin document type and its golden file). Developed in parallel with PR 6, **merged before PR 6** (integrator decision): PR 6's startup wiring reads this PR's `config.livez_checks`, and both PRs edit `docs/security/threat-model.md` (PR 5 adds the HTTP-listener rows and actor; PR 6 adds the admin-socket row and actor), so PR 6 rebases onto this one.

**Behaviour change (release-noted, §17 item 5).** Four reserved HTTP paths. A metrics client must send its complete request head within 1 s or it is closed. Probes and scrapes have separate connection pools, so `metrics_max_connections` now counts concurrent scrapes after the head has arrived, not idle connections. Every other path still gets today's response, byte for byte.

**Merge gate.**
- `cargo test -p weir-status` (public redaction, closed-string walk, bounds, key shape and golden).
- `cargo test -p weir-server --bin weir-server -- --test-threads=1` (router table, renderers, head reader, server pools, board glue, config, metrics).
- `cargo test -p weir-server --test system health_ -- --test-threads=1` and `... readyz_ ... public_status_ ... a_restart_over_the_cap` (the four PR 5 system tests, one filter per invocation).
- `cargo test -p weir-server --test docs_drift` (the metric catalogue guard sees the new counter row).
- `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1` (the family-count drift detector sees `weir_http_requests_dropped`, Task 5.4).
- `cargo clippy --all-targets -- -D warnings` and the three feature variants ci.yml's lint job runs.
- `act -W .github/workflows/ci.yml -j <job>` for every job, container jobs included, before pushing.
- **Security review** of the redaction and the listener (spec §10 "The PR includes a security-review step"; §16 "Security review of redaction ... before merge"): Task 5.11 step 6.

**Reconciliations with the spec made in this PR (code wins for current behaviour, spec wins for target):**
1. **`spawn` signature.** Spec §5.8 writes `metrics::server::spawn(listener, HttpState{registry, board, thresholds, livez_checks, pools}, max_connections)`. Here `max_connections` moves into `HttpPools::new(metrics_max_connections)` inside `HttpState`, so `spawn(listener, state)`. Reason: tests must be able to take a pool's permits from outside to prove isolation.
2. **`board` becomes `status: Option<Arc<dyn StatusSource>>`.** A small trait implemented by `StatusBoard` (Task 5.9). The router tests (Task 5.8) then use a 20-line fake instead of a fully wired board, and the board glue is tested separately against a real board. `None` is the any-path metrics contract that `HttpState::metrics_only` (spec §5.8) names.
3. **The existing test `endpoint_caps_concurrent_connections` cannot pass unchanged** although §16 says "Existing tests pass". Its precondition, an idle peer holding the only scrape permit, is exactly what §5.8 removes. It keeps its name and its property (a full pool closes a scrape without a response) with the permit now taken by the test. A new test pins the intended change. Every other existing server test only changes its `spawn(..)` call.
4. **Separator.** §10 writes `/livez` failures as `<code>[,<code>]` and `/readyz` failures as `<code>[, <code>]`. Implemented literally (Decision Q12).
5. **Condition time key.** §5.1's `Condition` has `since_unix_ms`; §9.1 says public conditions carry `since_ms`. `PublicCondition` serializes `since_ms`, the same key PR 1's `Condition` already serializes (`RawCondition.since_ms`).
6. **System test 3 of §16** (SIGTERM: `/readyz` 503 within 200 ms) gates PR 4 in the spec, but `/readyz` does not exist until this PR. PR 4 keeps the socket-gone and acks part; Task 5.10 adds the `/readyz` half.
7. **Today's code** (metrics/server.rs:59-88) takes one `metrics_max_connections` permit at accept and holds it through a single 5 s read; main.rs:1459-1467 binds and spawns (inside `rt.block_on`), and :1469-1474 logs. After PR 4 that bind is startup step 5; the line numbers below say "the metrics bind at startup step 5".

---

### Task 5.1: `PublicStatusDoc` and `weir_status::public`, with the sentinel redaction test

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Create: `crates/weir-status/src/public.rs`
- Modify: `crates/weir-status/src/lib.rs` (add `mod public; pub use public::{public, PublicStatusDoc, PublicCondition, PublicEvidenceValue, PublicDrainView, PublicRequeueView, PublicSinkView, PublicSinkFailure, PublicReloadView, PublicReloadLast, PublicReloadItem, PublicAdminSurface, MAX_PUBLIC_SHARDS, MAX_PUBLIC_CONDITIONS};`)
- Test: `crates/weir-status/tests/public_redaction.rs`

**Interfaces:**
- Consumes (PR 1 Task 1.8, `weir-status`; exact names as PR 1 defines them, all `Clone, Debug, PartialEq, Serialize, Deserialize`):
  ```rust
  pub struct AdminStatusDoc { pub schema: String, pub view: View, pub version: String, pub wire_version: u8,
      pub generated_at_ms: u64, pub started_at_ms: u64, pub uptime_secs: f64, pub pid: u32,
      pub instance: InstanceView,                                  // admin only
      pub verdict: Verdict, pub conditions: Vec<Condition>, pub unevaluated: Vec<ConditionCode>,
      pub startup: StartupView, pub lifecycle: LifecycleView, pub ingest: IngestView,
      pub shards: Vec<ShardView>, pub drain: Option<AdminDrainView>, pub sink: Option<AdminSinkView>,
      pub backlog: Option<BacklogView>, pub reload: AdminReloadView, pub admin: AdminSurfaceView,
      pub config: AdminConfigDoc }
  pub enum View { Public, Admin }                        // serde snake_case
  pub struct Verdict { pub ready: bool, pub live: bool, pub startup_complete: bool,
      pub livez_checks: Vec<LivezCheck>, pub readiness_failing: Vec<ConditionCode>,
      pub liveness_failing: Vec<ConditionCode> }
  pub struct Condition { pub code: ConditionCode, pub severity: Severity, pub affects: Affects,
      pub since_unix_ms: Option<u64> /* JSON key "since_ms" */, pub summary: String, pub detail: Option<String>,
      pub evidence: BTreeMap<String, EvidenceValue>, pub runbook: Cow<'static, str>, pub gates: Vec<Gate> }
  #[serde(untagged)] pub enum EvidenceValue { U64(u64), F64(f64), Bool(bool), Segment(SegmentId),
      Shards(Vec<u16>), Code(String), Text(String) }
  pub struct AdminDrainView { pub state: DrainStateCode, pub state_since_ms: Option<u64>, pub paused: bool,
      pub pause_requested: bool, pub held: bool, pub paused_since_ms: Option<u64>, pub paused_by_uid: Option<u32>,
      pub generation: u64, pub observed_requeue: u64, pub in_flight: Option<InFlightView>,
      pub pending_segments: u64, pub backlog_segments: u64, pub channel_depth: Option<u64>,
      pub channel_capacity: u64, pub heartbeat_age_secs: Option<f64>, pub blocked_since_ms: Option<u64>,
      pub last_requeue: Option<AdminRequeueView>, pub exit: DrainExitCode, pub last_panic: Option<String> }
  pub struct AdminRequeueView { pub ticket: u64, pub requeued: u64, pub already_queued: u64, pub backoff_cut: bool,
      pub coalesced: bool, pub scan_failed: bool, pub at_ms: u64, pub scan_error: Option<String> }
  pub struct AdminSinkView { #[serde(rename = "type")] pub sink_type: String, pub health: Option<SinkHealthCode>,
      pub health_since_ms: Option<u64>, pub last_success_ms: Option<u64>,
      pub last_failure: Option<AdminSinkFailure>, pub consecutive_failures: u64, pub probe_reason: Option<String> }
  pub struct AdminSinkFailure { pub at_ms: u64, pub kind: Option<SinkFailureKind>, pub message: Option<String> }
  pub struct AdminReloadView { pub last: Option<AdminReloadLast> }
  pub struct AdminReloadLast { pub trigger: ReloadTriggerCode, pub at_ms: u64, pub outcome: ReloadOutcomeCode,
      pub items: Vec<AdminReloadItem>, pub not_reloaded: Vec<NotReloadedView> }
  pub struct AdminReloadItem { pub item: String, pub outcome: ReloadItemOutcome, pub detail: Option<String> }
  pub struct NotReloadedView { pub item: String, pub reason_code: String }
  pub struct AdminSurfaceView { pub protocol: u32, pub verbs: Vec<String>, pub state: Option<AdminState>,
      pub reason_code: Option<String>, pub path: Option<String> }
  pub struct AdminConfigDoc { pub public: PublicConfig, pub view: BTreeMap<String, ConfigValue> }
  pub struct PublicConfig { /* §9.1 fixed numeric subset + peer_uid_check, tls_enabled; no String */ }
  pub const MAX_DOC_SHARDS: usize = 256;  pub const MAX_DOC_CONDITIONS: usize = 40;
  // Shared, free-text-free sub-views reused verbatim by the public doc:
  // StartupView, LifecycleView, IngestView, ShardView, BacklogView, InFlightView, SegmentId,
  // ConditionCode (Copy), Severity (Copy), Affects (Copy), Gate (Clone), LivezCheck (Copy),
  // and the closed-set enums DrainStateCode, DrainExitCode, SinkHealthCode, SinkFailureKind,
  // ReloadTriggerCode, ReloadOutcomeCode, ReloadItemOutcome, AdminState (PR 1 Task 1.2).
  ```
  *(Integrator reconciliation: the draft coded against the spec's `*View` enum names and non-`Option` `drain` / `sink` / `backlog` / `channel_depth` / `health` / `kind` / `state`. PR 1 keeps those `Option` because they are unknown until their writers publish (PR 1 reconciliation 12), so the public projection below maps `Option` to `Option`.)*
  Requirement on PR 1: `crates/weir-status/tests/golden/status_v1_admin.json` exists and populates **every** `Option` with `Some` (`last_failure`, `last_requeue`, `reload.last` with at least one item, `in_flight`, at least one condition, at least one shard), because this task plants sentinels into it by JSON pointer.
- Produces:
  ```rust
  pub const MAX_PUBLIC_SHARDS: usize = MAX_DOC_SHARDS;          // 256, PR 1's bound
  pub const MAX_PUBLIC_CONDITIONS: usize = MAX_DOC_CONDITIONS;  // 40, PR 1's bound
  pub fn public(doc: &AdminStatusDoc) -> PublicStatusDoc;
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicStatusDoc { pub schema: String, pub view: View, pub version: String, pub wire_version: u8,
      pub generated_at_ms: u64, pub started_at_ms: u64, pub uptime_secs: f64, pub verdict: Verdict,
      pub conditions: Vec<PublicCondition>, pub unevaluated: Vec<ConditionCode>, pub startup: StartupView,
      pub lifecycle: LifecycleView, pub ingest: IngestView, pub shards: Vec<ShardView>,
      pub drain: Option<PublicDrainView>, pub sink: Option<PublicSinkView>, pub backlog: Option<BacklogView>,
      pub reload: PublicReloadView, pub admin: PublicAdminSurface, pub config: PublicConfig }
  pub struct PublicCondition { pub code: ConditionCode, pub severity: Severity, pub affects: Affects,
      pub since_ms: Option<u64>, pub summary: String,
      pub evidence: BTreeMap<String, PublicEvidenceValue>, pub runbook: String, pub gates: Vec<Gate> }
  #[serde(untagged)] pub enum PublicEvidenceValue { U64(u64), F64(f64), Bool(bool), Segment(SegmentId), Shards(Vec<u16>), Code(String) }
  impl PublicEvidenceValue { pub fn from_admin(v: &EvidenceValue) -> Option<Self>; }
  pub struct PublicDrainView { /* AdminDrainView minus paused_by_uid, last_panic; last_requeue: Option<PublicRequeueView> */ }
  pub struct PublicRequeueView { pub ticket: u64, pub requeued: u64, pub already_queued: u64,
      pub backoff_cut: bool, pub coalesced: bool, pub scan_failed: bool, pub at_ms: u64 }
  pub struct PublicSinkView { /* AdminSinkView minus probe_reason; last_failure: Option<PublicSinkFailure> */ }
  pub struct PublicSinkFailure { pub at_ms: u64, pub kind: Option<SinkFailureKind> }
  pub struct PublicReloadView { pub last: Option<PublicReloadLast> }
  pub struct PublicReloadLast { pub trigger: ReloadTriggerCode, pub at_ms: u64, pub outcome: ReloadOutcomeCode,
      pub items: Vec<PublicReloadItem>, pub not_reloaded: Vec<NotReloadedView> }
  pub struct PublicReloadItem { pub item: String, pub outcome: ReloadItemOutcome }
  pub struct PublicAdminSurface { pub protocol: u32, pub verbs: Vec<String>, pub state: Option<AdminState>,
      pub reason_code: Option<String> }
  ```

- [ ] **Step 1: Write the failing test** `crates/weir-status/tests/public_redaction.rs`:
  ```rust
  //! The public `weir.status/v1` rendering carries no free text (spec §9.1
  //! redaction rules, §16 "Sentinel redaction test").
  //!
  //! The admin fixture is PR 1's golden admin document with a unique sentinel
  //! planted in every admin-only field. The test first proves each sentinel
  //! survived deserialization into `AdminStatusDoc` (serde ignores unknown keys,
  //! so a mistyped pointer would otherwise make the test vacuous), then proves
  //! none survives `public()`.

  use serde_json::{Value, json};
  use weir_status::{AdminStatusDoc, EvidenceValue, MAX_PUBLIC_CONDITIONS, MAX_PUBLIC_SHARDS, public};

  const GOLDEN_ADMIN: &str = include_str!("golden/status_v1_admin.json");
  const PID_SENTINEL: u64 = 3_141_592_653;
  const UID_SENTINEL: u64 = 2_718_281_828;
  /// Planted in memory, not through JSON: `EvidenceValue` is untagged with
  /// `Code(String)` before `Text(String)`, so a plain JSON string always reads
  /// back as `Code`, which the public document keeps.
  const TEXT_EVIDENCE_SENTINEL: &str = "SENTINEL-evidence-text";

  fn plant(doc: &mut Value, pointer: &str, value: Value) {
      let (parent, key) = pointer.rsplit_once('/').expect("pointer has a parent");
      let slot = doc.pointer_mut(parent).unwrap_or_else(|| {
          panic!("golden admin doc has no {parent}; PR 1's golden must populate every Option")
      });
      match slot {
          Value::Object(m) => {
              m.insert(key.to_string(), value);
          }
          Value::Array(a) => {
              let i: usize = key.parse().expect("array index");
              a[i] = value;
          }
          other => panic!("{parent} is {other}; PR 1's golden must populate it with an object"),
      }
  }

  /// `(pointer, planted value, the text that must not appear in public output)`.
  fn sentinels() -> Vec<(&'static str, Value, String)> {
      let text = |s: &str| (Value::String(s.to_string()), s.to_string());
      let mut out = Vec::new();
      for (ptr, s) in [
          ("/instance/wab_dir", "/SENTINEL-wab-dir"),
          ("/instance/socket_path", "/SENTINEL-socket-path"),
          ("/conditions/0/detail", "SENTINEL-condition-detail"),
          ("/drain/last_panic", "SENTINEL-panic-payload"),
          ("/drain/last_requeue/scan_error", "SENTINEL-scan-error"),
          ("/sink/last_failure/message", "SENTINEL-sink-last-error"),
          ("/sink/probe_reason", "SENTINEL-probe-reason"),
          ("/reload/last/items/0/detail", "SENTINEL-reload-detail"),
          ("/admin/path", "/SENTINEL-admin-socket"),
      ] {
          let (v, needle) = text(s);
          out.push((ptr, v, needle));
      }
      out.push(("/pid", json!(PID_SENTINEL), PID_SENTINEL.to_string()));
      out.push(("/drain/paused_by_uid", json!(UID_SENTINEL), UID_SENTINEL.to_string()));
      out
  }

  fn sentinel_admin_doc() -> AdminStatusDoc {
      let mut v: Value = serde_json::from_str(GOLDEN_ADMIN).expect("golden admin doc parses");
      for (ptr, value, _) in sentinels() {
          plant(&mut v, ptr, value);
      }
      let mut doc: AdminStatusDoc = serde_json::from_value(v).expect("planted admin doc still deserializes");
      // Text evidence cannot be planted through JSON: an untagged string reads back as Code.
      doc.conditions[0]
          .evidence
          .insert("sentinel_text".into(), EvidenceValue::Text(TEXT_EVIDENCE_SENTINEL.into()));
      doc
  }

  #[test]
  fn every_sentinel_reaches_the_admin_doc() {
      let admin = serde_json::to_string(&sentinel_admin_doc()).unwrap();
      for (ptr, _, needle) in sentinels() {
          assert!(
              admin.contains(&needle),
              "sentinel planted at {ptr} did not survive deserialization into AdminStatusDoc; \n\
               the pointer no longer names a real field, so the redaction test below is vacuous for it"
          );
      }
      assert!(admin.contains(TEXT_EVIDENCE_SENTINEL), "the Text evidence sentinel is missing from the admin doc");
  }

  #[test]
  fn no_sentinel_reaches_the_public_doc() {
      let public_json = serde_json::to_string(&public(&sentinel_admin_doc())).unwrap();
      for (ptr, _, needle) in sentinels() {
          assert!(
              !public_json.contains(&needle),
              "admin-only value planted at {ptr} leaked into the public /status document"
          );
      }
      assert!(
          !public_json.contains(TEXT_EVIDENCE_SENTINEL),
          "Text evidence planted at /conditions/0/evidence/sentinel_text leaked into the public /status document"
      );
      assert!(!public_json.contains("SENTINEL"), "{public_json}");
  }

  /// Every string in the public document comes from a closed set: an enum
  /// rendering, a code, a segment identity, the schema or version, or a
  /// template-rendered `summary` (covered by the sentinel test). No absolute
  /// path, no capital letters, no spaces.
  fn assert_closed_strings(v: &Value, path: &str) {
      match v {
          Value::String(s) => {
              let key = path.rsplit('/').next().unwrap_or("");
              let ok = match key {
                  "schema" => s == "weir.status/v1",
                  "version" => s.chars().all(|c| c.is_ascii_alphanumeric() || ".-+".contains(c)),
                  "summary" => true,
                  "runbook" => s.starts_with("docs/monitoring.md#cond-"),
                  _ => {
                      !s.is_empty()
                          && s.len() <= 96
                          && !s.starts_with('/')
                          && s.chars().all(|c| {
                              c.is_ascii_lowercase() || c.is_ascii_digit() || "_-./:".contains(c)
                          })
                  }
              };
              assert!(ok, "public doc string at {path} is not from a closed set: {s:?}");
          }
          Value::Array(a) => {
              for (i, x) in a.iter().enumerate() {
                  assert_closed_strings(x, &format!("{path}/{i}"));
              }
          }
          Value::Object(o) => {
              for (k, x) in o {
                  assert!(
                      k.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                      "public doc key {path}/{k} is not snake_case"
                  );
                  assert_closed_strings(x, &format!("{path}/{k}"));
              }
          }
          _ => {}
      }
  }

  #[test]
  fn public_doc_strings_are_closed() {
      for doc in [serde_json::from_str::<AdminStatusDoc>(GOLDEN_ADMIN).unwrap(), sentinel_admin_doc()] {
          let v = serde_json::to_value(public(&doc)).unwrap();
          assert_eq!(v["view"], "public");
          assert_closed_strings(&v, "");
      }
  }

  #[test]
  fn public_doc_is_bounded() {
      let mut v: Value = serde_json::from_str(GOLDEN_ADMIN).unwrap();
      let shard = v["shards"][0].clone();
      v["shards"] = Value::Array(vec![shard; 300]);
      let cond = v["conditions"][0].clone();
      v["conditions"] = Value::Array(vec![cond; 50]);
      let doc: AdminStatusDoc = serde_json::from_value(v).unwrap();
      let p = public(&doc);
      assert_eq!(p.shards.len(), MAX_PUBLIC_SHARDS);
      assert_eq!(p.conditions.len(), MAX_PUBLIC_CONDITIONS);
  }

  #[test]
  fn public_doc_round_trips() {
      let p = public(&sentinel_admin_doc());
      let back: weir_status::PublicStatusDoc =
          serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
      assert_eq!(back, p);
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status --test public_redaction`. Expected: compile error `unresolved imports weir_status::public, weir_status::MAX_PUBLIC_SHARDS, weir_status::MAX_PUBLIC_CONDITIONS`.
- [ ] **Step 3: Implement** `crates/weir-status/src/public.rs`:
  ```rust
  //! The public rendering of `weir.status/v1` (spec §5.1 rules, §9.1).
  //!
  //! `PublicStatusDoc` is a separate type, not a filtered serialization of
  //! `AdminStatusDoc`. Its only String-typed fields are closed-enum
  //! renderings, condition codes, segment identities, the schema and version,
  //! and the template-rendered `summary`. Panic messages, I/O and sink errors,
  //! probe reasons, reload detail, uids, the pid, absolute paths and the sink
  //! URL, host or bucket have no field here, so they cannot be serialized into
  //! `/status`. Every admin struct is destructured without `..`: a field added
  //! to the admin document does not compile until it is classified here.

  use std::collections::BTreeMap;

  use serde::{Deserialize, Serialize};

  use crate::{
      AdminConfigDoc, AdminDrainView, AdminReloadItem, AdminReloadLast, AdminReloadView, AdminRequeueView,
      AdminSinkFailure, AdminSinkView, AdminState, AdminStatusDoc, AdminSurfaceView, Affects,
      BacklogView, Condition, ConditionCode, DrainExitCode, DrainStateCode, EvidenceValue, Gate,
      InFlightView, IngestView, LifecycleView, MAX_DOC_CONDITIONS, MAX_DOC_SHARDS, NotReloadedView,
      PublicConfig, ReloadItemOutcome, ReloadOutcomeCode, ReloadTriggerCode, SegmentId, Severity,
      ShardView, SinkFailureKind, SinkHealthCode, StartupView, Verdict, View,
  };

  /// `/status` is bounded (spec §10): `shard_count` is at most 256 already, and
  /// the catalogue has 30 codes; the caps make the bound explicit. The same
  /// bounds PR 1's admin document applies.
  pub const MAX_PUBLIC_SHARDS: usize = MAX_DOC_SHARDS;
  pub const MAX_PUBLIC_CONDITIONS: usize = MAX_DOC_CONDITIONS;

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicStatusDoc {
      pub schema: String,
      pub view: View,
      pub version: String,
      pub wire_version: u8,
      pub generated_at_ms: u64,
      pub started_at_ms: u64,
      pub uptime_secs: f64,
      pub verdict: Verdict,
      pub conditions: Vec<PublicCondition>,
      pub unevaluated: Vec<ConditionCode>,
      pub startup: StartupView,
      pub lifecycle: LifecycleView,
      pub ingest: IngestView,
      pub shards: Vec<ShardView>,
      /// `None` until the drain publishes (PR 1: unknown is `null`).
      pub drain: Option<PublicDrainView>,
      pub sink: Option<PublicSinkView>,
      pub backlog: Option<BacklogView>,
      pub reload: PublicReloadView,
      pub admin: PublicAdminSurface,
      pub config: PublicConfig,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicCondition {
      pub code: ConditionCode,
      pub severity: Severity,
      pub affects: Affects,
      pub since_ms: Option<u64>,
      pub summary: String,
      pub evidence: BTreeMap<String, PublicEvidenceValue>,
      pub runbook: String,
      pub gates: Vec<Gate>,
  }

  /// `EvidenceValue` without `Text`. The same serde shape as PR 1's
  /// `EvidenceValue` (untagged, same variant order: a segment-id string reads
  /// as `Segment`, any other string as `Code`) so weir-ctl parses both
  /// renderings with one reader.
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  #[serde(untagged)]
  pub enum PublicEvidenceValue {
      U64(u64),
      F64(f64),
      Bool(bool),
      Segment(SegmentId),
      Shards(Vec<u16>),
      Code(String),
  }

  impl PublicEvidenceValue {
      /// `None` for admin-only evidence. Exhaustive on purpose: a new
      /// `EvidenceValue` variant fails to compile until it is classified.
      pub fn from_admin(v: &EvidenceValue) -> Option<Self> {
          Some(match v {
              EvidenceValue::U64(n) => Self::U64(*n),
              EvidenceValue::F64(x) => Self::F64(*x),
              EvidenceValue::Bool(b) => Self::Bool(*b),
              EvidenceValue::Code(c) => Self::Code(c.clone()),
              EvidenceValue::Segment(s) => Self::Segment(*s),
              EvidenceValue::Shards(s) => Self::Shards(s.clone()),
              EvidenceValue::Text(_) => return None,
          })
      }
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicDrainView {
      pub state: DrainStateCode,
      pub state_since_ms: Option<u64>,
      pub paused: bool,
      pub pause_requested: bool,
      pub held: bool,
      pub paused_since_ms: Option<u64>,
      pub generation: u64,
      pub observed_requeue: u64,
      pub in_flight: Option<InFlightView>,
      pub pending_segments: u64,
      pub backlog_segments: u64,
      pub channel_depth: Option<u64>,
      pub channel_capacity: u64,
      pub heartbeat_age_secs: Option<f64>,
      pub blocked_since_ms: Option<u64>,
      pub last_requeue: Option<PublicRequeueView>,
      pub exit: DrainExitCode,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicRequeueView {
      pub ticket: u64,
      pub requeued: u64,
      pub already_queued: u64,
      pub backoff_cut: bool,
      pub coalesced: bool,
      pub scan_failed: bool,
      pub at_ms: u64,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicSinkView {
      #[serde(rename = "type")]
      pub sink_type: String,
      pub health: Option<SinkHealthCode>,
      pub health_since_ms: Option<u64>,
      pub last_success_ms: Option<u64>,
      pub last_failure: Option<PublicSinkFailure>,
      pub consecutive_failures: u64,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicSinkFailure {
      pub at_ms: u64,
      pub kind: Option<SinkFailureKind>,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicReloadView {
      pub last: Option<PublicReloadLast>,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicReloadLast {
      pub trigger: ReloadTriggerCode,
      pub at_ms: u64,
      pub outcome: ReloadOutcomeCode,
      pub items: Vec<PublicReloadItem>,
      pub not_reloaded: Vec<NotReloadedView>,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicReloadItem {
      pub item: String,
      pub outcome: ReloadItemOutcome,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct PublicAdminSurface {
      pub protocol: u32,
      pub verbs: Vec<String>,
      pub state: Option<AdminState>,
      pub reason_code: Option<String>,
  }

  fn public_condition(c: &Condition) -> PublicCondition {
      let Condition { code, severity, affects, since_unix_ms, summary, detail: _, evidence, runbook, gates } = c;
      PublicCondition {
          code: *code,
          severity: *severity,
          affects: *affects,
          since_ms: *since_unix_ms,
          summary: summary.clone(),
          evidence: evidence
              .iter()
              .filter_map(|(k, v)| PublicEvidenceValue::from_admin(v).map(|pv| (k.clone(), pv)))
              .collect(),
          runbook: runbook.to_string(),
          gates: gates.clone(),
      }
  }

  fn public_drain(d: &AdminDrainView) -> PublicDrainView {
      let AdminDrainView {
          state, state_since_ms, paused, pause_requested, held, paused_since_ms,
          paused_by_uid: _, generation, observed_requeue, in_flight, pending_segments,
          backlog_segments, channel_depth, channel_capacity, heartbeat_age_secs,
          blocked_since_ms, last_requeue, exit, last_panic: _,
      } = d;
      PublicDrainView {
          state: *state,
          state_since_ms: *state_since_ms,
          paused: *paused,
          pause_requested: *pause_requested,
          held: *held,
          paused_since_ms: *paused_since_ms,
          generation: *generation,
          observed_requeue: *observed_requeue,
          in_flight: *in_flight,
          pending_segments: *pending_segments,
          backlog_segments: *backlog_segments,
          channel_depth: *channel_depth,
          channel_capacity: *channel_capacity,
          heartbeat_age_secs: *heartbeat_age_secs,
          blocked_since_ms: *blocked_since_ms,
          last_requeue: last_requeue.as_ref().map(|r| {
              let AdminRequeueView { ticket, requeued, already_queued, backoff_cut, coalesced, scan_failed, at_ms, scan_error: _ } = r;
              PublicRequeueView {
                  ticket: *ticket, requeued: *requeued, already_queued: *already_queued,
                  backoff_cut: *backoff_cut, coalesced: *coalesced, scan_failed: *scan_failed, at_ms: *at_ms,
              }
          }),
          exit: *exit,
      }
  }

  fn public_sink(s: &AdminSinkView) -> PublicSinkView {
      let AdminSinkView { sink_type, health, health_since_ms, last_success_ms, last_failure, consecutive_failures, probe_reason: _ } = s;
      PublicSinkView {
          sink_type: sink_type.clone(),
          health: *health,
          health_since_ms: *health_since_ms,
          last_success_ms: *last_success_ms,
          last_failure: last_failure.as_ref().map(|f| {
              let AdminSinkFailure { at_ms, kind, message: _ } = f;
              PublicSinkFailure { at_ms: *at_ms, kind: *kind }
          }),
          consecutive_failures: *consecutive_failures,
      }
  }

  fn public_reload(r: &AdminReloadView) -> PublicReloadView {
      let AdminReloadView { last } = r;
      PublicReloadView {
          last: last.as_ref().map(|l| {
              let AdminReloadLast { trigger, at_ms, outcome, items, not_reloaded } = l;
              PublicReloadLast {
                  trigger: *trigger,
                  at_ms: *at_ms,
                  outcome: *outcome,
                  items: items
                      .iter()
                      .map(|i| {
                          let AdminReloadItem { item, outcome, detail: _ } = i;
                          PublicReloadItem { item: item.clone(), outcome: *outcome }
                      })
                      .collect(),
                  not_reloaded: not_reloaded.clone(),
              }
          }),
      }
  }

  fn public_admin_surface(a: &AdminSurfaceView) -> PublicAdminSurface {
      let AdminSurfaceView { protocol, verbs, state, reason_code, path: _ } = a;
      PublicAdminSurface {
          protocol: *protocol,
          verbs: verbs.clone(),
          state: *state,
          reason_code: reason_code.clone(),
      }
  }

  /// The one way to build the public document (spec §5.1).
  pub fn public(doc: &AdminStatusDoc) -> PublicStatusDoc {
      let AdminStatusDoc {
          schema, view: _, version, wire_version, generated_at_ms, started_at_ms, uptime_secs,
          pid: _, instance: _, verdict, conditions, unevaluated, startup, lifecycle, ingest,
          shards, drain, sink, backlog, reload, admin, config,
      } = doc;
      let AdminConfigDoc { public: public_config, .. } = config; // the full view is admin only
      PublicStatusDoc {
          schema: schema.clone(),
          view: View::Public,
          version: version.clone(),
          wire_version: *wire_version,
          generated_at_ms: *generated_at_ms,
          started_at_ms: *started_at_ms,
          uptime_secs: *uptime_secs,
          verdict: verdict.clone(),
          conditions: conditions.iter().take(MAX_PUBLIC_CONDITIONS).map(public_condition).collect(),
          unevaluated: unevaluated.clone(),
          startup: *startup,
          lifecycle: *lifecycle,
          ingest: ingest.clone(),
          shards: shards.iter().take(MAX_PUBLIC_SHARDS).cloned().collect(),
          drain: drain.as_ref().map(public_drain),
          sink: sink.as_ref().map(public_sink),
          backlog: backlog.clone(),
          reload: public_reload(reload),
          admin: public_admin_surface(admin),
          config: public_config.clone(),
      }
  }
  ```
  Every field of a `Copy` type (PR 1's `closed_set!` enums, `SegmentId`, `StartupView`, `LifecycleView`, `InFlightView` and `Option`s of them) is copied with `*`, not `.clone()`: `clippy::clone_on_copy` is warn-by-default and CI's lint job runs `-D warnings`. `AdminConfigDoc { public, .. }` is the one `..` in the file and is deliberate: everything beyond `public` in the config is admin only by construction, so a new field there needs no classification. `serde_json` stays a dev-dependency of weir-status; the crate's runtime deps remain serde and serde_json only (spec §5.1).
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status --test public_redaction`. Expected: 5 passed. Then `cargo clippy -p weir-status --all-targets -- -D warnings`. Expected: clean (no `clone_on_copy`).
- [ ] **Step 5: Mutation check (the spec requires the sentinel test to be verified by mutation).** Temporarily change `public_condition`'s `summary: summary.clone(),` to `summary: detail.clone().unwrap_or_else(|| summary.clone()),` (and bind `detail` instead of `detail: _`). Run `cargo test -p weir-status --test public_redaction no_sentinel`. Expected: FAIL with `admin-only value planted at /conditions/0/detail leaked`. Revert, rerun, PASS. Do the same once for `PublicSinkFailure` by adding a `message` field copied from the admin one: expected FAIL at `/sink/last_failure/message`. Revert.
- [ ] **Step 6: Commit.** `feat(status): the public weir.status/v1 document, built so it cannot carry free text`

### Task 5.2: Pin the public document's key shape and its golden file

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Create: `crates/weir-status/tests/public_shape.rs`
- Create: `crates/weir-status/tests/golden/status_v1_public.json` (written by the bless run in Step 3, then committed)

**Interfaces:**
- Consumes: `weir_status::public`, `AdminStatusDoc`, PR 1's `tests/golden/status_v1_admin.json`.
- Produces: `crates/weir-status/tests/golden/status_v1_public.json`, the golden public document weir-ctl (PR 7) and the docs_drift `/status` top-level-keys guard (PR 8) read.

- [ ] **Step 1: Write the failing test** `crates/weir-status/tests/public_shape.rs`:
  ```rust
  //! The public document's keys are a contract (spec §9.1, additive-only):
  //! weir-ctl parses them, docs_drift guards them, dashboards scrape them.

  use std::collections::BTreeSet;
  use serde_json::Value;
  use weir_status::{AdminStatusDoc, public};

  const GOLDEN_ADMIN: &str = include_str!("golden/status_v1_admin.json");
  const GOLDEN_PUBLIC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/status_v1_public.json");

  fn keys(v: &Value) -> BTreeSet<String> {
      v.as_object().expect("an object").keys().cloned().collect()
  }
  fn set(names: &[&str]) -> BTreeSet<String> {
      names.iter().map(|s| s.to_string()).collect()
  }
  fn public_value() -> Value {
      let admin: AdminStatusDoc = serde_json::from_str(GOLDEN_ADMIN).unwrap();
      serde_json::to_value(public(&admin)).unwrap()
  }

  #[test]
  fn public_top_level_keys_are_exactly_the_spec_list() {
      assert_eq!(
          keys(&public_value()),
          set(&[
              "schema", "view", "version", "wire_version", "generated_at_ms", "started_at_ms",
              "uptime_secs", "verdict", "conditions", "unevaluated", "startup", "lifecycle",
              "ingest", "shards", "drain", "sink", "backlog", "reload", "admin", "config",
          ])
      );
  }

  #[test]
  fn admin_only_keys_are_absent_from_every_public_section() {
      let v = public_value();
      assert_eq!(
          keys(&v["verdict"]),
          set(&["ready", "live", "startup_complete", "livez_checks", "readiness_failing", "liveness_failing"])
      );
      assert_eq!(
          keys(&v["conditions"][0]),
          set(&["code", "severity", "affects", "since_ms", "summary", "evidence", "runbook", "gates"])
      );
      assert_eq!(
          keys(&v["drain"]),
          set(&[
              "state", "state_since_ms", "paused", "pause_requested", "held", "paused_since_ms",
              "generation", "observed_requeue", "in_flight", "pending_segments", "backlog_segments",
              "channel_depth", "channel_capacity", "heartbeat_age_secs", "blocked_since_ms",
              "last_requeue", "exit",
          ])
      );
      assert_eq!(
          keys(&v["drain"]["last_requeue"]),
          set(&["ticket", "requeued", "already_queued", "backoff_cut", "coalesced", "scan_failed", "at_ms"])
      );
      assert_eq!(
          keys(&v["sink"]),
          set(&["type", "health", "health_since_ms", "last_success_ms", "last_failure", "consecutive_failures"])
      );
      assert_eq!(keys(&v["sink"]["last_failure"]), set(&["at_ms", "kind"]));
      assert_eq!(keys(&v["reload"]["last"]["items"][0]), set(&["item", "outcome"]));
      assert_eq!(keys(&v["admin"]), set(&["protocol", "verbs", "state", "reason_code"]));
  }

  #[test]
  fn public_golden_matches() {
      let rendered = serde_json::to_string_pretty(&public_value()).unwrap() + "\n";
      if std::env::var_os("WEIR_BLESS").is_some() {
          std::fs::write(GOLDEN_PUBLIC_PATH, &rendered).unwrap();
      }
      let golden = std::fs::read_to_string(GOLDEN_PUBLIC_PATH)
          .expect("run once with WEIR_BLESS=1 to write tests/golden/status_v1_public.json, then review it");
      assert_eq!(rendered, golden, "public /status rendering changed; additive-only (spec §9.1). Re-bless only for an additive change");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status --test public_shape`. Expected: `public_golden_matches` FAILS with `run once with WEIR_BLESS=1 ...` (no golden yet). The two key tests pass already if Task 5.1 is right; if either fails, the PR 1 admin field names differ from §9.1, so reconcile `public.rs` first.
- [ ] **Step 3: Implement (bless and review).** `WEIR_BLESS=1 cargo test -p weir-status --test public_shape public_golden_matches`. Open `tests/golden/status_v1_public.json` and check by eye: no `pid`, `instance`, `detail`, `paused_by_uid`, `last_panic`, `scan_error`, `message`, `probe_reason`, `path`; `view` is `"public"`. `WEIR_BLESS` is PR 1's bless variable (PR 1 Task 1.8's golden test).
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass.
- [ ] **Step 5: Commit.** `test(status): pin the public /status key shape and its golden document`

### Task 5.3: Config key `livez_checks`

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/config/mod.rs` (`PartialConfig` after `metrics_max_connections` at :241; `Config` after `metrics_max_connections` at :445; parse after the range check at :907-908; struct literal at :1239; new `parse_livez_checks` next to `parse_wab_dir_lock` at :205-217; tests next to `wab_dir_lock_defaults_to_required` at :2710)
- Modify: `crates/weir-server/src/config/file.rs` (`RawServer` field after :30; `KNOWN` list after :115; mapping after :241; test next to `wab_dir_lock_is_a_known_key` at :379)
- Modify: `crates/weir-server/src/config/env.rs` (after :22)
- Modify: `crates/weir-server/src/config/cli.rs` (`HELP` after the `--metrics-max-connections` lines at :46-47; parse after :203-205; the `PartialConfig` literal near :289 if it lists fields; test next to `wab_dir_lock_flag_is_parsed` at :409)
- Modify: `crates/weir-server/src/status/config_view.rs` (PR 1's exhaustive `Config` destructure)

**Interfaces:**
- Consumes (PR 1, weir-status): `#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)] pub enum LivezCheck { ListenerDown, NoLiveShard, WorkerExited, DrainStopped, DrainWedged }`, `pub const ALL: [LivezCheck; 5]` (catalogue order: `listener_down` first, as in §9.3), `pub fn as_str(self) -> &'static str` (`"listener_down"`, `"no_live_shard"`, ...), `pub fn parse(s: &str) -> Option<Self>`.
- Produces:
  ```rust
  // config/mod.rs
  pub(crate) struct PartialConfig { /* … */ pub livez_checks: Option<String>, /* … */ }
  pub struct Config { /* … */ pub livez_checks: Vec<weir_status::LivezCheck>, /* … */ }
  fn parse_livez_checks(s: &str) -> Result<Vec<weir_status::LivezCheck>, ConfigError>;
  ```
  TOML `livez_checks = ["no_live_shard", "listener_down"]` (array), env `WEIR_LIVEZ_CHECKS=no_live_shard,listener_down`, CLI `--livez-checks no_live_shard,listener_down`. Default `[]`.

- [ ] **Step 1: Write the failing tests.**
  In `config/mod.rs` `mod tests`:
  ```rust
  #[test]
  fn livez_checks_defaults_to_empty() {
      let dir = tmp_dir("livez_default");
      let c = layers_with_wab(dir.clone()).unwrap();
      assert!(c.livez_checks.is_empty(), "liveness is opt-in (spec §12)");
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn livez_checks_parse_dedup_and_catalogue_order() {
      use weir_status::LivezCheck;
      let dir = tmp_dir("livez_parse");
      let c = Config::from_layers(
          PartialConfig {
              wab_dir: Some(dir.clone()),
              livez_checks: Some(" worker_exited,no_live_shard , worker_exited,".into()),
              ..PartialConfig::empty()
          },
          PartialConfig::empty(),
          PartialConfig::empty(),
      )
      .unwrap();
      assert_eq!(c.livez_checks, vec![LivezCheck::NoLiveShard, LivezCheck::WorkerExited]);
      fs::remove_dir_all(dir).ok();
  }

  #[test]
  fn livez_checks_rejects_an_unselectable_code() {
      let dir = tmp_dir("livez_bad");
      // `sink_down` is a real condition but never a liveness check: a restart
      // does not fix a down sink (spec §12).
      for bad in ["sink_down", "No_Live_Shard", "starting"] {
          let err = Config::from_layers(
              PartialConfig {
                  wab_dir: Some(dir.clone()),
                  livez_checks: Some(bad.into()),
                  ..PartialConfig::empty()
              },
              PartialConfig::empty(),
              PartialConfig::empty(),
          )
          .map(|_| ())
          .unwrap_err()
          .to_string();
          assert!(err.contains("livez_checks"), "{err}");
          assert!(err.contains("no_live_shard"), "the error lists the valid checks: {err}");
      }
      fs::remove_dir_all(dir).ok();
  }
  ```
  In `config/file.rs` `mod tests`:
  ```rust
  #[test]
  fn livez_checks_is_a_known_array_key() {
      let dir = crate::testutil::scratch_dir("filecfg_livez");
      crate::testutil::mkdir_p(&dir);
      let path = dir.join("weir.toml");
      std::fs::write(&path, "[server]\nlivez_checks = [\"no_live_shard\", \"listener_down\"]\n").unwrap();
      let (partial, warnings) = read(&path).unwrap().expect("the file exists");
      assert_eq!(partial.livez_checks.as_deref(), Some("no_live_shard,listener_down"));
      assert!(warnings.iter().all(|w| !w.contains("livez_checks")), "{warnings:?}");
      std::fs::remove_file(&path).ok();
  }
  ```
  In `config/cli.rs` `mod tests`:
  ```rust
  #[test]
  fn livez_checks_flag_is_parsed() {
      let (partial, _) = parse_from(args(&["--livez-checks", "no_live_shard,listener_down"])).unwrap();
      assert_eq!(partial.livez_checks.as_deref(), Some("no_live_shard,listener_down"));
      assert!(HELP.contains("--livez-checks"), "the flag must be in --help");
  }
  ```
- [ ] **Step 2: Run them to verify they fail.** `cargo test -p weir-server --bin weir-server livez_checks -- --test-threads=1`. Expected: compile error `struct PartialConfig has no field named livez_checks`.
- [ ] **Step 3: Implement.**
  `config/mod.rs`:
  ```rust
  /// Parses `livez_checks`: a comma-separated subset of the five liveness
  /// checks a restart can fix (spec §12). Empty, the default, means `/livez`
  /// answers 200 whenever the runtime answers. Duplicates fold, and the result
  /// is in catalogue order so the rendered list is stable.
  fn parse_livez_checks(s: &str) -> Result<Vec<weir_status::LivezCheck>, ConfigError> {
      use weir_status::LivezCheck;
      let mut out: Vec<LivezCheck> = Vec::new();
      for raw in s.split(',').map(str::trim).filter(|t| !t.is_empty()) {
          let Some(check) = LivezCheck::parse(raw) else {
              return Err(ConfigError::InvalidValue {
                  field: "livez_checks",
                  reason: format!(
                      "{raw:?} is not a liveness check; expected a subset of {}",
                      LivezCheck::ALL.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", ")
                  ),
              });
          };
          if !out.contains(&check) {
              out.push(check);
          }
      }
      out.sort_by_key(|c| LivezCheck::ALL.iter().position(|a| a == c));
      Ok(out)
  }
  ```
  In `from_layers`, after `check_range("metrics_max_connections", …)?;`:
  ```rust
  let livez_checks = parse_livez_checks(&merge!(livez_checks).unwrap_or_default())?;
  ```
  Add `pub livez_checks: Option<String>,` to `PartialConfig`, and to `Config`:
  ```rust
  /// Liveness checks `/livez` fails on (spec §12). Empty (default): `/livez`
  /// is 200 whenever the runtime answers. Only states a restart fixes are
  /// selectable; every check is evaluated only while the phase is `serving`.
  pub livez_checks: Vec<weir_status::LivezCheck>,
  ```
  and `livez_checks,` in the `Ok(Config { … })` literal after `metrics_max_connections,`.
  `config/file.rs`: `livez_checks: Option<Vec<String>>,` in `RawServer`; `"livez_checks",` in `KNOWN`; mapping `livez_checks: s.livez_checks.map(|v| v.join(",")),`.
  `config/env.rs`: `livez_checks: env_string("WEIR_LIVEZ_CHECKS")?,`.
  `config/cli.rs`: in `parse_from`, `livez_checks: pargs.opt_value_from_str("--livez-checks").map_err(pico_err)?,`; in `HELP` after the `--metrics-max-connections` entry:
  ```text
      --livez-checks <a,b>                     /livez fails on these (listener_down,
                                                 no_live_shard, worker_exited,
                                                 drain_stopped, drain_wedged)
                                                 [default: none]
  ```
  Every other `PartialConfig { … }` literal that lists fields without `..` gets `livez_checks: None,`.
  `status/config_view.rs`: add `livez_checks` to the exhaustive `let Config { … } = cfg;` destructure (the build fails until you do, which is the point of §5.14) and classify it **Admin**, rendered as the comma-joined `as_str()` list, with the same `admin(..)` call PR 1 uses for `wab_dir_lock`:
  ```rust
          v.admin(
              "livez_checks",
              s(livez_checks.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(",")),
          );
  ```
  It is not in the §9.1 public config subset; the public document carries it as `verdict.livez_checks` instead.
- [ ] **Step 4: Run to verify they pass.** `cargo test -p weir-server --bin weir-server config -- --test-threads=1`. Expected: all config tests pass, including the PR 1 ConfigView compile guard.
- [ ] **Step 5: Commit.** `feat(config): livez_checks selects the restart-fixable states /livez fails on`

### Task 5.4: Counter `weir_http_requests_dropped_total{reason}` and its reference row

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs` (label types after `ConfigReloadOutcome` at :285-293; field after `config_reloads` at :489; registration after the config-reload block at :858-866; struct literal after `config_reloads,` at :1004; pre-initialisation after the `sink_health` block at :1047-1058; tests at the end of `mod tests`)
- Modify: `docs/monitoring.md` (`### System / security` table, after the `weir_config_reloads_total` row)
- Modify: `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`'s `expected` vec, :795-907 at 2491883: it asserts the `# HELP weir_` count equals the vec length, so every new family must be listed there too)

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct HttpDroppedLabel { pub reason: HttpDropReason }
  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum HttpDropReason { head_timeout, head_pool_full, metrics_pool_full, probe_pool_full }
  // Metrics
  pub http_requests_dropped: Family<HttpDroppedLabel, Counter<u64, AtomicU64>>,
  ```

- [ ] **Step 1: Write the failing test** in `metrics/mod.rs` `mod tests`:
  ```rust
  /// All four drop reasons exist at 0 from boot (spec §13: every new family
  /// pre-creates its label set), so `rate()` works before the first drop.
  #[test]
  fn http_requests_dropped_series_are_pre_initialised_to_zero() {
      let (m, reg) = Metrics::new();
      let out = encode_to_string(&reg);
      for reason in ["head_timeout", "head_pool_full", "metrics_pool_full", "probe_pool_full"] {
          let series = format!("weir_http_requests_dropped_total{{reason=\"{reason}\"}} 0");
          assert!(out.contains(&series), "{series:?} missing:\n{out}");
      }
      m.http_requests_dropped
          .get_or_create(&HttpDroppedLabel { reason: HttpDropReason::head_timeout })
          .inc();
      assert!(encode_to_string(&reg)
          .contains("weir_http_requests_dropped_total{reason=\"head_timeout\"} 1"));
  }
  ```
  Add `"weir_http_requests_dropped",` to the `expected` array of `all_metric_names_present_in_output`.
  In `crates/weir-server/tests/system.rs`, `metrics_all_families_registered`, append to the `expected` vec (after the entries the earlier Foundations PRs added, before `];`):
  ```rust
          // Metrics / health listener (Foundations PR 5)
          "weir_http_requests_dropped",
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server http_requests_dropped -- --test-threads=1`. Expected: compile error `cannot find struct HttpDroppedLabel`.
- [ ] **Step 3: Implement.** Label types as in Interfaces, each with a doc comment. Registration:
  ```rust
  // ── Metrics / health listener (Foundations, spec §5.8) ───────────────
  let http_requests_dropped = reg!(
      Family::<HttpDroppedLabel, Counter<u64, AtomicU64>>::default(),
      "weir_http_requests_dropped",
      "Connections to the metrics/health listener closed without a response, by reason: \
       head_timeout (no complete request head within 1 s), head_pool_full (all 64 \
       head slots busy), metrics_pool_full (more concurrent scrapes than \
       metrics_max_connections), probe_pool_full (more than 4 concurrent /livez, \
       /readyz, /startupz or /status requests)"
  );
  ```
  Pre-initialise after the `sink_health` block:
  ```rust
  for reason in [
      HttpDropReason::head_timeout,
      HttpDropReason::head_pool_full,
      HttpDropReason::metrics_pool_full,
      HttpDropReason::probe_pool_full,
  ] {
      let _ = metrics
          .http_requests_dropped
          .get_or_create(&HttpDroppedLabel { reason });
  }
  ```
  (Same shape as the `drain_local_io_strands` pre-initialisation at metrics/mod.rs:1070-1074.)
  `docs/monitoring.md`, `### System / security` table, new row:
  ```markdown
  | `weir_http_requests_dropped_total{reason}` | counter | Connections to the metrics/health listener (`metrics_bind:metrics_port`) closed without a response. `reason` is `head_timeout` (no complete request head within 1 s: a stalled or slowloris peer), `head_pool_full` (all 64 head slots busy: a connection flood), `metrics_pool_full` (more concurrent scrapes than [`metrics_max_connections`](operations/configuration.md#metrics_max_connections); Prometheus retries on its next interval) or `probe_pool_full` (more than 4 concurrent `/livez`, `/readyz`, `/startupz` or `/status` requests). Should be ~0. A rising `head_timeout` or `head_pool_full` on a listener bound beyond loopback means someone is holding connections open: restrict the port to the scraper and the kubelet. |
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server metrics:: -- --test-threads=1`, `cargo test -p weir-server --test docs_drift every_registered_metric_is_in_the_monitoring_reference` and `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1`. Expected: PASS (the catalogue guard fails without the row, and the family-count test fails with `metric family count mismatch` without the `expected` entry: check both by running them once before adding the row and the entry).
- [ ] **Step 5: Commit.** `feat(metrics): count metrics-listener connections closed before a response, by reason` (stage `crates/weir-server/src/metrics/mod.rs`, `crates/weir-server/tests/system.rs` and `docs/monitoring.md`).

### Task 5.5: `route()`: the pure, exact-path routing table

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Create: `crates/weir-server/src/metrics/router.rs`
- Modify: `crates/weir-server/src/metrics/mod.rs` (add `pub(crate) mod router;` after `pub(crate) mod server;` at :13)
- Test: `crates/weir-server/src/metrics/router.rs` (`mod tests`)

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Endpoint { Livez, Readyz, Startupz, Status }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Route {
      Metrics,
      Probe { endpoint: Endpoint, head_only: bool, verbose: bool },
      MethodNotAllowed,
  }
  pub(crate) fn route(head: &[u8]) -> Route;
  ```

- [ ] **Step 1: Write the failing test** (`router.rs` `mod tests`):
  ```rust
  use super::*;

  fn probe(endpoint: Endpoint, head_only: bool, verbose: bool) -> Route {
      Route::Probe { endpoint, head_only, verbose }
  }

  #[test]
  fn routing_table() {
      use Endpoint::*;
      let cases: &[(&[u8], Route)] = &[
          // The four reserved paths, GET and HEAD.
          (b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n", probe(Livez, false, false)),
          (b"GET /readyz HTTP/1.0\r\n\r\n", probe(Readyz, false, false)),
          (b"GET /startupz HTTP/1.1\r\n\r\n", probe(Startupz, false, false)),
          (b"GET /status HTTP/1.1\r\n\r\n", probe(Status, false, false)),
          (b"HEAD /readyz HTTP/1.1\r\n\r\n", probe(Readyz, true, false)),
          // Query stripped; `verbose` recognised as a bare key or key=value.
          (b"GET /livez?verbose HTTP/1.1\r\n\r\n", probe(Livez, false, true)),
          (b"GET /readyz?a=1&verbose=1 HTTP/1.1\r\n\r\n", probe(Readyz, false, true)),
          (b"GET /readyz?verbosely HTTP/1.1\r\n\r\n", probe(Readyz, false, false)),
          (b"GET /status?x=y HTTP/1.1\r\n\r\n", probe(Status, false, false)),
          // Other methods on a reserved path.
          (b"POST /readyz HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
          (b"get /readyz HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
          (b"DELETE /status HTTP/1.1\r\n\r\n", Route::MethodNotAllowed),
          // Exact, case-sensitive, no trailing-slash folding: these are metrics.
          (b"GET /READYZ HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET /readyz/ HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET //readyz HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET http://h/readyz HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET /metrics HTTP/1.0\r\n\r\n", Route::Metrics),
          (b"GET / HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET /foo HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"POST /metrics HTTP/1.1\r\n\r\n", Route::Metrics),
          // Unparseable request lines get metrics, as today.
          (b"", Route::Metrics),
          (b"GARBAGE not http", Route::Metrics),
          (b"GET /readyz\r\n\r\n", Route::Metrics),
          (b"GET  /readyz HTTP/1.1\r\n\r\n", Route::Metrics),
          (b"GET /readyz HTTP/1.1 extra\r\n\r\n", Route::Metrics),
          (b"GET /readyz FTP/1.1\r\n\r\n", Route::Metrics),
          (b"\xff\xfe /readyz HTTP/1.1\r\n\r\n", Route::Metrics),
      ];
      for (head, want) in cases {
          assert_eq!(route(head), *want, "head {:?}", String::from_utf8_lossy(head));
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server metrics::router -- --test-threads=1`. Expected: compile error `cannot find function route`.
- [ ] **Step 3: Implement** (top of `router.rs`):
  ```rust
  //! Routing and rendering for the metrics/health listener (spec §5.8, §10).
  //!
  //! Pure: `route` maps a request head to a [`Route`], and the `render_*`
  //! functions map a probe answer to a [`Response`]. No I/O, no clock, no
  //! lock. `server.rs` does the socket work, and a [`StatusSource`] does the
  //! board loads.
  #![cfg_attr(not(test), allow(dead_code, reason = "wired into metrics::server in Task 5.8 and status::http in Task 5.9"))]

  /// The four reserved, read-only routes.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Endpoint {
      Livez,
      Readyz,
      Startupz,
      Status,
  }

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Route {
      /// Every other path, and every head that does not parse: today's metrics
      /// response, byte for byte (weir-ctl and every scrape config rely on it).
      Metrics,
      Probe { endpoint: Endpoint, head_only: bool, verbose: bool },
      /// A reserved path with a method other than GET or HEAD: 405.
      MethodNotAllowed,
  }

  /// Routes on the request line only: `METHOD SP target SP HTTP/x`, the query
  /// stripped, the path matched exactly and case-sensitively.
  pub(crate) fn route(head: &[u8]) -> Route {
      let line_end = head.windows(2).position(|w| w == b"\r\n").unwrap_or(head.len());
      let Ok(line) = std::str::from_utf8(&head[..line_end]) else {
          return Route::Metrics;
      };
      let mut parts = line.split(' ');
      let (Some(method), Some(target), Some(version), None) =
          (parts.next(), parts.next(), parts.next(), parts.next())
      else {
          return Route::Metrics;
      };
      if method.is_empty() || !version.starts_with("HTTP/") {
          return Route::Metrics;
      }
      let (path, query) = match target.split_once('?') {
          Some((p, q)) => (p, Some(q)),
          None => (target, None),
      };
      let endpoint = match path {
          "/livez" => Endpoint::Livez,
          "/readyz" => Endpoint::Readyz,
          "/startupz" => Endpoint::Startupz,
          "/status" => Endpoint::Status,
          _ => return Route::Metrics,
      };
      let head_only = match method {
          "GET" => false,
          "HEAD" => true,
          _ => return Route::MethodNotAllowed,
      };
      let verbose = query.is_some_and(|q| {
          q.split('&').any(|kv| kv.split_once('=').map_or(kv, |(k, _)| k) == "verbose")
      });
      Route::Probe { endpoint, head_only, verbose }
  }
  ```
- [ ] **Step 4: Run to verify it passes.** Same command. Expected: `routing_table` passes. Then `cargo clippy -p weir-server --all-targets -- -D warnings`. Expected: clean. The module-level `allow(dead_code)` (non-test builds only) covers `route`, `Route`, `Endpoint` and Task 5.6's renderers until they have production callers; Task 5.9 removes it.
- [ ] **Step 5: Commit.** `feat(metrics): route /livez /readyz /startupz /status by exact path; every other path stays metrics`

### Task 5.6: Probe renderers, `StatusSource`, and the response encoder

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/metrics/router.rs`
- Test: `crates/weir-server/src/metrics/router.rs` (`mod tests`)

**Interfaces:**
- Consumes (PR 1, weir-status): `ProbeVerdict { pub ok: bool, pub failing: Vec<ConditionCode>, pub checks: Vec<ProbeCheck> }`, `ProbeCheck { pub code: ConditionCode, pub ok: bool, pub summary: Option<String> }`, `ConditionCode::as_str(self) -> &'static str` with variants `Starting`, `ShuttingDown`, `ListenerDown`, `NoLiveShard`, `WabCapRejecting`, `WorkerExited`, `DrainStopped`, `DrainWedged`; `Thresholds` (`Clone`, `Default`); `LivezCheck`; `PublicStatusDoc` (Task 5.1). `serde_json` as a normal dependency of weir-server (spec §13, PR 1).
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) struct StartupFacts { pub phase: &'static str, pub phase_age_secs: u64, pub replay_remaining: Option<u64> }
  #[derive(Debug, Clone)]
  pub(crate) struct ProbeAnswer { pub verdict: weir_status::ProbeVerdict,
      pub summaries: Vec<(weir_status::ConditionCode, String)>, pub startup: StartupFacts }
  pub(crate) trait StatusSource: Send + Sync + 'static {
      fn probe(&self, endpoint: Endpoint, now: std::time::SystemTime, checks: &[weir_status::LivezCheck],
               thresholds: &weir_status::Thresholds) -> ProbeAnswer;
      fn public_doc(&self, now: std::time::SystemTime, checks: &[weir_status::LivezCheck],
                    thresholds: &weir_status::Thresholds) -> weir_status::PublicStatusDoc;
  }
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) struct Response { pub status: u16, pub reason: &'static str,
      pub content_type: &'static str, pub allow: bool, pub body: Vec<u8> }
  impl Response { pub(crate) fn encode(&self, head_only: bool) -> Vec<u8>; }
  pub(crate) fn render_livez(a: &ProbeAnswer, verbose: bool) -> Response;
  pub(crate) fn render_readyz(a: &ProbeAnswer, verbose: bool) -> Response;
  pub(crate) fn render_startupz(a: &ProbeAnswer) -> Response;
  pub(crate) fn render_status(doc: &weir_status::PublicStatusDoc) -> Response;
  pub(crate) fn render_method_not_allowed() -> Response;
  pub(crate) fn respond(source: &dyn StatusSource, endpoint: Endpoint, verbose: bool,
      now: std::time::SystemTime, checks: &[weir_status::LivezCheck],
      thresholds: &weir_status::Thresholds) -> Response;
  ```

- [ ] **Step 1: Write the failing tests** (append to `router.rs` `mod tests`):
  ```rust
  use weir_status::{ConditionCode, ProbeCheck, ProbeVerdict};

  fn answer(checks: &[(ConditionCode, bool)], summaries: &[(ConditionCode, &str)]) -> ProbeAnswer {
      let failing = checks.iter().filter(|(_, ok)| !ok).map(|(c, _)| *c).collect::<Vec<_>>();
      ProbeAnswer {
          verdict: ProbeVerdict {
              ok: failing.is_empty(),
              failing,
              checks: checks.iter().map(|(code, ok)| ProbeCheck { code: *code, ok: *ok, summary: None }).collect(),
          },
          summaries: summaries.iter().map(|(c, s)| (*c, s.to_string())).collect(),
          startup: StartupFacts { phase: "recovering", phase_age_secs: 12, replay_remaining: Some(40) },
      }
  }

  fn text(r: &Response) -> &str {
      std::str::from_utf8(&r.body).unwrap()
  }

  #[test]
  fn livez_bodies() {
      let ok = answer(&[], &[]);
      assert_eq!((render_livez(&ok, false).status, text(&render_livez(&ok, false))), (200, "ok\n"));
      let bad = answer(
          &[(ConditionCode::NoLiveShard, false), (ConditionCode::ListenerDown, false), (ConditionCode::WorkerExited, true)],
          &[(ConditionCode::NoLiveShard, "every shard's flusher or owning worker is offline"),
            (ConditionCode::ListenerDown, "unix listener is not accepting; restart re-binds it")],
      );
      let r = render_livez(&bad, false);
      assert_eq!((r.status, text(&r)), (503, "not live: no_live_shard,listener_down\n"));
      let r = render_livez(&bad, true);
      assert_eq!(
          text(&r),
          "[-]no_live_shard failed: every shard's flusher or owning worker is offline\n\
           [-]listener_down failed: unix listener is not accepting; restart re-binds it\n\
           [+]worker_exited ok\n\
           not live: no_live_shard,listener_down\n"
      );
  }

  #[test]
  fn readyz_bodies() {
      let bad = answer(
          &[(ConditionCode::Starting, true), (ConditionCode::ShuttingDown, false), (ConditionCode::WabCapRejecting, false)],
          &[(ConditionCode::ShuttingDown, "graceful shutdown; new connections refused"),
            (ConditionCode::WabCapRejecting, "WAB holds 9 of the 8 cap\nINJECTED")],
      );
      let r = render_readyz(&bad, false);
      assert_eq!((r.status, text(&r)), (503, "not ready: shutting_down, wab_cap_rejecting\n"));
      let v = render_readyz(&bad, true);
      // A control character in a summary cannot forge a probe line.
      assert!(text(&v).contains("[-]wab_cap_rejecting failed: WAB holds 9 of the 8 cap INJECTED\n"), "{}", text(&v));
      assert!(text(&v).starts_with("[+]starting ok\n"));
      assert_eq!(text(&render_readyz(&answer(&[], &[]), false)), "ok\n");
  }

  #[test]
  fn startupz_bodies() {
      let starting = answer(&[(ConditionCode::Starting, false)], &[]);
      let r = render_startupz(&starting);
      assert_eq!((r.status, text(&r)), (503, "starting: phase=recovering since=12s replay=40\n"));
      let mut unknown = starting.clone();
      unknown.startup.replay_remaining = None;
      assert_eq!(text(&render_startupz(&unknown)), "starting: phase=recovering since=12s replay=unknown\n");
      assert_eq!(text(&render_startupz(&answer(&[], &[]))), "ok\n");
  }

  #[test]
  fn encode_heads_get_head_and_405() {
      let r = render_livez(&answer(&[], &[]), false);
      assert_eq!(
          r.encode(false),
          b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 3\r\n\
            Cache-Control: no-store\r\nConnection: close\r\n\r\nok\n".to_vec()
      );
      // HEAD: the GET head, Content-Length included, no body.
      assert!(r.encode(true).ends_with(b"Content-Length: 3\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"));
      let m = render_method_not_allowed().encode(false);
      let m = String::from_utf8(m).unwrap();
      assert!(m.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"), "{m}");
      assert!(m.contains("\r\nAllow: GET, HEAD\r\n"), "{m}");
      assert!(m.contains("\r\nCache-Control: no-store\r\n"), "{m}");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server metrics::router -- --test-threads=1`. Expected: compile error `cannot find struct ProbeAnswer`.
- [ ] **Step 3: Implement** (append to `router.rs`, before `mod tests`):
  ```rust
  use std::fmt::Write as _;
  use std::time::SystemTime;

  use weir_status::{ConditionCode, LivezCheck, ProbeVerdict, PublicStatusDoc, Thresholds};

  /// What `/startupz` prints while startup is not complete.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) struct StartupFacts {
      pub phase: &'static str,
      pub phase_age_secs: u64,
      pub replay_remaining: Option<u64>,
  }

  /// One probe's verdict plus the public summaries of its failing codes.
  #[derive(Debug, Clone)]
  pub(crate) struct ProbeAnswer {
      pub verdict: ProbeVerdict,
      /// Public, template-rendered summaries (spec §9.1), only for failing codes.
      pub summaries: Vec<(ConditionCode, String)>,
      pub startup: StartupFacts,
  }

  /// Where the router reads status from. Implemented by `StatusBoard`
  /// (Task 5.9). Implementations do loads only: no lock, no I/O, no await
  /// (spec §9.2 items 4 and 8), so a probe never waits on the pipeline.
  pub(crate) trait StatusSource: Send + Sync + 'static {
      fn probe(&self, endpoint: Endpoint, now: SystemTime, checks: &[LivezCheck], thresholds: &Thresholds) -> ProbeAnswer;
      fn public_doc(&self, now: SystemTime, checks: &[LivezCheck], thresholds: &Thresholds) -> PublicStatusDoc;
  }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub(crate) struct Response {
      pub status: u16,
      pub reason: &'static str,
      pub content_type: &'static str,
      /// Adds `Allow: GET, HEAD` (405 only).
      pub allow: bool,
      pub body: Vec<u8>,
  }

  const TEXT: &str = "text/plain; charset=utf-8";
  const JSON: &str = "application/json; charset=utf-8";

  impl Response {
      fn text(status: u16, reason: &'static str, body: String) -> Self {
          Self { status, reason, content_type: TEXT, allow: false, body: body.into_bytes() }
      }

      /// Route responses carry `Cache-Control: no-store` and `Connection:
      /// close` (spec §10). A HEAD request gets the GET head, Content-Length
      /// included, and no body. The metrics response never goes through here:
      /// its head stays byte-identical to today's.
      pub(crate) fn encode(&self, head_only: bool) -> Vec<u8> {
          let mut head = format!(
              "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
              self.status, self.reason, self.content_type, self.body.len()
          );
          if self.allow {
              head.push_str("Allow: GET, HEAD\r\n");
          }
          head.push_str("\r\n");
          let mut out = head.into_bytes();
          if !head_only {
              out.extend_from_slice(&self.body);
          }
          out
      }
  }

  fn join_codes(codes: &[ConditionCode], sep: &str) -> String {
      codes.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(sep)
  }

  /// `[+]<code> ok` / `[-]<code> failed: <public summary>`, one line per check.
  /// Control characters in a summary become spaces, so a summary can never
  /// forge a second probe line.
  fn verbose_lines(a: &ProbeAnswer) -> String {
      let mut s = String::new();
      for check in &a.verdict.checks {
          if check.ok {
              let _ = writeln!(s, "[+]{} ok", check.code.as_str());
          } else {
              let summary = a
                  .summaries
                  .iter()
                  .find(|(c, _)| *c == check.code)
                  .map(|(_, s)| s.replace(|ch: char| ch.is_control(), " "))
                  .unwrap_or_default();
              let _ = writeln!(s, "[-]{} failed: {summary}", check.code.as_str());
          }
      }
      s
  }

  fn render_probe(a: &ProbeAnswer, verbose: bool, failed: &str, sep: &str) -> Response {
      let mut body = if verbose { verbose_lines(a) } else { String::new() };
      if a.verdict.ok {
          body.push_str("ok\n");
          Response::text(200, "OK", body)
      } else {
          let _ = writeln!(body, "{failed}: {}", join_codes(&a.verdict.failing, sep));
          Response::text(503, "Service Unavailable", body)
      }
  }

  /// `200 ok` or `503 not live: <code>[,<code>]` (spec §10, separator as written there).
  pub(crate) fn render_livez(a: &ProbeAnswer, verbose: bool) -> Response {
      render_probe(a, verbose, "not live", ",")
  }

  /// `200 ok` or `503 not ready: <code>[, <code>]` (spec §10).
  pub(crate) fn render_readyz(a: &ProbeAnswer, verbose: bool) -> Response {
      render_probe(a, verbose, "not ready", ", ")
  }

  /// `200 ok` once the daemon has ever served (sticky), else
  /// `503 starting: phase=<p> since=<s>s replay=<n>`.
  pub(crate) fn render_startupz(a: &ProbeAnswer) -> Response {
      if a.verdict.ok {
          return Response::text(200, "OK", "ok\n".into());
      }
      let replay = a.startup.replay_remaining.map_or_else(|| "unknown".to_string(), |n| n.to_string());
      Response::text(
          503,
          "Service Unavailable",
          format!("starting: phase={} since={}s replay={replay}\n", a.startup.phase, a.startup.phase_age_secs),
      )
  }

  /// Always 200 with the public document; 500 with an empty body if it
  /// cannot be serialized (spec §10).
  pub(crate) fn render_status(doc: &PublicStatusDoc) -> Response {
      match serde_json::to_vec(doc) {
          Ok(body) => Response { status: 200, reason: "OK", content_type: JSON, allow: false, body },
          Err(_) => Response { status: 500, reason: "Internal Server Error", content_type: TEXT, allow: false, body: Vec::new() },
      }
  }

  pub(crate) fn render_method_not_allowed() -> Response {
      Response { status: 405, reason: "Method Not Allowed", content_type: TEXT, allow: true, body: b"method not allowed\n".to_vec() }
  }

  /// One probe or status request, start to finish. Synchronous: loads, a pure
  /// evaluation and one serialization, bounded at 256 shards and 40 conditions.
  pub(crate) fn respond(
      source: &dyn StatusSource,
      endpoint: Endpoint,
      verbose: bool,
      now: SystemTime,
      checks: &[LivezCheck],
      thresholds: &Thresholds,
  ) -> Response {
      match endpoint {
          Endpoint::Livez => render_livez(&source.probe(endpoint, now, checks, thresholds), verbose),
          Endpoint::Readyz => render_readyz(&source.probe(endpoint, now, checks, thresholds), verbose),
          Endpoint::Startupz => render_startupz(&source.probe(endpoint, now, checks, thresholds)),
          Endpoint::Status => render_status(&source.public_doc(now, checks, thresholds)),
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server metrics::router -- --test-threads=1`. Expected: 5 passed.
- [ ] **Step 5: Commit.** `feat(metrics): render /livez /readyz /startupz /status and 405 responses`

### Task 5.7: `read_head`: the bounded, deadline-bounded request-head reader

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/metrics/server.rs` (new constants after `SCRAPE_IO_TIMEOUT` at :32; new `HeadRead` and `read_head`)
- Test: `crates/weir-server/src/metrics/server.rs` (`mod tests`)

**Interfaces:**
- Produces:
  ```rust
  pub(crate) const HEAD_READ_SLOTS: usize = 64;
  pub(crate) const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(1);
  pub(crate) const HEAD_MAX_BYTES: usize = 4096;
  pub(crate) const PROBE_SLOTS: usize = 4;
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum HeadRead { Complete(Vec<u8>), TooLarge(Vec<u8>), Eof(Vec<u8>), TimedOut, Failed }
  pub(crate) async fn read_head<R: tokio::io::AsyncRead + Unpin>(r: &mut R, max: usize, deadline: Duration) -> HeadRead;
  ```

- [ ] **Step 1: Write the failing tests** (append to `server.rs` `mod tests`):
  ```rust
  use std::time::Instant;
  use tokio::io::duplex;

  #[tokio::test]
  async fn head_split_across_writes_and_across_the_terminator_is_complete() {
      let (mut client, mut server) = duplex(8192);
      let reader = tokio::spawn(async move { read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await });
      client.write_all(b"GET /readyz HTTP/1.1\r\n").await.unwrap();
      tokio::time::sleep(Duration::from_millis(30)).await;
      client.write_all(b"Host: x\r\n\r").await.unwrap();
      tokio::time::sleep(Duration::from_millis(30)).await;
      client.write_all(b"\n").await.unwrap();
      assert_eq!(
          reader.await.unwrap(),
          HeadRead::Complete(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n".to_vec())
      );
  }

  #[tokio::test]
  async fn head_without_terminator_stops_at_the_byte_bound() {
      let (mut client, mut server) = duplex(16384);
      client.write_all(&[b'a'; 5000]).await.unwrap();
      match read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await {
          HeadRead::TooLarge(b) => assert_eq!(b.len(), HEAD_MAX_BYTES),
          other => panic!("expected TooLarge, got {other:?}"),
      }
  }

  #[tokio::test]
  async fn silent_peer_times_out_at_the_deadline() {
      let (mut client, mut server) = duplex(1024);
      client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap(); // no blank line, ever
      let t = Instant::now();
      assert_eq!(read_head(&mut server, HEAD_MAX_BYTES, Duration::from_millis(200)).await, HeadRead::TimedOut);
      let e = t.elapsed();
      assert!(e >= Duration::from_millis(190) && e < Duration::from_millis(800), "{e:?}");
      drop(client);
  }

  #[tokio::test]
  async fn half_closed_peer_returns_what_it_sent() {
      let (mut client, mut server) = duplex(1024);
      client.write_all(b"GARBAGE not http").await.unwrap();
      drop(client);
      assert_eq!(read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await, HeadRead::Eof(b"GARBAGE not http".to_vec()));
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server metrics::server -- --test-threads=1`. Expected: compile error `cannot find function read_head`.
- [ ] **Step 3: Implement** in `server.rs`:
  ```rust
  /// Cheap slots for the head phase (spec §5.8 step 1). Every connection takes
  /// one before anything else, and holds it for at most `HEAD_READ_TIMEOUT`.
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  pub(crate) const HEAD_READ_SLOTS: usize = 64;
  /// A peer that has not sent a complete request head by then is closed, so
  /// it can no longer hold a scrape or probe permit for `SCRAPE_IO_TIMEOUT`.
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  pub(crate) const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(1);
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  pub(crate) const HEAD_MAX_BYTES: usize = 4096;
  /// The four health routes' own pool: a scrape storm cannot starve kubelet,
  /// and a probe flood cannot starve Prometheus.
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  pub(crate) const PROBE_SLOTS: usize = 4;
  /// After an oversized head: how long to read and discard what the peer still
  /// sends after our FIN, so the close is not a RST.
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  const OVERSIZE_DRAIN: Duration = Duration::from_millis(100);

  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum HeadRead {
      /// `\r\n\r\n` arrived. Holds every byte read.
      Complete(Vec<u8>),
      /// `max` bytes and no terminator.
      TooLarge(Vec<u8>),
      /// The peer half-closed before a terminator. Possibly empty.
      Eof(Vec<u8>),
      TimedOut,
      Failed,
  }

  /// Reads until `\r\n\r\n`, `max` bytes, EOF or `deadline`, whichever comes
  /// first. Reading the whole head also drains the request headers, so a close
  /// after responding sends FIN, not RST (spec §5.8).
  #[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
  pub(crate) async fn read_head<R: tokio::io::AsyncRead + Unpin>(
      r: &mut R,
      max: usize,
      deadline: Duration,
  ) -> HeadRead {
      let read = async move {
          let mut buf: Vec<u8> = Vec::with_capacity(512);
          let mut chunk = [0u8; 1024];
          loop {
              let room = (max - buf.len()).min(chunk.len());
              let n = match r.read(&mut chunk[..room]).await {
                  Ok(n) => n,
                  Err(_) => return HeadRead::Failed,
              };
              if n == 0 {
                  return HeadRead::Eof(buf);
              }
              // Re-scan 3 bytes of overlap: the terminator may straddle reads.
              let from = buf.len().saturating_sub(3);
              buf.extend_from_slice(&chunk[..n]);
              if buf[from..].windows(4).any(|w| w == b"\r\n\r\n") {
                  return HeadRead::Complete(buf);
              }
              if buf.len() >= max {
                  return HeadRead::TooLarge(buf);
              }
          }
      };
      match tokio::time::timeout(deadline, read).await {
          Ok(outcome) => outcome,
          Err(_) => HeadRead::TimedOut,
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.** Same command. Expected: the 4 new tests and the 6 existing ones pass (the existing server is not changed yet). Then `cargo clippy -p weir-server --all-targets -- -D warnings`. Expected: clean; the `cfg_attr(not(test), allow(dead_code))` on the five constants, `HeadRead` and `read_head` holds until Task 5.8's `handle()` uses them.
- [ ] **Step 5: Commit.** `feat(metrics): read the full request head within 1 s and 4096 bytes`

### Task 5.8: `HttpState`, the three pools, and the new connection handling

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/metrics/server.rs` (replace `spawn` :51-57, `serve` :59-88 and `handle` :90-127; module doc :1-8; migrate the 6 existing tests)
- Modify: `crates/weir-server/src/main.rs` (the metrics bind at startup step 5 after PR 4; at 2491883 the bind and spawn are main.rs:1459-1467). Verified against 2491883: `spawn_lock_integrity_thread` (main.rs:825) is a std thread started at lock acquisition (main.rs:998-1004), before preflight and the runtime, so it spans the settle wait and precedes the metrics bind; this task only changes the arguments of the existing metrics spawn, so the integrity loop is unaffected.
- Test: `crates/weir-server/src/metrics/server.rs` (`mod tests`)

**Interfaces:**
- Consumes: Task 5.4 `HttpDroppedLabel`, `HttpDropReason`, `Metrics::http_requests_dropped`; Task 5.5 `route`, `Route`; Task 5.6 `StatusSource`, `respond`, `render_method_not_allowed`, `Response::encode`; Task 5.7 `read_head`, constants.
- Produces:
  ```rust
  #[derive(Clone)]
  pub(crate) struct HttpPools { pub head: Arc<Semaphore>, pub metrics: Arc<Semaphore>, pub probe: Arc<Semaphore> }
  impl HttpPools { pub(crate) fn new(metrics_max_connections: usize) -> Self; }
  #[derive(Clone)]
  pub(crate) struct HttpState {
      pub registry: Arc<Registry>,
      pub metrics: Option<Arc<Metrics>>,
      pub status: Option<Arc<dyn StatusSource>>,
      pub thresholds: weir_status::Thresholds,
      pub livez_checks: Arc<[weir_status::LivezCheck]>,
      pub pools: HttpPools,
  }
  impl HttpState {
      pub(crate) fn new(registry: Arc<Registry>, metrics: Option<Arc<Metrics>>,
          status: Option<Arc<dyn StatusSource>>, livez_checks: Vec<weir_status::LivezCheck>,
          metrics_max_connections: usize) -> Self;
      #[cfg(test)] pub(crate) fn metrics_only(registry: Arc<Registry>, metrics_max_connections: usize) -> Self;
  }
  pub(crate) fn spawn(listener: TcpListener, state: HttpState) -> JoinHandle<()>;
  ```

- [ ] **Step 1: Write the failing tests.** Migrate the existing tests mechanically: every `spawn(listener, Arc::new(registry), N)` becomes `spawn(listener, HttpState::metrics_only(Arc::new(registry), N))`. Replace the body of `endpoint_caps_concurrent_connections`:
  ```rust
  #[tokio::test]
  async fn endpoint_caps_concurrent_connections() {
      // The cap is unchanged: with every scrape permit taken, a complete scrape
      // is closed without a response. What changed (spec §5.8) is WHEN a permit
      // is taken: after the request head, so the test takes it itself instead
      // of parking an idle peer on it (see idle_peer_holds_only_a_head_slot).
      let (_metrics, registry) = Metrics::new();
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();
      let state = HttpState::metrics_only(Arc::new(registry), 1);
      let _held = Arc::clone(&state.pools.metrics).try_acquire_owned().unwrap();
      let _handle = spawn(listener, state);

      let mut conn = TcpStream::connect(addr).await.unwrap();
      conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").await.unwrap();
      let mut buf = [0u8; 256];
      let read = tokio::time::timeout(Duration::from_millis(500), conn.read(&mut buf))
          .await
          .expect("the server closes at once when the scrape pool is full")
          .expect("a clean close, not a reset");
      assert_eq!(read, 0, "got {read} bytes: {:?}", &buf[..read]);
  }
  ```
  Add the fake source and helpers:
  ```rust
  use crate::metrics::router::{Endpoint, ProbeAnswer, StartupFacts, StatusSource};
  use crate::metrics::{HttpDropReason, HttpDroppedLabel};
  use std::sync::atomic::{AtomicBool, Ordering};
  use weir_status::{ConditionCode, LivezCheck, ProbeCheck, ProbeVerdict, PublicStatusDoc, Thresholds};

  const GOLDEN_ADMIN: &str = include_str!("../../../weir-status/tests/golden/status_v1_admin.json");

  struct FakeSource {
      ready: AtomicBool,
  }
  impl StatusSource for FakeSource {
      fn probe(&self, endpoint: Endpoint, _now: std::time::SystemTime, _c: &[LivezCheck], _t: &Thresholds) -> ProbeAnswer {
          let ok = endpoint != Endpoint::Readyz || self.ready.load(Ordering::Relaxed);
          ProbeAnswer {
              verdict: ProbeVerdict {
                  ok,
                  failing: if ok { vec![] } else { vec![ConditionCode::Starting] },
                  checks: vec![ProbeCheck { code: ConditionCode::Starting, ok, summary: None }],
              },
              summaries: vec![(ConditionCode::Starting, "startup phase recovering for 3s".into())],
              startup: StartupFacts { phase: "serving", phase_age_secs: 1, replay_remaining: Some(0) },
          }
      }
      fn public_doc(&self, _now: std::time::SystemTime, _c: &[LivezCheck], _t: &Thresholds) -> PublicStatusDoc {
          weir_status::public(&serde_json::from_str(GOLDEN_ADMIN).unwrap())
      }
  }

  /// A router with a status source and counted drops.
  fn routed(max: usize) -> (HttpState, Arc<Metrics>) {
      let (m, reg) = Metrics::new();
      let m = Arc::new(m);
      let source: Arc<dyn StatusSource> = Arc::new(FakeSource { ready: AtomicBool::new(true) });
      (HttpState::new(Arc::new(reg), Some(Arc::clone(&m)), Some(source), Vec::new(), max), m)
  }

  fn dropped(m: &Metrics, reason: HttpDropReason) -> u64 {
      m.http_requests_dropped.get_or_create(&HttpDroppedLabel { reason }).get()
  }

  async fn start(state: HttpState) -> std::net::SocketAddr {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();
      let _ = spawn(listener, state);
      addr
  }

  async fn exchange(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
      let mut s = TcpStream::connect(addr).await.unwrap();
      s.write_all(request).await.unwrap();
      let mut out = Vec::new();
      tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out))
          .await
          .expect("response within 5 s")
          .expect("a full response, not a reset");
      out
  }

  async fn idle(addr: std::net::SocketAddr, n: usize) -> Vec<TcpStream> {
      let mut v = Vec::with_capacity(n);
      for _ in 0..n {
          v.push(TcpStream::connect(addr).await.unwrap());
      }
      tokio::time::sleep(Duration::from_millis(100)).await; // let the server accept them all
      v
  }
  ```
  New tests:
  ```rust
  /// Today's `handle()` response, for comparison: head and body format.
  fn legacy_metrics_response(registry: &Registry) -> Vec<u8> {
      let mut body = String::new();
      prometheus_client::encoding::text::encode(&mut body, registry).unwrap();
      let mut out = format!(
          "HTTP/1.1 200 OK\r\n\
           Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
           Content-Length: {}\r\n\
           \r\n",
          body.len()
      )
      .into_bytes();
      out.extend_from_slice(body.as_bytes());
      out
  }

  #[tokio::test]
  async fn metrics_paths_are_byte_identical_to_the_pre_router_response() {
      let (state, _m) = routed(8);
      let registry = Arc::clone(&state.registry);
      let addr = start(state).await;
      for path in ["/metrics", "/", "/foo", "/READYZ", "/readyz/", "/metrics?x=1"] {
          let got = exchange(addr, format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes()).await;
          assert_eq!(
              String::from_utf8_lossy(&got),
              String::from_utf8_lossy(&legacy_metrics_response(&registry)),
              "path {path}"
          );
      }
  }

  #[tokio::test]
  async fn routes_answer_get_head_and_405() {
      let (state, _m) = routed(8);
      let addr = start(state).await;
      let livez = String::from_utf8(exchange(addr, b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n").await).unwrap();
      assert!(livez.starts_with("HTTP/1.1 200 OK\r\n"), "{livez}");
      assert!(livez.contains("\r\nCache-Control: no-store\r\nConnection: close\r\n"), "{livez}");
      assert!(livez.ends_with("\r\n\r\nok\n"), "{livez}");
      let head = String::from_utf8(exchange(addr, b"HEAD /readyz HTTP/1.1\r\n\r\n").await).unwrap();
      assert!(head.contains("Content-Length: 3\r\n") && head.ends_with("\r\n\r\n"), "{head}");
      let post = String::from_utf8(exchange(addr, b"POST /status HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await).unwrap();
      assert!(post.starts_with("HTTP/1.1 405 Method Not Allowed\r\n") && post.contains("Allow: GET, HEAD\r\n"), "{post}");
      let status = String::from_utf8(exchange(addr, b"GET /status HTTP/1.1\r\n\r\n").await).unwrap();
      assert!(status.contains("Content-Type: application/json; charset=utf-8\r\n"), "{status}");
      let body = &status[status.find("\r\n\r\n").unwrap() + 4..];
      let v: serde_json::Value = serde_json::from_str(body).unwrap();
      assert_eq!((v["schema"].as_str(), v["view"].as_str()), (Some("weir.status/v1"), Some("public")));
  }

  #[tokio::test]
  async fn request_head_split_across_writes_gets_a_full_response() {
      let (state, _m) = routed(8);
      let addr = start(state).await;
      let mut s = TcpStream::connect(addr).await.unwrap();
      s.write_all(b"GET /metrics HTTP/1.1\r\n").await.unwrap();
      tokio::time::sleep(Duration::from_millis(100)).await;
      s.write_all(b"Host: x\r\nAccept: */*\r\n\r\n").await.unwrap();
      let mut out = Vec::new();
      s.read_to_end(&mut out).await.expect("FIN after the full head, not RST");
      let text = String::from_utf8_lossy(&out);
      let split = text.find("\r\n\r\n").unwrap();
      let len: usize = text[..split]
          .lines()
          .find_map(|l| l.strip_prefix("Content-Length: "))
          .unwrap()
          .parse()
          .unwrap();
      assert_eq!(out.len() - split - 4, len, "the whole body arrived");
  }

  #[tokio::test]
  async fn silent_peer_is_closed_after_the_head_deadline() {
      let (state, m) = routed(8);
      let addr = start(state).await;
      let mut s = TcpStream::connect(addr).await.unwrap();
      let t = std::time::Instant::now();
      let mut buf = [0u8; 16];
      let n = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf)).await.unwrap().unwrap();
      assert_eq!(n, 0);
      let e = t.elapsed();
      assert!(e >= Duration::from_millis(900) && e < Duration::from_millis(2000), "{e:?}");
      assert_eq!(dropped(&m, HttpDropReason::head_timeout), 1);
  }

  #[tokio::test]
  async fn idle_peer_holds_only_a_head_slot() {
      // The intended change behind the rewritten endpoint_caps_concurrent_connections:
      // with metrics_max_connections = 1, an idle peer no longer blocks the scrape.
      let (state, _m) = routed(1);
      let addr = start(state).await;
      let _idle = idle(addr, 1).await;
      let got = exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n").await;
      assert!(got.starts_with(b"HTTP/1.1 200 OK\r\n"));
  }

  #[tokio::test]
  async fn eight_idle_connections_do_not_prevent_a_readyz_200() {
      let (state, _m) = routed(8);
      let addr = start(state).await;
      let _idle = idle(addr, 8).await;
      let t = std::time::Instant::now();
      let got = String::from_utf8(exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n").await).unwrap();
      assert!(got.starts_with("HTTP/1.1 200 OK\r\n") && got.ends_with("ok\n"), "{got}");
      assert!(t.elapsed() < Duration::from_millis(500));
  }

  #[tokio::test]
  async fn sixty_four_idle_connections_are_shed_within_a_second() {
      let (state, m) = routed(8);
      let addr = start(state).await;
      let mut held = idle(addr, HEAD_READ_SLOTS).await;
      // The head pool is full: a 65th connection is closed at once, uncounted
      // against any scrape or probe permit.
      let mut extra = TcpStream::connect(addr).await.unwrap();
      let mut buf = [0u8; 16];
      let n = tokio::time::timeout(Duration::from_millis(300), extra.read(&mut buf)).await.unwrap().unwrap();
      assert_eq!(n, 0);
      assert!(dropped(&m, HttpDropReason::head_pool_full) >= 1);
      // Every idle peer is closed by the head deadline.
      let t = std::time::Instant::now();
      for s in &mut held {
          let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await.unwrap().unwrap();
          assert_eq!(n, 0);
      }
      assert!(t.elapsed() < Duration::from_millis(1500), "{:?}", t.elapsed());
      let got = exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n").await;
      assert!(got.starts_with(b"HTTP/1.1 200 OK\r\n"));
  }

  #[tokio::test]
  async fn a_full_scrape_pool_does_not_block_probes() {
      let (state, m) = routed(1);
      let _held = Arc::clone(&state.pools.metrics).try_acquire_owned().unwrap();
      let addr = start(state).await;
      assert!(exchange(addr, b"GET /readyz HTTP/1.1\r\n\r\n").await.starts_with(b"HTTP/1.1 200 OK\r\n"));
      assert!(exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n").await.is_empty());
      assert_eq!(dropped(&m, HttpDropReason::metrics_pool_full), 1);
  }

  #[tokio::test]
  async fn a_full_probe_pool_does_not_block_scrapes() {
      let (state, m) = routed(8);
      let _held: Vec<_> = (0..PROBE_SLOTS)
          .map(|_| Arc::clone(&state.pools.probe).try_acquire_owned().unwrap())
          .collect();
      let addr = start(state).await;
      assert!(exchange(addr, b"GET /metrics HTTP/1.0\r\n\r\n").await.starts_with(b"HTTP/1.1 200 OK\r\n"));
      assert!(exchange(addr, b"GET /livez HTTP/1.1\r\n\r\n").await.is_empty());
      assert_eq!(dropped(&m, HttpDropReason::probe_pool_full), 1);
  }

  #[tokio::test]
  async fn oversized_head_gets_a_response_then_a_clean_close() {
      let (state, _m) = routed(8);
      let addr = start(state).await;
      let mut req = b"GET /livez HTTP/1.1\r\nX-Pad: ".to_vec();
      req.extend(std::iter::repeat_n(b'a', 6000));
      let got = exchange(addr, &req).await;
      assert!(got.starts_with(b"HTTP/1.1 200 OK\r\n") && got.ends_with(b"ok\n"), "{}", String::from_utf8_lossy(&got));
  }

  #[tokio::test]
  async fn metrics_only_keeps_the_any_path_contract() {
      let (_m, registry) = Metrics::new();
      let registry = Arc::new(registry);
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();
      let _ = spawn(listener, HttpState::metrics_only(Arc::clone(&registry), 8));
      for path in ["/readyz", "/status", "/livez?verbose"] {
          let got = exchange(addr, format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes()).await;
          assert_eq!(got, legacy_metrics_response(&registry), "{path}");
      }
  }
  ```
  `std::iter::repeat_n` is stable since 1.82, under the 1.88 MSRV.
- [ ] **Step 2: Run them to verify they fail.** `cargo test -p weir-server --bin weir-server metrics::server -- --test-threads=1`. Expected: compile error `cannot find struct HttpState`.
- [ ] **Step 3: Implement.** Module doc (replaces :1-8):
  ```rust
  //! HTTP server for the metrics/health listener (spec §5.8, §10).
  //!
  //! Every connection goes through two phases. **Head:** one of
  //! `HEAD_READ_SLOTS` cheap slots, held while the request head is read
  //! (≤ 4096 bytes, ≤ 1 s). **Route:** `/livez`, `/readyz`, `/startupz` and
  //! `/status` take a permit from a pool of `PROBE_SLOTS`; every other path
  //! takes one of `metrics_max_connections` and gets today's metrics response
  //! byte for byte. Writes are bounded by `SCRAPE_IO_TIMEOUT`. No route reads a
  //! body or changes state.
  ```
  Types, `spawn` and `serve`:
  ```rust
  use tokio::sync::OwnedSemaphorePermit;
  use weir_status::{LivezCheck, Thresholds};
  use crate::metrics::router::{self, Route, StatusSource};
  use crate::metrics::{HttpDropReason, HttpDroppedLabel, Metrics};

  #[derive(Clone)]
  pub(crate) struct HttpPools {
      pub head: Arc<Semaphore>,
      pub metrics: Arc<Semaphore>,
      pub probe: Arc<Semaphore>,
  }

  impl HttpPools {
      pub(crate) fn new(metrics_max_connections: usize) -> Self {
          Self {
              head: Arc::new(Semaphore::new(HEAD_READ_SLOTS)),
              metrics: Arc::new(Semaphore::new(metrics_max_connections)),
              probe: Arc::new(Semaphore::new(PROBE_SLOTS)),
          }
      }
  }

  #[derive(Clone)]
  pub(crate) struct HttpState {
      pub registry: Arc<Registry>,
      /// `None` only in tests: drops are then not counted.
      pub metrics: Option<Arc<Metrics>>,
      /// `None`: every path gets metrics (the pre-router contract).
      pub status: Option<Arc<dyn StatusSource>>,
      pub thresholds: Thresholds,
      pub livez_checks: Arc<[LivezCheck]>,
      pub pools: HttpPools,
  }

  impl HttpState {
      pub(crate) fn new(
          registry: Arc<Registry>,
          metrics: Option<Arc<Metrics>>,
          status: Option<Arc<dyn StatusSource>>,
          livez_checks: Vec<LivezCheck>,
          metrics_max_connections: usize,
      ) -> Self {
          Self {
              registry,
              metrics,
              status,
              thresholds: Thresholds::default(),
              livez_checks: livez_checks.into(),
              pools: HttpPools::new(metrics_max_connections),
          }
      }

      #[cfg(test)]
      pub(crate) fn metrics_only(registry: Arc<Registry>, metrics_max_connections: usize) -> Self {
          Self::new(registry, None, None, Vec::new(), metrics_max_connections)
      }

      fn count_drop(&self, reason: HttpDropReason) {
          if let Some(m) = &self.metrics {
              m.http_requests_dropped.get_or_create(&HttpDroppedLabel { reason }).inc();
          }
      }
  }

  /// Spawns the server as a tokio task. It never ends on its own (G14): it
  /// stops when its runtime shuts down or the handle is aborted. Must be
  /// called inside a tokio runtime.
  pub(crate) fn spawn(listener: TcpListener, state: HttpState) -> JoinHandle<()> {
      tokio::spawn(serve(listener, state))
  }

  async fn serve(listener: TcpListener, state: HttpState) {
      loop {
          let (stream, _peer) = match listener.accept().await {
              Ok(s) => s,
              Err(e) => {
                  // unchanged from today (G14): log, back off, continue
                  debug!(error = %e, "metrics: accept error; backing off and continuing");
                  tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                  continue;
              }
          };
          // try_acquire never blocks: a full head pool closes the connection
          // at once (the stream drops here).
          let Ok(head_permit) = Arc::clone(&state.pools.head).try_acquire_owned() else {
              state.count_drop(HttpDropReason::head_pool_full);
              continue;
          };
          let state = state.clone();
          tokio::spawn(handle(stream, state, head_permit));
      }
  }

  async fn handle(mut stream: TcpStream, state: HttpState, head_permit: OwnedSemaphorePermit) {
      let head = read_head(&mut stream, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await;
      drop(head_permit);
      let (bytes, oversized) = match head {
          HeadRead::Complete(b) => (b, false),
          HeadRead::Eof(b) if !b.is_empty() => (b, false),
          HeadRead::TooLarge(b) => (b, true),
          HeadRead::Eof(_) | HeadRead::Failed => return,
          HeadRead::TimedOut => {
              state.count_drop(HttpDropReason::head_timeout);
              return;
          }
      };
      let route = match &state.status {
          None => Route::Metrics,
          Some(_) => router::route(&bytes),
      };
      match route {
          Route::Metrics => {
              let Ok(_permit) = Arc::clone(&state.pools.metrics).try_acquire_owned() else {
                  state.count_drop(HttpDropReason::metrics_pool_full);
                  return;
              };
              write_metrics(&mut stream, &state.registry).await;
          }
          Route::Probe { .. } | Route::MethodNotAllowed => {
              let Ok(_permit) = Arc::clone(&state.pools.probe).try_acquire_owned() else {
                  state.count_drop(HttpDropReason::probe_pool_full);
                  return;
              };
              let Some(source) = state.status.as_deref() else { return };
              let (response, head_only) = match route {
                  Route::Probe { endpoint, head_only, verbose } => (
                      router::respond(source, endpoint, verbose, std::time::SystemTime::now(), &state.livez_checks, &state.thresholds),
                      head_only,
                  ),
                  _ => (router::render_method_not_allowed(), false),
              };
              let bytes = response.encode(head_only);
              let _ = tokio::time::timeout(SCRAPE_IO_TIMEOUT, stream.write_all(&bytes)).await;
          }
      }
      if oversized {
          finish_oversized(&mut stream).await;
      }
  }

  /// Today's `handle()` response, unchanged: same head, same body, no new
  /// headers (spec §10), writes bounded by `SCRAPE_IO_TIMEOUT` (F20).
  async fn write_metrics(stream: &mut TcpStream, registry: &Registry) {
      let mut body = String::new();
      if encode(&mut body, registry).is_err() {
          let _ = tokio::time::timeout(
              SCRAPE_IO_TIMEOUT,
              stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\n\r\n"),
          )
          .await;
          return;
      }
      let body_bytes = body.as_bytes();
      let response_head = format!(
          "HTTP/1.1 200 OK\r\n\
           Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
           Content-Length: {}\r\n\
           \r\n",
          body_bytes.len()
      );
      let write = async {
          stream.write_all(response_head.as_bytes()).await?;
          stream.write_all(body_bytes).await
      };
      let _ = tokio::time::timeout(SCRAPE_IO_TIMEOUT, write).await;
  }

  /// The head hit 4096 bytes, so the peer may still be sending: FIN our half,
  /// then read and discard for up to 100 ms, so the close is not a RST that
  /// destroys the response in the peer's receive buffer (spec §5.8).
  async fn finish_oversized(stream: &mut TcpStream) {
      let _ = stream.shutdown().await;
      let mut sink = [0u8; 1024];
      let _ = tokio::time::timeout(OVERSIZE_DRAIN, async {
          while let Ok(n) = stream.read(&mut sink).await {
              if n == 0 {
                  break;
              }
          }
      })
      .await;
  }
  ```
  `main.rs`, the metrics spawn (main.rs:1463-1467 at 2491883):
  ```rust
  let http_state = metrics::server::HttpState::new(
      Arc::clone(&registry),
      Some(Arc::clone(&metrics)),
      None, // Task 5.10 wires the status board; until then every path is metrics
      config.livez_checks.clone(),
      config.metrics_max_connections,
  );
  metrics::server::spawn(metrics_listener, http_state);
  ```
  Remove the seven `#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]` lines Task 5.7 added to server.rs: `handle()` now uses every one of those items. Keep router.rs's module-level allowance: `ProbeAnswer` and `StartupFacts` have no production constructor until Task 5.9.
- [ ] **Step 4: Run to verify they pass.** `cargo test -p weir-server --bin weir-server metrics:: -- --test-threads=1`. Expected: all server and router tests pass (6 migrated, 11 new). Then `cargo test -p weir-server --test system -- --test-threads=1` to confirm nothing that scrapes `/metrics` regressed (testkit's `scrape_metrics` sends `GET /metrics HTTP/1.0\r\n\r\n`, a complete head). Then `cargo clippy -p weir-server --all-targets -- -D warnings`. Expected: clean.
- [ ] **Step 5: Commit.** `feat(metrics): split the listener into a 1 s head phase and separate scrape and probe pools`

### Task 5.9: `impl StatusSource for StatusBoard`: readiness and liveness from the live board

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Create: `crates/weir-server/src/status/http.rs` (glue only; it holds no state and takes no lock, spec §9.2 rule 5)
- Modify: `crates/weir-server/src/status/mod.rs` (add `mod http;`; no lock-scan edit: PR 1 Task 1.16's `no_blocking_locks_under_status` walks every `.rs` file in `src/status/`, so it scans http.rs as soon as the file exists)
- Modify: `crates/weir-server/src/metrics/router.rs` (remove Task 5.5's module-level dead-code allowance)
- Test: `crates/weir-server/src/status/http.rs` (`mod tests`)

**Interfaces:**
- Consumes (PR 1 `status::*`, PR 4 writers; exact names as PR 1 defines them):
  ```rust
  impl StatusBoard {
      pub(crate) fn snapshot(&self, now: SystemTime) -> StatusSnapshot;   // loads only
      pub(crate) latches: ConditionLatches,                                // impl weir_status::SinceLookup
      pub(crate) life: LifecycleCell,      // enter(Phase, now_ms) -> bool (sets ever_served on Serving), begin_shutdown(now_ms) -> bool
      pub(crate) listeners: ListenerCells, // .unix.set(ListenerState), .tcp.set(ListenerState)
      pub(crate) shards: Arc<ShardRegistry>, // mark_up(shard: usize), mark_exit(shard: usize, SupervisorExit); workers live here too
      pub(crate) sink: SinkCell,           // last_error / probe_reason: ArcSwapOption<String>;
                                           // set_health(&SinkHealth, reason: Option<String>)
      pub(crate) wab_cap_rejecting: Arc<AtomicBool>,
      pub(crate) drain: DrainCell,         // set_state(DrainStateValue), set_pause_requested(bool, Option<u32>),
                                           // set_last_requeue(RequeueResult), set_exit(DrainExitInfo)
      pub(crate) reload: ReloadCell,       // publish(crate::reload::ReloadReport, now_ms: u64)
      pub(crate) fn new(statik: StaticInfo, shard_count: usize, worker_count: usize, tcp_configured: bool,
          wab_cap_rejecting: Arc<AtomicBool>, wab_bytes_now: Arc<AtomicU64>) -> Arc<Self>;
  }
  impl SinkCell { pub(crate) fn record_failure(&self, weir_status::SinkFailureKind, message: String); }
  pub(crate) struct status::board::StaticInfo { pub wab_dir: PathBuf, pub socket_path: PathBuf, /* … */ }  // + #[cfg(test)] for_test()
  pub(crate) use status::{RequeueResult, DrainExitInfo, DrainExitReason};  // board.rs, re-exported by status/mod.rs
  pub(crate) struct crate::reload::ReloadReport { trigger, outcome, changed, rejected: Vec<(&'static str, String)>, note }
  pub(crate) enum SupervisorExit { InputClosed, CounterScanFailed, RespawnExhausted }  // crate::status
  pub(crate) struct WorkerGuard; impl WorkerGuard { fn enter(Arc<ShardRegistry>, worker: usize) -> Self }  // Drop marks exited
  pub enum weir_status::ListenerState { NotConfigured, Pending, Bound, Closed, Failed }
  pub enum crate::sink::SinkHealth { Healthy, Degraded(String), Down(String) }
  pub(crate) fn status::render::render_admin_doc(&StatusBoard, &Thresholds, &[LivezCheck], SystemTime) -> AdminStatusDoc;
  impl StatusSnapshot { fn evidence(&self) -> weir_status::Evidence; }
  #[cfg(test)] pub(crate) fn test_board(shard_count: usize, worker_count: usize) -> Arc<StatusBoard>; // status/mod.rs
  // weir-status
  pub fn evaluate(e: &Evidence, t: &Thresholds, latches: &dyn SinceLookup) -> Evaluation;
  pub fn readiness(e: &Evidence) -> ProbeVerdict;
  pub fn liveness(e: &Evidence, t: &Thresholds, checks: &[LivezCheck]) -> ProbeVerdict;
  pub fn startup(e: &Evidence) -> ProbeVerdict;
  pub struct Evidence { pub now_unix_ms: u64, pub phase: Option<Phase>, pub phase_since_unix_ms: Option<u64>,
      pub drain: Option<DrainEvidence /* pub replay_remaining: u64 */>, /* … */ }
  impl Phase { pub fn as_str(self) -> &'static str; }
  ```
  A shard goes offline through the supervisor's single writer (`mark_exit`): `RespawnExhausted` → `Offline(RespawnExhausted)`, `CounterScanFailed` → `Offline(CounterScanFailed)`, `InputClosed` while not shutting down → `Offline(FlusherExited)`. A worker is "running" while a `WorkerGuard` for it is alive; the tests `mem::forget` one, as PR 1's own poll tests do.
- Produces: `impl crate::metrics::router::StatusSource for crate::status::StatusBoard`; `fn startup_facts(ev: &weir_status::Evidence) -> crate::metrics::router::StartupFacts`.

- [ ] **Step 1: Write the failing tests** (`status/http.rs` `mod tests`):
  ```rust
  use std::sync::Arc;
  use std::sync::atomic::Ordering;
  use std::time::SystemTime;

  use weir_status::{ConditionCode, ListenerState, LivezCheck, Phase, Thresholds};

  use crate::metrics::router::{Endpoint, StatusSource, render_livez, render_readyz, render_startupz};
  use crate::sink::SinkHealth;
  use crate::status::{StatusBoard, SupervisorExit, WorkerGuard, test_board};

  fn now_ms() -> u64 {
      SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
  }

  /// A board after a clean PR 4 startup: phase serving, unix listener bound,
  /// every shard Up and every worker running.
  fn serving_board(shards: usize, workers: usize) -> Arc<StatusBoard> {
      let b = test_board(shards, workers);
      for id in 0..shards {
          b.shards.mark_up(id);
      }
      for w in 0..workers {
          // Keep each worker "running" for the test's life (PR 1's poll tests do the same).
          std::mem::forget(WorkerGuard::enter(Arc::clone(&b.shards), w));
      }
      b.listeners.unix.set(ListenerState::Bound);
      b.life.enter(Phase::Serving, now_ms()); // also sets ever_served
      b
  }

  fn body(r: crate::metrics::router::Response) -> (u16, String) {
      (r.status, String::from_utf8(r.body).unwrap())
  }

  fn readyz(b: &StatusBoard) -> (u16, String) {
      body(render_readyz(&b.probe(Endpoint::Readyz, SystemTime::now(), &[], &Thresholds::default()), false))
  }

  #[test]
  fn a_fresh_board_is_live_but_neither_ready_nor_started() {
      let b = test_board(2, 2);
      let t = Thresholds::default();
      let checks = [LivezCheck::NoLiveShard, LivezCheck::ListenerDown];
      // Liveness checks run only while serving: startup never fails /livez (spec §10, §12).
      assert_eq!(body(render_livez(&b.probe(Endpoint::Livez, SystemTime::now(), &checks, &t), false)), (200, "ok\n".into()));
      let (s, text) = readyz(&b);
      assert_eq!(s, 503);
      assert!(text.starts_with("not ready: ") && text.contains("starting"), "{text}");
      let (s, text) = body(render_startupz(&b.probe(Endpoint::Startupz, SystemTime::now(), &[], &t)));
      assert_eq!(s, 503);
      assert!(text.starts_with("starting: phase=initializing since="), "{text}");
  }

  #[test]
  fn a_serving_board_is_ready_and_stays_started_through_shutdown() {
      let b = serving_board(2, 2);
      assert_eq!(readyz(&b), (200, "ok\n".into()));
      b.life.begin_shutdown(now_ms());
      let (s, text) = readyz(&b);
      assert_eq!(s, 503);
      assert!(text.contains("shutting_down"), "{text}");
      // /startupz is sticky once served (spec §10); /livez never fails on shutdown.
      let t = Thresholds::default();
      assert_eq!(body(render_startupz(&b.probe(Endpoint::Startupz, SystemTime::now(), &[], &t))).0, 200);
      assert_eq!(body(render_livez(&b.probe(Endpoint::Livez, SystemTime::now(), &[LivezCheck::NoLiveShard], &t), false)).0, 200);
  }

  #[test]
  fn readiness_is_ingest_capability_only() {
      // Cap rejecting gates readiness ...
      let b = serving_board(1, 1);
      b.wab_cap_rejecting.store(true, Ordering::Relaxed);
      let (s, text) = readyz(&b);
      assert_eq!(s, 503);
      assert!(text.contains("wab_cap_rejecting"), "{text}");
      // ... as does losing every shard ...
      let b = serving_board(2, 2);
      b.shards.mark_exit(0, SupervisorExit::RespawnExhausted);
      assert_eq!(readyz(&b).0, 200, "one live shard is enough (owner decision 4)");
      b.shards.mark_exit(1, SupervisorExit::CounterScanFailed);
      let (s, text) = readyz(&b);
      assert_eq!(s, 503);
      assert!(text.contains("no_live_shard"), "{text}");
      // ... and a failed producer listener ...
      let b = serving_board(1, 1);
      b.listeners.unix.set(ListenerState::Failed);
      assert!(readyz(&b).1.contains("listener_down"));
      // ... but a down sink never does (spec §12).
      let b = serving_board(1, 1);
      b.sink.last_error.store(Some(Arc::new("connection refused".into())));
      b.sink.set_health(&SinkHealth::Down("connection refused".into()), Some("connection refused".into()));
      assert_eq!(readyz(&b), (200, "ok\n".into()));
  }

  #[test]
  fn liveness_fails_only_on_selected_restart_fixable_checks() {
      let b = serving_board(1, 1);
      b.shards.mark_exit(0, SupervisorExit::InputClosed); // not shutting down: Offline(FlusherExited)
      let t = Thresholds::default();
      let none = b.probe(Endpoint::Livez, SystemTime::now(), &[], &t);
      assert_eq!(body(render_livez(&none, false)), (200, "ok\n".into()), "livez_checks = [] never fails");
      let sel = b.probe(Endpoint::Livez, SystemTime::now(), &[LivezCheck::NoLiveShard], &t);
      let (s, text) = body(render_livez(&sel, true));
      assert_eq!(s, 503);
      assert!(text.contains("[-]no_live_shard failed: "), "{text}");
      assert!(text.ends_with("not live: no_live_shard\n"), "{text}");
      assert!(sel.summaries.iter().any(|(c, _)| *c == ConditionCode::NoLiveShard));
  }

  /// Spec §16 sentinel test, board half: a sentinel in every String-typed
  /// board field (and the pause uid) goes through every board writer, then
  /// through `render_admin_doc` and the evaluator, and reaches neither
  /// `/status` nor the `?verbose` probe lines. Task 5.1 covers `public()` on a
  /// pre-built document; this covers the leaks only a live board can show.
  #[test]
  fn board_free_text_never_reaches_status_or_verbose_probes() {
      const S: &str = "SENTINEL-board-free-text";
      const UID: u32 = 2_718_281_828;
      let b = StatusBoard::new(
          crate::status::board::StaticInfo {
              wab_dir: std::path::PathBuf::from(format!("/{S}-wab")),
              socket_path: std::path::PathBuf::from(format!("/{S}-sock")),
              ..crate::status::board::StaticInfo::for_test()
          },
          1,
          1,
          false,
          Arc::new(std::sync::atomic::AtomicBool::new(false)),
          Arc::new(std::sync::atomic::AtomicU64::new(0)),
      );
      b.shards.mark_up(0);
      std::mem::forget(WorkerGuard::enter(Arc::clone(&b.shards), 0));
      b.life.enter(Phase::Serving, now_ms());
      b.listeners.unix.set(ListenerState::Failed);
      b.sink.set_health(&SinkHealth::Down(format!("{S} down")), Some(format!("{S} probe_reason")));
      b.sink.record_failure(weir_status::SinkFailureKind::Transient, format!("{S} commit error"));
      b.drain.set_state(crate::metrics::DrainStateValue::draining);
      b.drain.set_pause_requested(true, Some(UID));
      b.drain.set_last_requeue(crate::status::RequeueResult {
          ticket: 1,
          requeued: 0,
          already_queued: 0,
          scan_error: Some(format!("{S} scan_error")),
          backoff_cut: false,
          coalesced: false,
          at_ms: now_ms(),
      });
      b.drain.set_exit(crate::status::DrainExitInfo {
          reason: crate::status::DrainExitReason::GaveUp,
          abandoned: None,
          pending_left: 0,
          backlog_left: 0,
          last_panic: Some(format!("{S} panic")),
          at_ms: now_ms(),
      });
      // PR 6 Task 6.8 adds `at_unix_ms` to `ReloadReport`; it adds `at_unix_ms: 0` here.
      b.reload.publish(
          crate::reload::ReloadReport {
              trigger: crate::reload::ReloadTrigger::Sighup,
              outcome: crate::reload::ReloadOutcome::Failed,
              changed: vec![],
              rejected: vec![("tls", format!("{S} reload detail"))],
              note: None,
          },
          now_ms(),
      );
      let t = Thresholds::default();
      let checks = [LivezCheck::ListenerDown, LivezCheck::DrainStopped];
      let doc = serde_json::to_string(&b.public_doc(SystemTime::now(), &checks, &t)).unwrap();
      assert!(!doc.contains("SENTINEL") && !doc.contains(&UID.to_string()), "{doc}");
      for (endpoint, r) in [
          (Endpoint::Livez, render_livez(&b.probe(Endpoint::Livez, SystemTime::now(), &checks, &t), true)),
          (Endpoint::Readyz, render_readyz(&b.probe(Endpoint::Readyz, SystemTime::now(), &checks, &t), true)),
      ] {
          let text = String::from_utf8(r.body).unwrap();
          assert!(!text.contains("SENTINEL") && !text.contains(&UID.to_string()), "{endpoint:?}: {text}");
      }
      // Vacuity guard: the admin rendering of the same board carries every
      // planted value, so each writer above really reached the document.
      let admin =
          serde_json::to_string(&crate::status::render::render_admin_doc(&b, &t, &checks, SystemTime::now())).unwrap();
      for needle in [
          format!("{S} panic"),
          format!("{S} scan_error"),
          format!("{S} reload detail"),
          format!("/{S}-wab"),
          format!("{S} commit error"),
          UID.to_string(),
      ] {
          assert!(admin.contains(&needle), "admin doc lacks {needle}: {admin}");
      }
      assert!(
          doc.contains("\"livez_checks\":[\"listener_down\",\"drain_stopped\"]"),
          "verdict carries the selected checks: {doc}"
      );
  }
  ```
- [ ] **Step 2: Run them to verify they fail.** `cargo test -p weir-server --bin weir-server status::http -- --test-threads=1`. Expected: compile error `the trait StatusSource is not implemented for StatusBoard`.
- [ ] **Step 3: Implement** `status/http.rs`:
  ```rust
  //! The status board as the HTTP router's source (spec §5.8, §10, §12).
  //!
  //! Loads and a pure evaluation only: no lock, no I/O, no await (§9.2).
  //! Readiness and liveness come from `weir_status::{readiness, liveness,
  //! startup}`, the same predicates `weir_ready`, `weir_live`, admin `status`
  //! and offline `weir-ctl status` use (G2), evaluated live on every request.

  use std::time::SystemTime;

  use weir_status::{Evidence, LivezCheck, Phase, PublicStatusDoc, Thresholds};

  use crate::metrics::router::{Endpoint, ProbeAnswer, StartupFacts, StatusSource};
  use crate::status::StatusBoard;

  pub(crate) fn startup_facts(ev: &Evidence) -> StartupFacts {
      StartupFacts {
          phase: ev.phase.map_or("unknown", Phase::as_str),
          phase_age_secs: ev.phase_since_unix_ms.map_or(0, |since| ev.now_unix_ms.saturating_sub(since) / 1000),
          replay_remaining: ev.drain.as_ref().map(|d| d.replay_remaining),
      }
  }

  impl StatusSource for StatusBoard {
      fn probe(&self, endpoint: Endpoint, now: SystemTime, checks: &[LivezCheck], t: &Thresholds) -> ProbeAnswer {
          let ev = self.snapshot(now).evidence();
          let verdict = match endpoint {
              Endpoint::Livez => weir_status::liveness(&ev, t, checks),
              Endpoint::Readyz => weir_status::readiness(&ev),
              Endpoint::Startupz | Endpoint::Status => weir_status::startup(&ev),
          };
          // The 200 path is a handful of loads; summaries are rendered only
          // for a failing probe, and only for its failing codes.
          let summaries = if verdict.ok {
              Vec::new()
          } else {
              weir_status::evaluate(&ev, t, &self.latches)
                  .conditions
                  .into_iter()
                  .filter(|c| verdict.failing.contains(&c.code))
                  .map(|c| (c.code, c.summary))
                  .collect()
          };
          ProbeAnswer { verdict, summaries, startup: startup_facts(&ev) }
      }

      fn public_doc(&self, now: SystemTime, checks: &[LivezCheck], t: &Thresholds) -> PublicStatusDoc {
          // PR 1's one admin rendering (the admin `status` verb, PR 6, uses
          // the same function), then the type-enforced public projection.
          weir_status::public(&crate::status::render::render_admin_doc(self, t, checks, now))
      }
  }
  ```
- [ ] **Step 4: Run to verify they pass.** Remove the `#[cfg_attr(not(test), allow(dead_code, reason = "PR 5 and PR 6 call it"))]` PR 1 put on `render_admin_doc`, and the `#![cfg_attr(not(test), allow(dead_code, …))]` Task 5.5 put at the top of router.rs: the `impl StatusSource for StatusBoard` is now a production caller of `render_admin_doc` and the constructor of `ProbeAnswer` and `StartupFacts`. `cargo test -p weir-server --bin weir-server status:: -- --test-threads=1`. Expected: the 5 new tests pass and `no_blocking_locks_under_status` stays green. `cargo clippy -p weir-server --all-targets -- -D warnings`. Expected: clean.
  Verify the lock scan covers http.rs, by mutation: add `use std::sync::Mutex as _M;` to status/http.rs, after its `//!` doc comment and before the first `use`. Run `cargo test -p weir-server --bin weir-server no_blocking_locks -- --test-threads=1`. Expected: FAIL with a message ending `src/status/http.rs mentions Mutex; status/ must stay wait-free (spec §9.2)`. Revert, rerun: PASS.
- [ ] **Step 5: Mutation check.** In `public_doc`, temporarily bind `let mut doc = crate::status::render::render_admin_doc(self, t, checks, now);`, insert `if let Some(s) = doc.sink.as_mut() { s.sink_type = self.sink.last_error.load_full().map(|e| e.to_string()).unwrap_or_default(); }`, and return `weir_status::public(&doc)`, routing an admin-only string into a field the public type keeps. Run `cargo test -p weir-server --bin weir-server board_free_text -- --test-threads=1`: expected FAIL with `SENTINEL-board-free-text` in the message. Revert and rerun: PASS.
- [ ] **Step 6: Commit.** `feat(status): readiness, liveness and startup from the live board, for the HTTP probes`

### Task 5.10: Wire the board into the router, and the three system tests

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `crates/weir-server/src/main.rs` (the `HttpState::new` call from Task 5.8)
- Test: `crates/weir-server/tests/system.rs` (new section `// ── HTTP health routes (Foundations, spec §10) ──` after the SIGHUP section, which starts at :3139 at 2491883 and ends before `// ── File descriptor limit exhaustion` at :3423)

**Interfaces:**
- Consumes: Task 5.8 `HttpState::new`; Task 5.9 `impl StatusSource for StatusBoard`; PR 1/PR 4 `board: Arc<StatusBoard>` in `main` (created at startup step 3, before the metrics bind at step 5); PR 4 lame duck (the runtime outlives the worker and flusher joins) and `drain_shutdown_budget_secs` (PR 4 Task 4.1); testkit's `RecordingSink::start_hanging`.
- Produces: the production wiring; system tests `health_routes_answer_on_a_serving_daemon`, `readyz_goes_503_within_200ms_of_sigterm_while_metrics_stay_up`, `public_status_and_verbose_probes_carry_no_secrets`.

- [ ] **Step 1: Write the failing system tests:**
  ```rust
  // ── HTTP health routes (Foundations, spec §10) ────────────────────────────────

  /// One raw HTTP exchange against the metrics/health listener: (status, head, body).
  fn http_exchange(port: u16, request: &str) -> (u16, String, Vec<u8>) {
      use std::io::{Read, Write};
      let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect metrics listener");
      s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
      s.write_all(request.as_bytes()).unwrap();
      let mut raw = Vec::new();
      s.read_to_end(&mut raw).expect("a full response, not a reset");
      let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a response head");
      let head = String::from_utf8_lossy(&raw[..split]).into_owned();
      let status = head.split(' ').nth(1).and_then(|c| c.parse().ok()).expect("a status code");
      (status, head, raw[split + 4..].to_vec())
  }

  fn http_get(port: u16, path: &str) -> (u16, String, String) {
      let (s, h, b) = http_exchange(port, &format!("GET {path} HTTP/1.0\r\n\r\n"));
      (s, h, String::from_utf8_lossy(&b).into_owned())
  }

  #[test]
  fn health_routes_answer_on_a_serving_daemon() {
      let srv = weir_server!("health_routes").start();
      let p = srv.metrics_port;

      let (s, h, b) = http_get(p, "/livez");
      assert_eq!((s, b.as_str()), (200, "ok\n"));
      assert!(h.contains("Cache-Control: no-store") && h.contains("Connection: close"), "{h}");

      // `serving` is stored after the listener stores (spec §6 step 12), a
      // hair after wait_ready's connect can succeed: poll briefly.
      let deadline = Instant::now() + Duration::from_secs(5);
      loop {
          let (s, _, b) = http_get(p, "/readyz");
          if s == 200 {
              assert_eq!(b, "ok\n");
              break;
          }
          assert!(Instant::now() < deadline, "/readyz never became 200: {b}");
          thread::sleep(Duration::from_millis(20));
      }
      assert_eq!(http_get(p, "/startupz").2, "ok\n");

      let (s, h, b) = http_get(p, "/status");
      assert_eq!(s, 200);
      assert!(h.contains("Content-Type: application/json; charset=utf-8"), "{h}");
      let doc: serde_json::Value = serde_json::from_str(&b).unwrap();
      assert_eq!(doc["schema"], "weir.status/v1");
      assert_eq!(doc["view"], "public");
      assert_eq!(doc["verdict"]["ready"], true);
      assert!(doc.get("pid").is_none() && doc.get("instance").is_none(), "{doc}");

      for path in ["/metrics", "/", "/foo", "/READYZ", "/readyz/"] {
          let (s, h, b) = http_get(p, path);
          assert_eq!(s, 200, "{path}");
          assert!(h.contains("Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8"), "{path}: {h}");
          assert!(!h.contains("Cache-Control"), "metrics head is byte-identical to 4.0: {h}");
          assert!(b.contains("weir_records_accepted"), "{path}");
      }

      let (s, h, b) = http_exchange(p, "HEAD /readyz HTTP/1.0\r\n\r\n");
      assert_eq!(s, 200);
      assert!(h.contains("Content-Length: 3") && b.is_empty(), "{h}");
      let (s, h, _) = http_exchange(p, "POST /status HTTP/1.0\r\nContent-Length: 0\r\n\r\n");
      assert_eq!(s, 405);
      assert!(h.contains("Allow: GET, HEAD"), "{h}");
  }

  /// §16 system test 3, the /readyz half (PR 4 owns the socket-gone and
  /// acks half): /readyz answers 503 within 200 ms of SIGTERM, and /livez and
  /// /metrics stay up until exit. A hanging sink keeps the drain inside its
  /// 10 s shutdown budget (spec §7), so the daemon is still running while the
  /// probes are asked. An idle producer connection would not hold it: under
  /// Decision Q5 it gets EOF at the lame duck.
  #[test]
  fn readyz_goes_503_within_200ms_of_sigterm_while_metrics_stay_up() {
      let hole = RecordingSink::start_hanging();
      let mut srv = weir_server!("readyz_sigterm")
          .shutdown_timeout_secs(5)
          .extra_config(
              "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
               drain_shutdown_budget_secs = 10",
          )
          .env("WEIR_SINK_URL", hole.url())
          .start();
      let p = srv.metrics_port;
      let deadline = Instant::now() + Duration::from_secs(5);
      while http_get(p, "/readyz").0 != 200 {
          assert!(Instant::now() < deadline, "never ready");
          thread::sleep(Duration::from_millis(20));
      }
      // 8 x 2 KiB durable records over 4 KiB segments: sealed segments reach
      // the drain, whose commit hangs (the same setup as PR 4's system test 3).
      let mut c = srv.client();
      for i in 0..8 {
          let mut payload = format!("rz-{i}-").into_bytes();
          payload.resize(2048, b'r');
          c.push(payload, Durability::Durable).unwrap();
      }
      drop(c);
      let deadline = Instant::now() + Duration::from_secs(15);
      while hole.connections() == 0 {
          assert!(Instant::now() < deadline, "the drain never reached the sink");
          thread::sleep(Duration::from_millis(50));
      }

      // SAFETY: kill(2) with a pid we spawned and still own (not yet reaped).
      let rc = unsafe { libc::kill(srv.child_pid() as libc::pid_t, libc::SIGTERM) };
      assert_eq!(rc, 0);
      let t = Instant::now();
      let body = loop {
          let (s, _, b) = http_get(p, "/readyz");
          if s == 503 {
              break b;
          }
          assert!(t.elapsed() < Duration::from_secs(2), "/readyz still 200 after SIGTERM");
          thread::sleep(Duration::from_millis(5));
      };
      assert!(t.elapsed() < Duration::from_millis(200), "/readyz took {:?} to fail", t.elapsed());
      assert!(body.contains("shutting_down"), "{body}");
      assert_eq!(http_get(p, "/livez").0, 200, "liveness never fails on shutdown (spec §10)");
      assert!(http_get(p, "/metrics").2.contains("weir_"), "/metrics stays up until exit (spec §7 T1)");

      // shutdown_timeout 5 s + drain budget 10 s + DRAIN_EXIT_GRACE 2 s, well inside 30 s.
      assert!(srv.wait_for_exit(Duration::from_secs(30)).is_some(), "daemon exits after SIGTERM");
  }

  /// The public surface never shows a secret or free text (spec §9.1, §16
  /// sentinel test; the board-level half is in status::http's unit tests).
  #[test]
  fn public_status_and_verbose_probes_carry_no_secrets() {
      let wab = std::env::temp_dir().join(format!("weir-SENTINELWABDIR-{}", std::process::id()));
      fs::create_dir_all(&wab).unwrap();
      let srv = weir_server!("public_status_secrets")
          .wab_dir(&wab)
          .extra_config(
              "sink_type = \"http\"\n\
               sink_max_retries = 0\n\
               health_poll_interval_secs = 1\n\
               wab_segment_max_age_secs = 1",
          )
          // Port 1 refuses at once, so the probe and the commits fail with
          // errors that name the URL; the admin side would carry them.
          .env("WEIR_SINK_URL", "http://SENTINELUSER:SENTINELPASS@127.0.0.1:1/SENTINELPATH?token=SENTINELQUERY")
          .env("WEIR_SINK_BEARER_TOKEN", "SENTINELBEARER")
          .start();
      let p = srv.metrics_port;
      srv.client().push(b"one record to fail delivery", Durability::Durable).unwrap();

      // Wait until the sink is known down, so every sink field is populated.
      let deadline = Instant::now() + Duration::from_secs(20);
      loop {
          let doc: serde_json::Value = serde_json::from_str(&http_get(p, "/status").2).unwrap();
          if doc["sink"]["health"] == "down" && !doc["sink"]["last_failure"].is_null() {
              assert_eq!(doc["sink"]["type"], "http");
              break;
          }
          assert!(Instant::now() < deadline, "sink never reported down: {doc}");
          thread::sleep(Duration::from_millis(200));
      }
      for path in ["/status", "/livez?verbose", "/readyz?verbose", "/startupz"] {
          let (_, h, b) = http_get(p, path);
          for text in [&h, &b] {
              assert!(!text.contains("SENTINEL"), "{path} leaked: {text}");
              assert!(!text.contains("127.0.0.1:1"), "{path} leaked the sink endpoint: {text}");
          }
      }
      drop(srv);
      let _ = fs::remove_dir_all(&wab);
  }
  ```
- [ ] **Step 2: Run them to verify they fail.** `cargo test -p weir-server --test system health_routes_answer_on_a_serving_daemon -- --test-threads=1`. Expected: FAIL at the first assertion: `/livez` returns the openmetrics body (200 with metrics text, not `ok\n`), because `main` still passes `status: None`. The other two fail the same way (`/readyz` never 503 / `/status` body not JSON).
- [ ] **Step 3: Implement.** In `main.rs` replace the `None` from Task 5.8:
  ```rust
  let http_state = metrics::server::HttpState::new(
      Arc::clone(&registry),
      Some(Arc::clone(&metrics)),
      Some(Arc::clone(&board) as Arc<dyn metrics::router::StatusSource>),
      config.livez_checks.clone(),
      config.metrics_max_connections,
  );
  metrics::server::spawn(metrics_listener, http_state);
  info!(
      bind = %config.metrics_bind,
      port = config.metrics_port,
      max_connections = config.metrics_max_connections,
      livez_checks = ?config.livez_checks,
      "metrics and health endpoint listening (/livez /readyz /startupz /status; any other path: metrics)"
  );
  ```
  The existing `info!` (main.rs:1469-1474 at 2491883) is replaced by this one.
- [ ] **Step 4: Run to verify they pass.** `cargo test -p weir-server --test system health_routes_answer -- --test-threads=1`, `... readyz_goes_503 ...`, `... public_status_and_verbose ...`; then the whole `cargo test -p weir-server --test system -- --test-threads=1` (the testkit's `wait_ready` and `scrape_metrics` are unaffected: they connect to the producer socket and send a complete `GET /metrics` head).
- [ ] **Step 5: Commit.** `feat(server): serve /livez /readyz /startupz /status from the status board`

### Task 5.11: Threat model, configuration reference, and the security review

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

**Files:**
- Modify: `docs/security/threat-model.md` (In scope table :70-97; Out of scope table :255-271; Threat actors :295-315)
- Modify: `docs/operations/configuration.md` (`### Metrics endpoint` :934-996: `metrics_port` and `metrics_max_connections` text, new `#### livez_checks`)
- Modify: `CHANGELOG.md` (`## [Unreleased]`: one `### Added` and one `### Changed` bullet for PR 5)

**Interfaces:**
- Consumes: the behaviour from Tasks 5.1-5.10.
- Produces: the HTTP-listener rows and actor that PR 8's docs_drift guard ("the presence of the HTTP-listener and admin-socket rows and actors in threat-model.md", §16) checks. PR 6 adds the admin-socket rows and actors and the item 4/5 reconciliations (§17); PR 8 Task 8.12 rewrites the "Stuck pipeline on shutdown" row.

- [ ] **Step 1: Write the failing check.** No code test in this PR (PR 8 owns the docs_drift guard). Run the check PR 8 will encode, now, by hand: `grep -n "Network peer reading the metrics, health and status listener" docs/security/threat-model.md && grep -n "Network peer reaching the metrics/health port" docs/security/threat-model.md && grep -n "#### \`livez_checks\`" docs/operations/configuration.md`. Expected: no output, exit 1.
- [ ] **Step 2: Implement `threat-model.md`.** New In-scope rows after "Connection flood" (:81), titled exactly as PR 8 Task 8.12's guard pins them:
  ```markdown
  | Network peer reading the metrics, health and status listener | Read-only and unauthenticated by design, bound to 127.0.0.1 by default. `/livez`, `/readyz`, `/startupz` and `/status` match their exact paths, accept only GET and HEAD, read no body and change no state; every other path is the metrics exposition. `/status` serves `PublicStatusDoc`, a separate type from the admin document with no free-text field: no panic, I/O or sink error text, probe reason, reload detail, uid, pid, absolute path, or sink URL, host or bucket. Free text cannot be serialized into it, and the `?verbose` probe lines print only a condition code and its template-rendered summary. A sentinel test, a merge gate, plants a marker in every admin-only string and checks that none reaches `/status` or the verbose probes. |
  | Network peer starving the health probes | Every connection first takes one of 64 head slots and must send a complete request head (at most 4096 bytes) within 1 s, or it is closed (`weir_http_requests_dropped_total{reason="head_timeout"}`). Only then does it take a permit from its route's own pool: scrapes from `metrics_max_connections`, the four health routes from a separate pool of 4. A scrape flood cannot starve kubelet probes, and a probe flood cannot starve Prometheus. Writes are bounded at 5 s. **Residual:** a peer on the network path that keeps all 64 head slots busy delays probes by up to 1 s per cycle; liveness `failureThreshold: 6` absorbs about a minute of it. Loopback binding or a NetworkPolicy restricting the port to kubelet and Prometheus is the boundary. |
  ```
  New Out-of-scope row after "Resource accounting per-client":
  ```markdown
  | Authentication on the metrics/health listener | None, deliberately: probes and scrapers must reach it without credentials. It is bound to 127.0.0.1 by default. Binding it to `0.0.0.0` (as in Kubernetes) exposes counters, condition codes, summaries and the public config subset to anyone who can reach the port; restrict it with a NetworkPolicy or firewall. It never exposes free text, paths, uids or sink endpoints, and it cannot change state. |
  ```
  New actor in "Threat actors considered", after "Local unprivileged user":
  ```markdown
  - **Network peer reaching the metrics/health port** (when `metrics_bind`
    is not loopback): can read the metric exposition and the public
    `/status` document — counts, ages, condition codes and summaries, shard
    and drain state, the public config numbers — but never free text,
    paths, uids, the pid or sink endpoints, because the public type has no
    field for them. Can attempt probe starvation, bounded by the 1 s head
    deadline, the 64-slot head pool and the separate probe pool. Cannot
    change any state: no route mutates, and the admin socket is a Unix
    socket on a private path, not on this port.
  ```
- [ ] **Step 3: Implement `configuration.md`.** Under `#### metrics_port`, after the first paragraph:
  ```markdown
  The same listener serves four read-only health routes: `/livez`, `/readyz`,
  `/startupz` and `/status` (exact, case-sensitive paths; GET and HEAD only).
  Every other path returns the metrics exposition exactly as before, so
  existing scrape configs keep working. **Behaviour change:** a client must send its
  complete request head within 1 s, or the connection is closed. Prometheus,
  curl and `weir-ctl` send it at once.
  ```
  Replace the `metrics_max_connections` body:
  ```markdown
  Maximum number of concurrent **scrapes** of the metrics exposition. A
  connection takes a permit only after its request head has arrived; before
  that it holds one of 64 head slots for at most 1 s, so an idle peer can no
  longer hold a scrape permit. The four health routes use their own pool of 4
  and are not limited by this setting, so a burst of scrapes cannot starve
  kubelet probes. A scrape over the cap is closed at once and counted in
  `weir_http_requests_dropped_total{reason="metrics_pool_full"}`; Prometheus
  retries on its next interval.
  ```
  New entry after `metrics_max_connections`:
  ```markdown
  #### `livez_checks`

  - **Type**: list of liveness check names
  - **Default**: `[]` (none: `/livez` answers 200 whenever the daemon's runtime answers)
  - **Values**: any subset of `listener_down`, `no_live_shard`, `worker_exited`, `drain_stopped`, `drain_wedged`
  - **CLI**: `--livez-checks <a,b>`
  - **Env**: `WEIR_LIVEZ_CHECKS` (comma-separated)
  - **TOML**: `livez_checks = ["no_live_shard", "listener_down"]`

  The conditions that make `/livez` return 503. Only states a restart fixes
  are selectable, and each is evaluated only once startup has finished, so
  `/livez` never fails during recovery, during shutdown, or because the sink
  is down, a retry is backing off, delivery is blocked or paused. A liveness
  probe that restarts weir for a down sink would only replay the backlog into
  the same down sink.

  **Recommended:** `["no_live_shard", "listener_down", "worker_exited"]`, the
  ingest-dead set. `drain_stopped` restarts a daemon whose drain supervisor
  gave up (ingest still works; nothing is delivered until a restart).
  `drain_wedged` restarts a drain blocked inside a sink call that never yields;
  enable it only after watching the `drain_wedged` condition in production for
  a while, because a restart re-delivers the in-flight segment from record 0.
  An unknown name refuses startup.
  ```
- [ ] **Step 3b: Implement `CHANGELOG.md`.** Under `## [Unreleased]`, merge into the existing headings (do not add a second `### Added` or `### Changed`). Append to `### Added`:
  ```markdown
  - `/livez`, `/readyz`, `/startupz` and `/status` on the metrics listener (exact paths, GET/HEAD; every other path still returns metrics byte for byte); the `livez_checks` key (`--livez-checks`, `WEIR_LIVEZ_CHECKS`); `weir_http_requests_dropped_total{reason}`.
  ```
  Append to `### Changed`:
  ```markdown
  - A metrics client must send its complete request head within 1 s or is closed; `metrics_max_connections` now counts concurrent scrapes after the head, and probes use their own pool of 4.
  ```
- [ ] **Step 4: Run to verify.** Rerun the three `grep`s from Step 1: each prints its line, exit 0. `grep -n "weir_http_requests_dropped_total" CHANGELOG.md` prints the new `### Added` bullet. `cargo test -p weir-server --test docs_drift` stays green (no existing guard reads these sections, and the metric row from Task 5.4 is still in place).
- [ ] **Step 5: Commit.** Stage `docs/security/threat-model.md`, `docs/operations/configuration.md` and `CHANGELOG.md`. Subject: `docs(security): the metrics/health listener in the threat model, and the livez_checks reference`
- [ ] **Step 6: Security review (merge gate; run it after Tasks 5.12 and 5.13 too).** Run the `security-review` skill on the branch diff against `main`, focused on: (a) any path by which a String from the board, the config or the sink reaches `PublicStatusDoc`, the `?verbose` lines, or a response header; (b) the head reader's bounds (4096 bytes, 1 s, 64 slots) and that no permit is taken before the head completes; (c) that no route mutates state or reads a body; (d) request-line parsing on hostile input (non-UTF-8, embedded NUL, very long single token, `\n`-only line endings). Record the findings in the PR description; fix any finding in a new commit `fix(metrics): <finding>` with its own test, then rerun the merge gate.
- [ ] **Step 7: Local CI.** `act -W .github/workflows/ci.yml -j <job>` for every job in ci.yml, container jobs included, before the first push.

---

### Task 5.12: `weir_live` follows the configured `livez_checks`

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

*(Integrator addition. PR 1's 5 s poll task computes `weir_live` with `liveness(&ev, &self.thresholds, &[])` and leaves the comment "PR 5 passes the configured livez_checks"; no PR 5 task did, so `weir_live` would stay 1 while `/livez` answers 503 for a selected check.)*

**Files:**
- Modify: `crates/weir-server/src/status/poll.rs` (PR 1's `PollStatus`: a `livez_checks` field, `with_livez_checks`, and the `liveness` call in `publish`)
- Modify: `crates/weir-server/src/main.rs` (PR 4 Task 4.10's `PollStatus::new(..)` call)
- Test: `crates/weir-server/src/status/poll.rs` (`mod tests`)

**Interfaces:**
- Consumes: PR 1's `PollStatus::{new(Arc<StatusBoard>, PathBuf, Arc<AtomicU64>), publish(&mut self, now_ms: u64, &Metrics) -> Evaluation}`, `Metrics.live: Gauge<f64, AtomicU64>`, `WorkerGuard`, `ShardRegistry::mark_up`; Task 5.3's `Config::livez_checks: Vec<weir_status::LivezCheck>`.
- Produces: `impl PollStatus { pub(crate) fn with_livez_checks(self, checks: Vec<weir_status::LivezCheck>) -> Self }` (default `Vec::new()`).

- [ ] **Step 1: Write the failing test** (append to poll.rs `mod tests`):
  ```rust
  #[test]
  fn weir_live_follows_the_configured_livez_checks() {
      use weir_status::{ListenerState, LivezCheck, Phase};
      let b = crate::status::test_board(1, 1);
      b.shards.mark_up(0);
      // Worker 0 ran and exited: `worker_exited` holds once serving.
      drop(crate::status::WorkerGuard::enter(Arc::clone(&b.shards), 0));
      b.listeners.unix.set(ListenerState::Bound);
      b.life.enter(Phase::Serving, crate::status::unix_ms_now());
      let (m, _reg) = Metrics::new();
      let now = crate::status::unix_ms_now();
      let root = crate::testutil::scratch_dir("poll_livez_checks");

      let mut none = PollStatus::new(Arc::clone(&b), root.clone(), Arc::new(AtomicU64::new(200)));
      none.publish(now, &m);
      assert_eq!(m.live.get(), 1.0, "livez_checks = [] never fails weir_live");

      let mut sel = PollStatus::new(Arc::clone(&b), root, Arc::new(AtomicU64::new(200)))
          .with_livez_checks(vec![LivezCheck::WorkerExited]);
      sel.publish(now, &m);
      assert_eq!(m.live.get(), 0.0, "a selected failing check sets weir_live = 0, as /livez answers 503");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::poll::tests::weir_live_follows -- --test-threads=1`. Expected: compile error `no method named \`with_livez_checks\` found for struct \`PollStatus\``.
- [ ] **Step 3: Implement.** `status/poll.rs`: add the field `livez_checks: Vec<weir_status::LivezCheck>` to `PollStatus`, initialised to `Vec::new()` in `PollStatus::new`, and:
  ```rust
  impl PollStatus {
      /// The configured `livez_checks`, so `weir_live` answers exactly what
      /// `/livez` answers (G2).
      pub(crate) fn with_livez_checks(mut self, checks: Vec<weir_status::LivezCheck>) -> Self {
          self.livez_checks = checks;
          self
      }
  }
  ```
  In `publish`, replace PR 1's two lines
  ```rust
          // PR 5 passes the configured livez_checks; none exist yet, so `[]`.
          let live = weir_status::liveness(&ev, &self.thresholds, &[]).ok;
  ```
  with
  ```rust
          let live = weir_status::liveness(&ev, &self.thresholds, &self.livez_checks).ok;
  ```
  `main.rs` (PR 4 Task 4.10's 5 s task): `.with_status(status::poll::PollStatus::new(Arc::clone(&board), config.wab_dir.clone(), coalesce_hint_for_poll).with_livez_checks(config.livez_checks.clone()))`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server status::poll -- --test-threads=1`. Expected: the new test and PR 1's poll tests pass. `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` is clean.
- [ ] **Step 5: Commit.** `feat(status): weir_live follows the configured livez_checks`

---

### Task 5.13: System test 10: a restart over the cap is not ready at once, and recovers with zero pushes

*PR-level merge gate: PR 5's **Merge gate** (top of the PR 5 section; includes the security review in Task 5.11).*

*(Integrator addition: spec §16 system test 10 had no owning task. PR 1 unit-tests the synchronous cap gate and the poll refresh; nothing asserted the end-to-end `/readyz` behaviour.)*

**Files:**
- Test: `crates/weir-server/tests/system.rs` (the `// ── HTTP health routes` section from Task 5.10)

**Interfaces:**
- Consumes: PR 1's `status::poll::startup_cap_gate` (via PR 4's startup step 9) and the 5 s cap refresh; Task 5.10's `/readyz`; PR 4's test helpers `fresh_wab_dir` and `RecordingSink::start_slow`; system.rs's `seed_sealed_backlog`, `pad_sealed_backlog`, `wab_dir_bytes`.
- Produces: system test `a_restart_over_the_cap_is_not_ready_at_once_and_recovers_with_zero_pushes`.

- [ ] **Step 1: Write the test** (append to the health-routes section):
  ```rust
  /// Spec §16 system test 10: a restart over `wab_max_bytes` is not ready from
  /// the first answer after startup (the synchronous cap gate), and becomes
  /// ready again without a single push once the drain brings the WAB below
  /// the low-water mark (the 5 s poll task's cap refresh).
  #[test]
  fn a_restart_over_the_cap_is_not_ready_at_once_and_recovers_with_zero_pushes() {
      let wab = fresh_wab_dir("cap_restart");
      seed_sealed_backlog("cap_restart_seed", &wab);
      pad_sealed_backlog(&wab, 60);
      let on_disk = wab_dir_bytes(&wab);
      // Legal for one shard of 4096-byte segments (minimum 4560), and well
      // under what is on disk.
      let cap = (on_disk / 2).max(8192);
      assert!(on_disk > cap, "the seeded backlog ({on_disk} B) must exceed the cap ({cap} B)");
      let sink = RecordingSink::start_slow(Duration::from_millis(200));
      let srv = weir_server!("cap_restart")
          .wab_dir(&wab)
          .shard_count(1)
          .extra_config(format!(
              "sink_type = \"http\"\nwab_segment_max_bytes = 4096\nwab_max_bytes = {cap}"
          ))
          .env("WEIR_SINK_URL", sink.url())
          .spawn();
      let port = srv.metrics_port;
      let readyz = || -> Option<(u16, String)> {
          use std::io::{Read, Write};
          let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
          s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
          s.write_all(b"GET /readyz HTTP/1.0\r\n\r\n").ok()?;
          let mut raw = String::new();
          s.read_to_string(&mut raw).ok()?;
          let (head, body) = raw.split_once("\r\n\r\n")?;
          Some((head.split(' ').nth(1)?.parse().ok()?, body.to_string()))
      };

      // The first answer past startup names the cap; it is never a 200.
      let deadline = Instant::now() + Duration::from_secs(15);
      let (status, body) = loop {
          if let Some((s, b)) = readyz() {
              if !b.contains("starting") {
                  break (s, b);
              }
          }
          assert!(Instant::now() < deadline, "the daemon never left startup");
          thread::sleep(Duration::from_millis(20));
      };
      assert_eq!(status, 503, "a restart over the cap must not be ready: {body}");
      assert!(body.contains("wab_cap_rejecting"), "{body}");

      // No client ever connects: the poll task alone clears the cap.
      let deadline = Instant::now() + Duration::from_secs(120);
      while readyz().map(|(s, _)| s) != Some(200) {
          assert!(
              Instant::now() < deadline,
              "readiness never recovered without pushes; {} B on disk, cap {cap} B",
              wab_dir_bytes(&wab)
          );
          thread::sleep(Duration::from_millis(250));
      }
      drop(srv);
      let _ = fs::remove_dir_all(&wab);
  }
  ```
- [ ] **Step 2: Verify it can fail (by mutation).** The behaviour already exists (PR 1's cap gate, Task 5.10's route), so this is a forward guard. Two mutations together, because the 5 s poll task (spawned at PR 4's startup step 10, before the producer listeners serve) scans at once on its first `interval.tick()` and `WabPoll::tick` runs `wab::cap::evaluate` after every successful scan, so with only the gate removed the cap flag is still set before `serving`. In `main.rs`: (1) comment out the `status::poll::startup_cap_gate(..)` call; (2) in the 5 s task, add one `interval.tick().await;` before `loop {`, so the first scan runs 5 s after spawn instead of at once. Run `cargo test -p weir-server --test system a_restart_over_the_cap -- --test-threads=1`. Expected: FAIL with `a restart over the cap must not be ready` (the first answer is 200). Restore both lines.
- [ ] **Step 3: Implement.** No production change.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --test system a_restart_over_the_cap -- --test-threads=1`. Expected: PASS in about 15-30 s.
- [ ] **Step 5: Commit.** `test(system): a restart over the cap is not ready at once and recovers with zero pushes`
