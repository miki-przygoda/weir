# Operability Foundations — PR 7: weir-ctl status, reload and drain over the admin socket, with offline status from disk and /metrics; metrics and segments fixes

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 7: weir-ctl status, reload and drain over the admin socket, with offline status from disk and /metrics; metrics and segments fixes

**Scope (spec §16 PR table, row 7; spec §14).** `weir-ctl status` (identity checks; sources tried in order: admin socket, then `GET /status`, then `/metrics` plus disk, then disk only); `weir-ctl reload`; `weir-ctl drain pause|resume|retry-stranded`; read-only disk evidence (`offline.rs`); the exit-code table shared with the readiness script; `metrics` summary keys and warning order; `segments` skips `quarantine/` and gains `--files`. Size M.

**Depends on.** PR 5 (`/status` route, `PublicStatusDoc`, `/readyz`) and PR 6 (admin socket, derived default path, verbs, `instance` on every reply). Both come after PR 4, and PR 7 comes after both (spec §16 "PR 7 after 5 and 6"). It also uses the `weir-status` crate from PR 1 (types, `evaluate`, `readiness`, `exposition::evidence_from_metrics`).

**Behaviour change.** Additive CLI only. New subcommands: `status`, `reload`, `drain`. New flag: `segments --files`. `metrics --json` gains three keys (`drain_state`, `offline_shards`, `ready`) and keeps every existing key. `health`, `push`, `dl *`, `quarantine *` and `attest *` do not change (writ depends on the `attest --json` contract). weir-ctl still never takes, probes or releases `.weir.lock`.

**Merge gate.**
- `cargo test -p weir-status` passes, including the exit-code parity table.
- `cargo test -p weir-ctl` passes, including the identity checks: a wrong uid via the predicate exits 2, a (dev, ino) mismatch exits 2, and the derived default path matches. The existing `--json` key tests must pass unchanged.
- `cargo test -p weir-server --test system weir_ctl_ -- --test-threads=1` passes. This covers system test 9 from spec §16: admin, http and offline sources plus the parity check against `/readyz`.
- Run CI locally first with `act -W .github/workflows/ci.yml -j test` and `-j lint`.
- A security review of the client-side identity checks (Task 7.9) is done before merge.

Lint (clippy -D warnings, including dead_code in the weir-ctl bin target) is first expected green after Task 7.12, which gives offline.rs, admin_client.rs and status.rs their non-test callers. Run `act -j lint` from Task 7.12 on, not per task before it.

**Reconciliation with the code as it is today** (the code describes current behaviour; the spec describes the target):
- `scan_segments` (weir-ctl main.rs:482-546) excludes only `dead_letter/`. `quarantine/` is counted as a shard. Spec §14 calls this "the miscount at weir-ctl main.rs:506". Task 7.5 fixes it.
- `scrape` (main.rs:2189-2207) sends exactly `GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n`. Task 7.6 keeps those bytes and adds only a connect timeout and a size bound.
- The safety floor's `ReloadReport` (reload.rs:99-109) has `outcome: Ok|Noop|Failed` plus `changed` / `rejected` / `note`. Spec §5.10 and §9.1 name the outcomes `applied|noop|failed` with `items` / `not_reloaded`. PR 6 owns the wire rendering, so weir-ctl accepts both spellings of success (`"ok"` and `"applied"`) and renders whichever item lists are present (Task 7.13).
- The `weir_ctl_never_takes_the_wab_dir_lock` test (main.rs:3277-3304) scans a fixed list of two files. This PR adds four modules, so Task 7.3 changes the test to walk `src/`. Re-checked against the merged safety floor (2491883): the test still lists main.rs and attest.rs with the same six needles.

**Consumed from earlier PRs.** *(Integrator check: every name in this table was checked against the PR 1, PR 5 and PR 6 sections of this plan; the renames applied are `default_socket_path`, PR 1's `NoLatches` and `EXIT_*` / `exit_code`, and PR 6's `resumed` key.)* PR 7 confines the use of these items to `crates/weir-ctl/src/status.rs` (the `eval_*` functions) and `crates/weir-ctl/src/admin_client.rs` (`resolve_admin_target`), so a rename is a one-file fix.

| Item | From | Used in |
|---|---|---|
| `weir_status::SCHEMA: &str` (= `"weir.status/v1"`) | PR 1 | 7.10, 7.11 |
| `weir_status::{Condition, ConditionCode, Severity, Affects, EvidenceValue}`. `Condition` fields: `code, severity, affects, since_unix_ms: Option<u64>, summary: String, detail: Option<String>, evidence: BTreeMap<String, EvidenceValue>, runbook: Cow<'static, str>, gates: Vec<Gate>`. `ConditionCode::{DaemonUnreachable, RecoveryPending}`, both serde `snake_case`. `Severity::{Info, Warning, Critical}` with `PartialEq`. `Affects::{Ingest, Delivery, Durability}`. `EvidenceValue::U64(u64)` | PR 1 | 7.1, 7.2 |
| `weir_status::{Evidence, Source, BacklogEvidence, Thresholds, SinceLookup, NoLatches, Evaluation, ProbeVerdict}` (`NoLatches` is PR 1's unit `SinceLookup`, used for offline evaluation), and PR 1's exit-code table `weir_status::{EXIT_READY, EXIT_NOT_READY, EXIT_UNREACHABLE, EXIT_CRITICAL, exit_code(&ProbeVerdict, &Evaluation) -> i32}`. `Evidence` has pub `Option` fields and `Evidence::empty(now_unix_ms: u64, source: Source) -> Evidence`. `Source::Disk` exists. `BacklogEvidence { undelivered_segments: u64, undelivered_records: u64, oldest_undelivered_age_secs: Option<f64>, dead_letter_bytes: u64, dead_letter_max_bytes: Option<u64>, quarantine_bytes: u64, quarantine_files: u64, scanned_at_ms: u64 }`. `Thresholds: Default`. `trait SinceLookup { fn since(&self, code: ConditionCode) -> Option<u64>; }`. `Evaluation { pub conditions: Vec<Condition>, pub unevaluated: Vec<ConditionCode> }`. `ProbeVerdict { pub ok: bool, pub failing: Vec<ConditionCode>, .. }` | PR 1 | 7.1, 7.2, 7.10 |
| `weir_status::evaluate(&Evidence, &Thresholds, &dyn SinceLookup) -> Evaluation`, `weir_status::readiness(&Evidence) -> ProbeVerdict`, `weir_status::exposition::evidence_from_metrics(body: &str, now_unix_ms: u64) -> Evidence`, `weir_status::active_conditions_from_metrics(body: &str) -> Option<Vec<(ConditionCode, Severity)>>` (PR 1 Task 1.9) | PR 1 | 7.1, 7.10 |
| `weir_status::admin::default_socket_path(wab_dir: &Path) -> Option<PathBuf>`, which returns `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` (the derivation the daemon uses, spec §5.9). It lives in `weir-status` (PR 6 Task 6.1), so weir-ctl derives the same path | PR 6 | 7.8 |
| Admin wire (spec §11). Request `{"v":1,"id":…,"verb":…,"args":{…}}`. Reply `{"v":1,"id":…,"instance":{"pid","started_at_ms","wab_dir","socket_path","wab_dir_dev","wab_dir_ino"},"ok":bool,"result"|"error":{"code","message","retryable"}}`. Verbs: `status`, `reload`, `drain.pause`, `drain.resume`, `drain.retry_stranded` | PR 6 | 7.9, 7.12-7.14 |
| The "in effect" signal of each `drain.*` result, as PR 6 Task 6.1 defines the result types: `drain.pause` → `paused` (head) or `held` (mid-segment); `drain.resume` → the additive `resumed: bool` (the drain was observed no longer paused or held); `drain.retry_stranded` → `completed`. weir-ctl also honours a generic `"observed": bool` if a later daemon adds one, but PR 6 does not, and none is needed | PR 6 | 7.14 |
| Admin `status` result = `AdminStatusDoc` JSON (`schema`, `view:"admin"`, `verdict{ready,…}`, `conditions[]{code,severity,…}`, `instance{…}`) and `/status` = `PublicStatusDoc` JSON (`view:"public"`, no `instance`), content type `application/json; charset=utf-8` | PR 5, PR 6 | 7.11, 7.16 |
| Daemon creates `<parent(wab_dir)>/<basename>.admin.sock` by default under the test harness (tmp_dir is 0755, so the parent is not group- or world-writable) | PR 6 | 7.16 |
| `weir_wab::format::{build_segment_header, build_segment_footer, build_confirmed, parse_segment_header, parse_segment_footer, parse_confirmed, confirmed_path_for, Compression, SEGMENT_HEADER_LEN, SEGMENT_FOOTER_LEN, CONFIRMED_LEN}`, `weir_wab::{list_segment_files, SegmentState}` | existing (weir-wab 4.0.1) | 7.7 |

**New dependencies (weir-ctl).**
- `weir-status = { workspace = true }`. This is the shared evaluator the owner approved (spec §5.1, §20). It is added in Task 7.8.
- `libc = "0.2"` under `[target.'cfg(unix)'.dependencies]`, also added in Task 7.8. weir-ctl needs it for `SO_PEERCRED` / `getpeereid` and `geteuid` in the client-side identity check (spec §11). At MSRV 1.88, `std::os::unix::net::UnixStream::peer_cred` is still unstable (`peer_credentials_unix_socket`). libc is already in the lock tree through weir-server, so this adds no new crate to the build.

---

### Task 7.1: weir-status: the exit-code table shared by `weir-ctl status` and the readiness script

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

**Files:**
- Create: `crates/weir-status/src/exit.rs`
- Modify: `crates/weir-status/src/lib.rs` (add `pub mod exit;` next to the other `pub mod` lines)
- Create: `crates/weir-status/tests/exit_parity.rs`

**Interfaces:**
- Consumes: `weir_status::{Condition, Severity, Thresholds, NoLatches, evaluate, readiness}`, PR 1's `weir_status::{EXIT_READY, EXIT_NOT_READY, EXIT_UNREACHABLE, EXIT_CRITICAL, exit_code}` (PR 1 Task 1.7), `weir_status::exposition::evidence_from_metrics`.

  *(Integrator reconciliation: PR 1 Task 1.7 already defines the shared exit codes and `exit_code(&ProbeVerdict, &Evaluation)`. `StatusExit` stays as weir-ctl's typed view of that one table: `code()` returns PR 1's constants, and the parity test below asserts `StatusExit::from_verdict` and `exit_code` agree on every fixture, so there is one table, not two.)*
- Produces:
  - `pub enum weir_status::exit::StatusExit { Ready, NotReady, Unreachable, ReadyWithCritical }` (derives `Debug, Clone, Copy, PartialEq, Eq`)
  - `pub const fn StatusExit::code(self) -> u8` (0 / 1 / 2 / 3)
  - `pub const fn StatusExit::from_parts(ready: bool, any_critical: bool) -> StatusExit`
  - `pub fn StatusExit::from_verdict(ready: bool, conditions: &[Condition]) -> StatusExit`

- [ ] **Step 1: Write the failing test**

Add `pub mod exit;` to `crates/weir-status/src/lib.rs` now, so the new file is compiled. Then create `crates/weir-status/src/exit.rs` holding only this test module; the step fails to compile until Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_the_documented_table() {
        // spec §14 / §12: one table, shared with deploy/systemd/weir-readiness.sh.
        assert_eq!(StatusExit::Ready.code(), 0);
        assert_eq!(StatusExit::NotReady.code(), 1);
        assert_eq!(StatusExit::Unreachable.code(), 2);
        assert_eq!(StatusExit::ReadyWithCritical.code(), 3);
    }

    #[test]
    fn from_parts_is_ready_first_then_critical() {
        assert_eq!(StatusExit::from_parts(true, false), StatusExit::Ready);
        assert_eq!(StatusExit::from_parts(true, true), StatusExit::ReadyWithCritical);
        // Not ready wins over critical: exit 1 exactly when /readyz is 503.
        assert_eq!(StatusExit::from_parts(false, false), StatusExit::NotReady);
        assert_eq!(StatusExit::from_parts(false, true), StatusExit::NotReady);
    }

    #[test]
    fn exit_in_0_or_3_iff_ready() {
        for ready in [false, true] {
            for crit in [false, true] {
                let code = StatusExit::from_parts(ready, crit).code();
                assert_eq!(code == 0 || code == 3, ready, "ready={ready} critical={crit}");
                assert_ne!(code, 2, "2 is reserved for unreachable / identity mismatch");
            }
        }
    }
}
```

`crates/weir-status/tests/exit_parity.rs`:

```rust
//! spec §14 parity: for identical Evidence, exit ∈ {0,3} ⇔ readiness().ok
//! (⇔ /readyz 200, since /readyz renders the same `readiness`).

use std::path::Path;
use weir_status::exit::StatusExit;
use weir_status::exposition::evidence_from_metrics;
use weir_status::{NoLatches, Thresholds, evaluate, exit_code, readiness};

/// A 4.1 daemon serving normally (metric names from spec §13). PR 1's
/// `evidence_from_metrics` builds `wab_cap` only when `weir_wab_cap_rejecting`,
/// `weir_wab_bytes_on_disk` and `weir_config_wab_max_bytes` are all present;
/// without all three, `wab_cap_rejecting` is unevaluated and readiness fails
/// closed, so this fixture would never be ready.
const SERVING_4_1: &str = "\
weir_build_info{version=\"4.1.0\",wire_version=\"1\"} 1
weir_startup_phase{phase=\"initializing\"} 0
weir_startup_phase{phase=\"recovering\"} 0
weir_startup_phase{phase=\"handoff\"} 0
weir_startup_phase{phase=\"binding\"} 0
weir_startup_phase{phase=\"serving\"} 1
weir_startup_phase{phase=\"shutting_down\"} 0
weir_ready 1
weir_live 1
weir_wab_shard_up{shard=\"0\"} 1
weir_worker_up{worker=\"0\"} 1
weir_wab_cap_rejecting 0
weir_wab_bytes_on_disk 0
weir_config_wab_max_bytes 1073741824
weir_drain_state{state=\"draining\"} 1
weir_sink_health{state=\"healthy\"} 1
weir_sink_info{sink_type=\"http\"} 1
";

fn fixtures() -> Vec<(String, String)> {
    let mut out = vec![
        ("inline:serving_4_1".to_string(), SERVING_4_1.to_string()),
        (
            "inline:cap_rejecting".to_string(),
            SERVING_4_1.replace("weir_wab_cap_rejecting 0", "weir_wab_cap_rejecting 1"),
        ),
        (
            "inline:shutting_down".to_string(),
            SERVING_4_1
                .replace("weir_startup_phase{phase=\"serving\"} 1", "weir_startup_phase{phase=\"serving\"} 0")
                .replace(
                    "weir_startup_phase{phase=\"shutting_down\"} 0",
                    "weir_startup_phase{phase=\"shutting_down\"} 1",
                ),
        ),
        (
            "inline:no_live_shard".to_string(),
            SERVING_4_1.replace("weir_wab_shard_up{shard=\"0\"} 1", "weir_wab_shard_up{shard=\"0\"} 0"),
        ),
    ];
    // Plus every exposition PR 1 captured for `evidence_from_metrics`
    // (PR 1 Task 1.9 names them `metrics_*.txt`, e.g. metrics_4_0_1.txt).
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if (name.starts_with("metrics_") || name.starts_with("exposition")) && name.ends_with(".txt") {
                out.push((name, std::fs::read_to_string(e.path()).unwrap()));
            }
        }
    }
    out
}

#[test]
fn exit_in_0_or_3_iff_readiness_ok_for_every_fixture() {
    let (mut seen_ok, mut seen_not_ok) = (false, false);
    for (name, body) in fixtures() {
        let e = evidence_from_metrics(&body, 1_700_000_000_000);
        let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
        let r = readiness(&e);
        let exit = StatusExit::from_verdict(r.ok, &ev.conditions);
        assert_eq!(
            matches!(exit, StatusExit::Ready | StatusExit::ReadyWithCritical),
            r.ok,
            "{name}: exit {exit:?} disagrees with readiness().ok = {}",
            r.ok
        );
        assert_eq!(
            i32::from(exit.code()),
            exit_code(&r, &ev),
            "{name}: weir-ctl's StatusExit and PR 1's exit_code must be one table"
        );
        seen_ok |= r.ok;
        seen_not_ok |= !r.ok;
    }
    assert!(seen_ok, "a healthy 4.1 exposition must evaluate ready, or weir-ctl's metrics source never says READY");
    assert!(seen_not_ok, "the cap-rejecting fixture must evaluate not ready");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-status exit`
Expected: compile error `failed to resolve: use of undeclared type StatusExit` (in `src/exit.rs`) and `unresolved import weir_status::exit` (in `tests/exit_parity.rs`).

- [ ] **Step 3: Implement**

Put this above the test module in `crates/weir-status/src/exit.rs`:

```rust
//! The one exit-code table for `weir-ctl status`, shared with
//! `deploy/systemd/weir-readiness.sh` (spec §12, §14). The script maps these
//! codes 1:1; the parity rule is `code ∈ {0, 3} ⇔ readiness().ok ⇔ /readyz 200`.

use crate::{Condition, Severity};

/// How `weir-ctl status` exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusExit {
    /// 0: ready, and no critical condition is active.
    Ready,
    /// 1: not ready: exactly when `/readyz` would answer 503.
    NotReady,
    /// 2: no daemon answered and there are no offline facts, or the admin
    /// socket failed a client-side identity check (spec §14, README Q9).
    Unreachable,
    /// 3: ready, but a critical condition that does not gate readiness is
    /// active (any `affects`), e.g. `shard_offline` or `drain_stopped`.
    ReadyWithCritical,
}

impl StatusExit {
    /// The process exit status: PR 1's shared constants, so the readiness
    /// script, `weir-ctl status` and `exit_code` cannot disagree.
    pub const fn code(self) -> u8 {
        match self {
            StatusExit::Ready => crate::EXIT_READY as u8,
            StatusExit::NotReady => crate::EXIT_NOT_READY as u8,
            StatusExit::Unreachable => crate::EXIT_UNREACHABLE as u8,
            StatusExit::ReadyWithCritical => crate::EXIT_CRITICAL as u8,
        }
    }

    /// The table itself. Not-ready wins over critical, so exit 1 is exactly
    /// `ready == false`.
    pub const fn from_parts(ready: bool, any_critical: bool) -> StatusExit {
        match (ready, any_critical) {
            (false, _) => StatusExit::NotReady,
            (true, true) => StatusExit::ReadyWithCritical,
            (true, false) => StatusExit::Ready,
        }
    }

    /// The table over an evaluated condition list (`ready` is
    /// `readiness(&evidence).ok`, or a daemon document's `verdict.ready`).
    pub fn from_verdict(ready: bool, conditions: &[Condition]) -> StatusExit {
        StatusExit::from_parts(
            ready,
            conditions.iter().any(|c| c.severity == Severity::Critical),
        )
    }
}
```

(`pub mod exit;` is already in `lib.rs` from Step 1.)

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-status exit`
Expected: `exit_codes_are_the_documented_table`, `from_parts_is_ready_first_then_critical`, `exit_in_0_or_3_iff_ready` and `exit_in_0_or_3_iff_readiness_ok_for_every_fixture` all pass.

- [ ] **Step 5: Commit**

`git add crates/weir-status/src/exit.rs crates/weir-status/src/lib.rs crates/weir-status/tests/exit_parity.rs && git commit -m "feat(status): one exit-code table for weir-ctl status and the readiness script"`

---

### Task 7.2: weir-status: the offline-only conditions `daemon_unreachable` and `recovery_pending`

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

**Files:**
- Create: `crates/weir-status/src/offline.rs`
- Modify: `crates/weir-status/src/lib.rs` (add `pub mod offline;`)

**Interfaces:**
- Consumes: `weir_status::{Condition, ConditionCode::{DaemonUnreachable, RecoveryPending}, Severity, Affects, EvidenceValue, SinceLookup}`.
- Produces:
  - `pub const weir_status::offline::RUNBOOK_DAEMON_UNREACHABLE: &str = "docs/monitoring.md#cond-daemon_unreachable"`
  - `pub const weir_status::offline::RUNBOOK_RECOVERY_PENDING: &str = "docs/monitoring.md#cond-recovery_pending"`
  - `pub fn weir_status::offline::daemon_unreachable(now_unix_ms: u64) -> Condition`
  - `pub fn weir_status::offline::recovery_pending(undelivered_segments: u64, active_segments: u64) -> Option<Condition>`
  - *(Integrator reconciliation: the draft also defined `weir_status::offline::NoLatches`; PR 1 Task 1.3 already ships `weir_status::NoLatches` (`impl SinceLookup`), so this task re-exports it (`pub use crate::NoLatches;`) for its callers instead of defining a second one. And `recovery_pending`'s `affects` is PR 1's catalogue value, `durability` (PR 1's `OFFLINE_ONLY` entry), not `delivery`; a test pins both offline conditions to the catalogue.)*

- [ ] **Step 1: Write the failing test**

Add `pub mod offline;` to `crates/weir-status/src/lib.rs` now, so the new file is compiled. Then create `crates/weir-status/src/offline.rs` holding only these tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_unreachable_is_critical_ingest_with_its_anchor() {
        let c = daemon_unreachable(1_700_000_000_000);
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["code"], "daemon_unreachable");
        assert_eq!(v["severity"], "critical");
        assert_eq!(v["affects"], "ingest");
        assert_eq!(v["runbook"], RUNBOOK_DAEMON_UNREACHABLE);
        assert!(c.detail.is_none(), "offline conditions carry no free text");
        assert!(c.gates.is_empty(), "no /readyz exists to gate when the daemon is down");
    }

    #[test]
    fn recovery_pending_only_when_disk_holds_work_for_recovery() {
        assert!(recovery_pending(0, 0).is_none());
        let c = recovery_pending(3, 1).expect("3 sealed + 1 active segment is pending recovery");
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["code"], "recovery_pending");
        assert_eq!(v["severity"], "info");
        assert_eq!(v["runbook"], RUNBOOK_RECOVERY_PENDING);
        assert!(c.summary.contains('3') && c.summary.contains('1'), "{}", c.summary);
    }

    #[test]
    fn runbook_anchors_follow_the_cond_code_convention() {
        for (code, anchor) in [
            ("daemon_unreachable", RUNBOOK_DAEMON_UNREACHABLE),
            ("recovery_pending", RUNBOOK_RECOVERY_PENDING),
        ] {
            assert_eq!(anchor, format!("docs/monitoring.md#cond-{code}"));
        }
    }

    #[test]
    fn no_latches_reports_no_first_seen_time() {
        assert_eq!(NoLatches.since(ConditionCode::DaemonUnreachable), None);
    }

    /// Severity, `affects`, gates and runbook come from PR 1's `OFFLINE_ONLY`
    /// catalogue entries, which the monitoring.md table (PR 8) is checked against.
    #[test]
    fn offline_conditions_match_the_catalogue() {
        let built = [daemon_unreachable(1), recovery_pending(1, 1).unwrap()];
        for c in built {
            let e = crate::entry(c.code).expect("catalogued");
            assert_eq!((c.severity, c.affects), (e.severity, e.affects), "{:?}", c.code);
            assert_eq!(c.gates, e.gates.to_vec(), "{:?}", c.code);
            assert_eq!(c.runbook, e.runbook, "{:?}", c.code);
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-status offline`
Expected: compile error `cannot find function daemon_unreachable in this scope` (and `RUNBOOK_*`, `recovery_pending`).

- [ ] **Step 3: Implement**

Put this above the tests in `crates/weir-status/src/offline.rs`:

```rust
//! Conditions only `weir-ctl status` emits, when no daemon answered and the
//! facts come from disk (spec §9.3, "Emitted only by weir-ctl offline").
//! Summaries take numbers only: no `&str` parameter, so no path or error text
//! can reach them (spec §9.1 redaction rules).

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::{Affects, Condition, ConditionCode, EvidenceValue, Severity};
#[cfg(test)]
use crate::SinceLookup;

/// PR 1's unit `SinceLookup`: offline evaluation has no condition latches.
pub use crate::NoLatches;

pub const RUNBOOK_DAEMON_UNREACHABLE: &str = "docs/monitoring.md#cond-daemon_unreachable";
pub const RUNBOOK_RECOVERY_PENDING: &str = "docs/monitoring.md#cond-recovery_pending";

/// No daemon answered on the admin socket, `/status` or `/metrics`.
pub fn daemon_unreachable(now_unix_ms: u64) -> Condition {
    Condition {
        code: ConditionCode::DaemonUnreachable,
        severity: Severity::Critical,
        affects: Affects::Ingest,
        since_unix_ms: None,
        summary: "no daemon answered on the admin socket, /status or /metrics; the facts shown \
                  are read from the wab_dir on disk"
            .to_string(),
        detail: None,
        evidence: BTreeMap::from([(
            "checked_at_ms".to_string(),
            EvidenceValue::U64(now_unix_ms),
        )]),
        runbook: Cow::Borrowed(RUNBOOK_DAEMON_UNREACHABLE),
        gates: Vec::new(),
    }
}

/// The wab_dir holds sealed-but-unconfirmed or active segments that the next
/// start's recovery will seal and replay. `None` when there is nothing.
pub fn recovery_pending(undelivered_segments: u64, active_segments: u64) -> Option<Condition> {
    if undelivered_segments == 0 && active_segments == 0 {
        return None;
    }
    Some(Condition {
        code: ConditionCode::RecoveryPending,
        severity: Severity::Info,
        affects: Affects::Durability, // PR 1's OFFLINE_ONLY catalogue entry
        since_unix_ms: None,
        summary: format!(
            "{undelivered_segments} sealed segment(s) await delivery and {active_segments} \
             active segment(s) will be sealed by recovery at the next start"
        ),
        detail: None,
        evidence: BTreeMap::from([
            (
                "undelivered_segments".to_string(),
                EvidenceValue::U64(undelivered_segments),
            ),
            ("active_segments".to_string(), EvidenceValue::U64(active_segments)),
        ]),
        runbook: Cow::Borrowed(RUNBOOK_RECOVERY_PENDING),
        gates: Vec::new(),
    })
}
```

The `#### cond-daemon_unreachable` and `#### cond-recovery_pending` headings in docs/monitoring.md are PR 8's job (spec §9.3 "Contract"). The runbook strings here are the anchors PR 8's docs_drift guard resolves.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-status offline`
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

`git add crates/weir-status/src/offline.rs crates/weir-status/src/lib.rs && git commit -m "feat(status): offline-only conditions daemon_unreachable and recovery_pending"`

---

### Task 7.3: weir-ctl: the never-lock guard scans every source file

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

The guard (main.rs:3277-3304) lists `main.rs` and `attest.rs` by name. This PR adds `http.rs`, `offline.rs`, `admin_client.rs`, `status.rs` and `admin_cmds.rs`, and a fixed list would silently leave them unguarded. Re-checked against 2491883: the merged test has exactly these six needles; only the walk changes.

**Files:**
- Modify: `crates/weir-ctl/src/main.rs`, test `weir_ctl_never_takes_the_wab_dir_lock` (currently lines 3277-3304)

**Interfaces:**
- Consumes: nothing new.
- Produces: no runtime item. The test now covers every `*.rs` file under `crates/weir-ctl/src/`.

- [ ] **Step 1: Write the failing test**

Replace the body of `weir_ctl_never_takes_the_wab_dir_lock` with a walk of `src/`, and add a sentinel assertion that the walk found more than the two files the old list named. The sentinel is what makes this step fail today, because only `main.rs` and `attest.rs` exist:

```rust
    #[test]
    fn weir_ctl_never_takes_the_wab_dir_lock() {
        // Every source file, not a fixed list: a new module must not slip past.
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    files.push(p);
                }
            }
        }
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        for required in ["main.rs", "attest.rs", "offline.rs", "admin_client.rs", "status.rs"] {
            assert!(
                names.iter().any(|n| n == required),
                "the lock guard must see {required}; it saw {names:?}"
            );
        }
        // Split so this test's own source does not match itself.
        let needles = [
            ["fl", "ock("].concat(),
            ["lo", "ckf("].concat(),
            ["F_SET", "LK"].concat(),
            ["try_", "lock_shared"].concat(),
            [".try_", "lock("].concat(),
            [".lock_", "shared("].concat(),
        ];
        for path in &files {
            let text = std::fs::read_to_string(path).unwrap();
            for needle in &needles {
                assert!(
                    !text.contains(needle.as_str()),
                    "weir-ctl {} contains {needle:?}: weir-ctl must never lock the wab_dir",
                    path.display()
                );
            }
        }
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl weir_ctl_never_takes_the_wab_dir_lock`
Expected: FAIL with `the lock guard must see offline.rs; it saw ["main.rs", "attest.rs"]` (order may vary).

- [ ] **Step 3: Implement**

Create the module files as empty shells so the walk sees them. Later tasks fill them. Each file is a one-line module doc:

- `crates/weir-ctl/src/offline.rs`: `//! Read-only wab_dir facts for \`weir-ctl status\` (filled in by Task 7.7).`
- `crates/weir-ctl/src/admin_client.rs`: `//! Admin-socket client with client-side identity checks (Task 7.8 / 7.9).`
- `crates/weir-ctl/src/status.rs`: `//! \`weir-ctl status\` (Tasks 7.10-7.12).`

Declare them in `main.rs` after `mod attest;`:

```rust
mod admin_client;
mod attest;
mod offline;
mod status;
```

Then verify by mutation, as the repo requires for guards. Add a line `// fl` + `ock(` (written joined) to `offline.rs`, run the test, and see it fail with `weir-ctl …/offline.rs contains "flock("`. Remove the line.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl weir_ctl_never_takes_the_wab_dir_lock`
Expected: PASS.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "test(ctl): the never-lock guard walks every weir-ctl source file"`

---

### Task 7.4: `weir-ctl metrics`: stopped/blocked drain first, offline shards, three additive summary keys

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 "Changed (SP-D)". `metrics` gains summary keys only. Its warnings list STOPPED and BLOCKED first, add "sink_health is stale while the drain is stopped", and add an offline-shard list taken from `weir_wab_shard_up`. The existing panic warning ("a shard is offline until restart") is the lifetime-counter misdiagnosis from spec §1. Against a 4.1 daemon (`weir_wab_shard_up` present) it becomes an info line. Against an older daemon (family absent) it stays exactly as it is, which keeps the existing test `summary_warnings_flag_the_durability_hazards` green.

**Files:**
- Modify: `crates/weir-ctl/src/main.rs`:
  - `MetricsSummary` (2227-2239): add `drain_state`, `offline_shards`, `ready`
  - `parse_summary` (2243-2268)
  - `summary_json` (2272-2287)
  - `summary_warnings` (2294-2322)
  - `print_summary` (2324-2353)
  - new `labelled_samples` and `parse_label_set` next to `active_label` (2362-2382)
- Test: `crates/weir-ctl/src/main.rs` `mod tests`. Update the `summary_with` helper (3167-3186) with the three new fields; this is a test-helper struct literal, not an assertion change.

**Interfaces:**
- Consumes: existing `active_label`, `get_metric`, `plural`.
- Produces:
  - `fn labelled_samples(body: &str, metric: &str) -> Vec<(std::collections::BTreeMap<String, String>, f64)>`
  - `fn parse_label_set(src: &str) -> std::collections::BTreeMap<String, String>`
  - New `MetricsSummary` fields: `drain_state: Option<String>`, `offline_shards: Option<Vec<u16>>`, `ready: Option<bool>`
  - New `summary_json` keys: `"drain_state"`, `"offline_shards"`, `"ready"` (each `null` when the family is absent)

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in main.rs:

```rust
    #[test]
    fn labelled_samples_parse_label_sets_and_match_the_family_exactly() {
        let body = "\
weir_wab_shard_up{shard=\"0\"} 1
weir_wab_shard_up{shard=\"1\"} 0
weir_wab_shard_up_extra{shard=\"9\"} 0
weir_condition_active{code=\"drain_paused\",severity=\"warning\"} 1
weir_condition_active{severity=\"critical\",code=\"shard_offline\"} 0 1700000000000
";
        let up = labelled_samples(body, "weir_wab_shard_up");
        assert_eq!(up.len(), 2, "a longer family name must not match: {up:?}");
        assert_eq!(up[1].0["shard"], "1");
        assert_eq!(up[1].1, 0.0);
        let cond = labelled_samples(body, "weir_condition_active");
        assert_eq!(cond[0].0["code"], "drain_paused");
        assert_eq!(cond[0].0["severity"], "warning");
        // Label order is free and a trailing timestamp is ignored.
        assert_eq!(cond[1].0["code"], "shard_offline");
        assert_eq!(cond[1].1, 0.0);
        assert_eq!(
            parse_label_set(r#"a="x\"y",b="z""#).get("a").map(String::as_str),
            Some("x\"y")
        );
    }

    #[test]
    fn metrics_summary_json_gains_drain_state_offline_shards_and_ready() {
        let body = "\
weir_records_accepted_total{tier=\"durable\"} 5
weir_drain_state{state=\"draining\"} 0
weir_drain_state{state=\"stopped\"} 1
weir_wab_shard_up{shard=\"0\"} 1
weir_wab_shard_up{shard=\"2\"} 0
weir_wab_shard_up{shard=\"1\"} 0
weir_ready 1
";
        let v = round_trip(&summary_json(body));
        assert_eq!(v["drain_state"], serde_json::json!("stopped"));
        assert_eq!(v["offline_shards"], serde_json::json!([1, 2]), "sorted ids of shards at 0");
        assert_eq!(v["ready"], serde_json::json!(true));
        // A pre-4.1 exposition: the new keys exist and are null, not absent.
        let old = round_trip(&summary_json("weir_records_accepted_total{tier=\"durable\"} 5\n"));
        for key in ["drain_state", "offline_shards", "ready"] {
            assert!(old.get(key).is_some_and(|x| x.is_null()), "{key} must be null on a 4.0 daemon");
        }
        // Every pre-existing key is still there.
        for key in [
            "accepted", "ack", "nack", "fsync_avg_ms", "queue_depth", "wab_bytes_on_disk",
            "dead_letter_bytes_on_disk", "sink_type", "sink_health", "flusher_panics",
            "fsync_failures",
        ] {
            assert!(old.get(key).is_some(), "existing key {key} must not move");
        }
    }

    #[test]
    fn summary_warnings_put_a_stopped_or_blocked_drain_first() {
        let mut s = summary_with("noop", 5, 0, 1);
        s.drain_state = Some("stopped".into());
        let w = summary_warnings(&s);
        assert!(w[0].contains("STOPPED"), "{w:?}");
        assert!(w[1].contains("sink_health is stale while the drain is stopped"), "{w:?}");
        assert!(w.iter().any(|l| l.contains("DISCARDED")), "the noop warning still follows: {w:?}");

        let mut b = summary_with("http", 0, 0, 0);
        b.drain_state = Some("blocked_dead_letter_full".into());
        let w = summary_warnings(&b);
        assert!(w[0].contains("BLOCKED"), "{w:?}");
        assert!(!w.iter().any(|l| l.contains("stale")), "blocked is not stopped: {w:?}");
    }

    #[test]
    fn summary_warnings_list_offline_shards_and_soften_recovered_panics() {
        let mut s = summary_with("http", 0, 2, 0);
        s.offline_shards = Some(vec![1, 3]);
        let w = summary_warnings(&s);
        assert!(w.iter().any(|l| l.contains("offline: 1, 3")), "{w:?}");

        // 4.1 daemon, panics recovered, every shard up: info, not "offline".
        let mut ok = summary_with("http", 0, 2, 0);
        ok.offline_shards = Some(vec![]);
        let w = summary_warnings(&ok);
        assert!(!w.iter().any(|l| l.contains("offline")), "{w:?}");
        assert!(w.iter().any(|l| l.contains("2 panics") && l.contains("every shard is up")), "{w:?}");
    }
```

Update the `summary_with` helper's struct literal by appending:

```rust
            drain_state: None,
            offline_shards: None,
            ready: None,
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl summary`
Expected: compile errors `cannot find function labelled_samples`, `struct MetricsSummary has no field named drain_state`.

- [ ] **Step 3: Implement**

Add `use std::collections::BTreeMap;` to the main.rs imports.

Add the parsing helpers after `active_label`:

```rust
/// Every labelled sample of exactly `metric` as `(labels, value)`. The family
/// must match exactly: `weir_wab_shard_up{` never matches
/// `weir_wab_shard_up_extra{`. A trailing OpenMetrics timestamp is ignored.
fn labelled_samples(body: &str, metric: &str) -> Vec<(BTreeMap<String, String>, f64)> {
    let needle = format!("{metric}{{");
    let mut out = Vec::new();
    for line in body.lines() {
        let Some(rest) = line.strip_prefix(needle.as_str()) else {
            continue;
        };
        let Some(close) = rest.rfind('}') else {
            continue;
        };
        let (labels, tail) = rest.split_at(close);
        let Some(value) = tail[1..]
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<f64>().ok())
        else {
            continue;
        };
        out.push((parse_label_set(labels), value));
    }
    out
}

/// Parses `k1="v1",k2="v2"` with `\"`, `\\` and `\n` escapes.
fn parse_label_set(src: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = src;
    while let Some(eq) = rest.find("=\"") {
        let key = rest[..eq].trim_start_matches(',').trim().to_string();
        let after = &rest[eq + 2..];
        let mut val = String::new();
        let mut end = after.len();
        let mut chars = after.char_indices();
        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => {
                    if let Some((_, n)) = chars.next() {
                        val.push(if n == 'n' { '\n' } else { n });
                    }
                }
                '"' => {
                    end = i + 1;
                    break;
                }
                _ => val.push(c),
            }
        }
        out.insert(key, val);
        rest = &after[end..];
    }
    out
}

/// `1, 3, 7`: shard ids for a warning line.
fn join_ids(ids: &[u16]) -> String {
    ids.iter().map(u16::to_string).collect::<Vec<_>>().join(", ")
}
```

Extend `MetricsSummary`:

```rust
    /// `weir_drain_state`'s active label (`draining`, `retrying_transient`,
    /// `blocked_dead_letter_full`, `stopped`), when the family is present.
    drain_state: Option<String>,
    /// Ids of shards whose `weir_wab_shard_up` is 0. `None` means a pre-4.1
    /// daemon (no such family), which is NOT the same as "none offline".
    offline_shards: Option<Vec<u16>>,
    /// `weir_ready` (4.1+).
    ready: Option<bool>,
```

In `parse_summary`, add to the struct literal:

```rust
        drain_state: active_label(body, "weir_drain_state", "state"),
        offline_shards: {
            let up = labelled_samples(body, "weir_wab_shard_up");
            if up.is_empty() {
                None
            } else {
                let mut ids: Vec<u16> = up
                    .iter()
                    .filter(|(_, v)| *v == 0.0)
                    .filter_map(|(l, _)| l.get("shard")?.parse().ok())
                    .collect();
                ids.sort_unstable();
                Some(ids)
            }
        },
        ready: get_metric(body, "weir_ready").map(|v| v == 1.0),
```

In `summary_json`, add three keys after `"fsync_failures"`:

```rust
        "drain_state": s.drain_state,
        "offline_shards": s.offline_shards,
        "ready": s.ready,
```

Replace `summary_warnings` with:

```rust
fn summary_warnings(s: &MetricsSummary) -> Vec<String> {
    let mut out = Vec::new();
    // A stopped or blocked drain explains most other symptoms, so it leads.
    match s.drain_state.as_deref() {
        Some("stopped") => {
            out.push(
                "⚠ drain: STOPPED — nothing is delivered and the WAB grows; restart the \
                 daemon to resume delivery"
                    .to_string(),
            );
            out.push(
                "⚠ sink_health is stale while the drain is stopped (no health probe runs)"
                    .to_string(),
            );
        }
        Some("blocked_dead_letter_full") => out.push(
            "⚠ drain: BLOCKED — dead_letter/ is full; free space (weir-ctl dl requeue / dl \
             drop) or raise dead_letter_max_bytes"
                .to_string(),
        ),
        _ => {}
    }
    if let Some(off) = &s.offline_shards
        && !off.is_empty()
    {
        out.push(format!(
            "⚠ shard(s) offline: {} — new connections avoid them; restart to recover",
            join_ids(off)
        ));
    }
    if s.sink_type == "noop" {
        out.push(
            "\n⚠ sink: noop — records are acked then DISCARDED, not delivered downstream. \
             Set --sink-type (http/mysql/postgres/clickhouse) to forward records."
                .to_string(),
        );
    }
    if s.panics > 0 {
        match &s.offline_shards {
            // 4.1+: the shard list above is the truth; a panic counter alone is
            // history (the supervisor respawns flushers).
            Some(off) if off.is_empty() => out.push(format!(
                "ℹ flusher {} since start (respawned; every shard is up)",
                plural(s.panics, "panic", "panics")
            )),
            Some(_) => out.push(format!(
                "ℹ flusher {} since start (see the offline shard list above)",
                plural(s.panics, "panic", "panics")
            )),
            // Pre-4.1 daemon: unchanged wording.
            None => out.push(format!(
                "\n⚠ flusher {} — a shard is offline until restart",
                plural(s.panics, "panic", "panics")
            )),
        }
    }
    if s.fsync_failures > 0 {
        out.push(format!(
            "⚠ {} — DURABILITY HAZARD (data may not be on stable storage)",
            plural(s.fsync_failures, "fsync failure", "fsync failures")
        ));
    }
    if s.nacked > 0 {
        out.push(format!(
            "ℹ {} nacked — check producer behaviour / capacity",
            plural(s.nacked, "record", "records")
        ));
    }
    out
}
```

In `print_summary`, add after the `sink` line:

```rust
    println!(
        "{:<10} state: {}  ready: {}",
        "drain",
        s.drain_state.as_deref().unwrap_or("?"),
        s.ready.map_or("?", |r| if r { "yes" } else { "no" })
    );
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl summary && cargo test -p weir-ctl metric && cargo test -p weir-ctl labelled_samples`
Expected: the four new tests pass. `summary_warnings_flag_the_durability_hazards`, `metrics_summary_json_has_expected_keys` and `metric_primitives_sum_get_and_active_label` still pass with their assertions unchanged.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "fix(ctl): metrics leads with a stopped or blocked drain and lists offline shards"`

---

### Task 7.5: `weir-ctl segments` skips `quarantine/` and gains `--files`

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

`scan_segments` (main.rs:482-546) treats every directory except `dead_letter/` as a shard, so `quarantine/` is counted as a shard (spec §14, "the miscount at weir-ctl main.rs:506"). Re-checked against 2491883: `scan_segments_ignores_the_wab_dir_lock_file` (main.rs:3251) and `scan_segments_ignores_a_confirmed_tmp_file` (main.rs:3306) exist and stay green unchanged.

**Files:**
- Modify: `crates/weir-ctl/src/main.rs`:
  - `Command::Segments` (75-80): add `files: bool`
  - the `main` match arm for `Segments`
  - `scan_segments` (482-546): skip non-shard dirs through the new `is_shard_dir_name`
  - `cmd_segments` (548-607): signature gains `files: bool`
  - new `scan_segment_files`, `classify_segment_name`, `segment_files_json`
- Test: `crates/weir-ctl/src/main.rs` `mod tests`

**Interfaces:**
- Consumes: `weir_wab::format::{EXT_ACTIVE, EXT_SEALED, EXT_CONFIRMED}`.
- Produces:
  - `fn is_shard_dir_name(name: &str) -> bool`
  - `fn classify_segment_name(name: &str) -> Option<&'static str>` (`"active" | "sealed" | "confirmed"`)
  - `struct SegFile { shard: String, name: String, state: &'static str, bytes: u64 }`
  - `fn scan_segment_files(wab_dir: &Path) -> Result<Vec<SegFile>, String>`
  - `fn segment_files_json(files: &[SegFile]) -> serde_json::Value`
  - `fn cmd_segments(wab_dir: &Path, files: bool, json: bool) -> Result<(), String>`
  - `segments --json --files` adds a top-level `"files": [{shard, name, state, bytes}]`. Without `--files` the JSON is byte-for-byte today's.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn scan_segments_does_not_count_quarantine_as_a_shard() {
        let dir = std::env::temp_dir().join(format!("weir_ctl_scan_q_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let shard = dir.join("shard_00");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 10]).unwrap();
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        std::fs::write(q.join("shard_00__seg_00000009.wab.sealed"), vec![0u8; 77]).unwrap();

        let (shards, _, _) = scan_segments(&dir).unwrap();
        let names: Vec<&str> = shards.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["shard_00"], "quarantine/ is not a shard");
        assert_eq!(shards[0].bytes, 10, "quarantined bytes are not shard bytes");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segment_files_lists_each_segment_with_state_and_size() {
        let dir = std::env::temp_dir().join(format!("weir_ctl_files_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let shard = dir.join("shard_01");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("seg_00000003.wab"), vec![0u8; 5]).unwrap();
        std::fs::write(shard.join("seg_00000002.wab.sealed"), vec![0u8; 7]).unwrap();
        std::fs::write(shard.join("seg_00000001.wab.confirmed"), vec![0u8; 36]).unwrap();
        std::fs::write(shard.join("seg_00000000.wab.confirmed.tmp"), vec![0u8; 36]).unwrap();
        std::fs::create_dir_all(dir.join("quarantine")).unwrap();
        std::fs::create_dir_all(dir.join("dead_letter")).unwrap();
        std::fs::write(dir.join(".weir.lock"), b"pid=1\n").unwrap();

        let files = scan_segment_files(&dir).unwrap();
        let rows: Vec<(&str, &str, &str, u64)> = files
            .iter()
            .map(|f| (f.shard.as_str(), f.name.as_str(), f.state, f.bytes))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("shard_01", "seg_00000001.wab.confirmed", "confirmed", 36),
                ("shard_01", "seg_00000002.wab.sealed", "sealed", 7),
                ("shard_01", "seg_00000003.wab", "active", 5),
            ],
            "sorted by shard then name; the .tmp, the lock and non-shard dirs are skipped"
        );
        let v = round_trip(&segment_files_json(&files));
        assert_eq!(v[1]["state"], serde_json::json!("sealed"));
        assert_eq!(v[1]["bytes"], serde_json::json!(7));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segments_cli_accepts_files_flag() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["weir-ctl", "segments", "--wab-dir", "/x", "--files"]).unwrap();
        assert!(matches!(cli.command, Command::Segments { files: true, .. }));
        let cli = Cli::try_parse_from(["weir-ctl", "segments", "--wab-dir", "/x"]).unwrap();
        assert!(matches!(cli.command, Command::Segments { files: false, .. }));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl segment`
Expected: compile errors `cannot find function scan_segment_files`, `variant Command::Segments has no field named files`. Once those compile, `scan_segments_does_not_count_quarantine_as_a_shard` fails with `left: ["quarantine", "shard_00"]`.

- [ ] **Step 3: Implement**

```rust
/// Directories under `wab_dir` that are NOT shards: the dead-letter store
/// (rolled up separately) and quarantine (forensic copies, see `quarantine list`).
fn is_shard_dir_name(name: &str) -> bool {
    name != "dead_letter" && name != "quarantine"
}

/// `.wab.confirmed` > `.wab.sealed` > `.wab`, longest suffix first.
fn classify_segment_name(name: &str) -> Option<&'static str> {
    use weir_wab::format::{EXT_ACTIVE, EXT_CONFIRMED, EXT_SEALED};
    if name.ends_with(EXT_CONFIRMED) {
        Some("confirmed")
    } else if name.ends_with(EXT_SEALED) {
        Some("sealed")
    } else if name.ends_with(EXT_ACTIVE) {
        Some("active")
    } else {
        None
    }
}

/// One segment file for `segments --files`.
struct SegFile {
    shard: String,
    name: String,
    state: &'static str,
    bytes: u64,
}

/// Every segment file in every shard directory, sorted by (shard, name).
/// Read-only; it never touches `.weir.lock`.
fn scan_segment_files(wab_dir: &Path) -> Result<Vec<SegFile>, String> {
    let entries =
        std::fs::read_dir(wab_dir).map_err(|e| format!("read {}: {e}", wab_dir.display()))?;
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(shard) = path.file_name().and_then(|n| n.to_str()).map(str::to_string) else {
            continue;
        };
        if !path.is_dir() || !is_shard_dir_name(&shard) {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&path) else {
            continue;
        };
        for f in files.flatten() {
            let Some(name) = f.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(state) = classify_segment_name(&name) else {
                continue;
            };
            let bytes = f.metadata().map(|m| m.len()).unwrap_or(0);
            out.push(SegFile { shard: shard.clone(), name, state, bytes });
        }
    }
    out.sort_by(|a, b| (a.shard.as_str(), a.name.as_str()).cmp(&(b.shard.as_str(), b.name.as_str())));
    Ok(out)
}

fn segment_files_json(files: &[SegFile]) -> serde_json::Value {
    serde_json::Value::Array(
        files
            .iter()
            .map(|f| {
                serde_json::json!({
                    "shard": f.shard, "name": f.name, "state": f.state, "bytes": f.bytes,
                })
            })
            .collect(),
    )
}
```

In `scan_segments`, replace the `if name == "dead_letter" { … continue; }` block's guard so the dead-letter rollup stays and every other non-shard directory is skipped:

```rust
        if name == "dead_letter" {
            if let Ok(segs) = dl_segments(&path) {
                dl_files += segs.len() as u64;
                dl_bytes += segs.iter().map(|(_, s)| *s).sum::<u64>();
            }
            continue;
        }
        if !is_shard_dir_name(&name) {
            continue;
        }
```

Add the flag to `Command::Segments`:

```rust
        /// Also list every segment file (shard, name, state, bytes).
        #[arg(long)]
        files: bool,
```

Change the match arm to `Command::Segments { wab_dir, files } => cmd_segments(&wab_dir, files, json),` and `cmd_segments`:
- Signature: `fn cmd_segments(wab_dir: &Path, files: bool, json: bool) -> Result<(), String>`.
- JSON branch:
  ```rust
  let mut v = segments_json(wab_dir, &shards, dl_files, dl_bytes);
  if files {
      v["files"] = segment_files_json(&scan_segment_files(wab_dir)?);
  }
  print_json(&v);
  return Ok(());
  ```
- Human branch: after the existing table and dead-letter line, print the files when the flag is set:
  ```rust
  if files {
      println!();
      for f in scan_segment_files(wab_dir)? {
          println!("{:<10} {:<34} {:<10} {:>12}", f.shard, f.name, f.state, fmt_bytes(f.bytes));
      }
  }
  ```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl segment && cargo test -p weir-ctl scan_segments`
Expected: the new tests pass. The existing `segments_json_has_expected_keys_and_totals` and the three `scan_segments_*` tests pass unchanged.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "fix(ctl): segments stops counting quarantine as a shard and gains --files"`

---

### Task 7.6: weir-ctl `http.rs`: one bounded HTTP/1.0 GET, shared by `metrics` and `status`

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

**Files:**
- Create: `crates/weir-ctl/src/http.rs`
- Modify: `crates/weir-ctl/src/main.rs`. Add `mod http;`. `scrape` (2189-2207) becomes `http::http_get(addr, "/metrics").map(|r| r.body)`.
- Test: `crates/weir-ctl/src/http.rs` `mod tests`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `pub(crate) const HTTP_TIMEOUT: Duration` (5 s)
  - `pub(crate) const MAX_RESPONSE_BYTES: u64` (16 MiB)
  - `pub(crate) struct HttpResponse { pub status: Option<u16>, pub content_type: Option<String>, pub body: String }` (derives `Debug, Clone, PartialEq, Eq`)
  - `pub(crate) fn http_get(addr: &str, path: &str) -> Result<HttpResponse, String>`
  - `pub(crate) fn parse_http_response(raw: &str) -> HttpResponse`
  - `pub(crate) fn is_json(content_type: Option<&str>) -> bool`

- [ ] **Step 1: Write the failing test**

Declare `mod http;` in main.rs (next to the Task 7.3 module lines), so the new file's tests are compiled. Then create `crates/weir-ctl/src/http.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    /// Serves one canned response and returns the exact request bytes it read.
    fn one_shot(response: &'static str) -> (String, std::thread::JoinHandle<Vec<u8>>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut r = BufReader::new(&s);
            let mut req = Vec::new();
            loop {
                let mut line = Vec::new();
                r.read_until(b'\n', &mut line).unwrap();
                req.extend_from_slice(&line);
                if line == b"\r\n" || line.is_empty() {
                    break;
                }
            }
            (&s).write_all(response.as_bytes()).unwrap();
            req
        });
        (addr, h)
    }

    #[test]
    fn metrics_request_bytes_are_unchanged() {
        let (addr, h) = one_shot("HTTP/1.0 200 OK\r\nContent-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\r\nweir_queue_depth 3\n");
        let r = http_get(&addr, "/metrics").unwrap();
        assert_eq!(h.join().unwrap(), b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n");
        assert_eq!(r.status, Some(200));
        assert_eq!(r.body, "weir_queue_depth 3\n");
        assert!(!is_json(r.content_type.as_deref()));
    }

    #[test]
    fn status_json_is_recognised_by_content_type() {
        let (addr, h) = one_shot("HTTP/1.0 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nCache-Control: no-store\r\n\r\n{\"schema\":\"weir.status/v1\"}");
        let r = http_get(&addr, "/status").unwrap();
        assert!(h.join().unwrap().starts_with(b"GET /status HTTP/1.0\r\n"));
        assert!(is_json(r.content_type.as_deref()));
        assert_eq!(r.body, "{\"schema\":\"weir.status/v1\"}");
    }

    #[test]
    fn parse_tolerates_a_missing_head_and_odd_header_case() {
        let r = parse_http_response("weir_queue_depth 1\n");
        assert_eq!(r, HttpResponse { status: None, content_type: None, body: "weir_queue_depth 1\n".into() });
        let r = parse_http_response("HTTP/1.1 503 Service Unavailable\r\ncontent-TYPE: text/plain\r\n\r\nnot ready: starting\n");
        assert_eq!(r.status, Some(503));
        assert_eq!(r.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn connect_refused_is_an_error_naming_the_address() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        drop(l);
        let e = http_get(&addr, "/status").unwrap_err();
        assert!(e.starts_with(&format!("connect {addr}")), "{e}");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl http::`
Expected: compile error `cannot find function http_get in this scope` (and `parse_http_response`, `is_json`, `HttpResponse`).

- [ ] **Step 3: Implement**

Above the tests in `http.rs`:

```rust
//! A minimal, bounded HTTP/1.0 GET for the daemon's metrics listener, so
//! weir-ctl needs no HTTP client dependency. The request bytes for `/metrics`
//! are exactly the ones `scrape` always sent.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Connect (per resolved address), read and write deadline.
pub(crate) const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// A /status document is bounded at 256 shards and 40 conditions (spec §10),
/// and a /metrics body is a few MiB at most; anything larger is refused.
pub(crate) const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    /// `None` when the peer sent no parseable status line.
    pub status: Option<u16>,
    pub content_type: Option<String>,
    pub body: String,
}

pub(crate) fn http_get(addr: &str, path: &str) -> Result<HttpResponse, String> {
    // Try every resolved address in turn, as `TcpStream::connect(addr)` in the
    // old `scrape` did: `localhost` may resolve to ::1 first while the daemon
    // listens on 127.0.0.1 only.
    let addrs: Vec<std::net::SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| format!("connect {addr}: {e}"))?
        .collect();
    let mut last_err: Option<std::io::Error> = None;
    let mut found = None;
    for a in &addrs {
        match TcpStream::connect_timeout(a, HTTP_TIMEOUT) {
            Ok(s) => {
                found = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = found.ok_or_else(|| match last_err {
        Some(e) => format!("connect {addr}: {e}"),
        None => format!("connect {addr}: no address"),
    })?;
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(HTTP_TIMEOUT)))
        .map_err(|e| format!("set timeout: {e}"))?;
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n").as_bytes())
        .map_err(|e| format!("write GET: {e}"))?;
    let mut raw = Vec::new();
    (&mut stream)
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|e| format!("read {path}: {e}"))?;
    if raw.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(format!("read {path}: response larger than {MAX_RESPONSE_BYTES} bytes"));
    }
    Ok(parse_http_response(&String::from_utf8_lossy(&raw)))
}

pub(crate) fn parse_http_response(raw: &str) -> HttpResponse {
    let Some((head, body)) = raw.split_once("\r\n\r\n") else {
        // No head at all: hand back everything, as `scrape` always did.
        return HttpResponse { status: None, content_type: None, body: raw.to_string() };
    };
    let mut lines = head.split("\r\n");
    let status = lines.next().and_then(|l| {
        let mut parts = l.split_whitespace();
        let version = parts.next()?;
        if !version.starts_with("HTTP/") {
            return None;
        }
        parts.next()?.parse().ok()
    });
    let content_type = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.trim().to_string());
    HttpResponse { status, content_type, body: body.to_string() }
}

/// `application/json` with any parameters. A pre-4.1 daemon answers every path,
/// `/status` included, with `application/openmetrics-text`.
pub(crate) fn is_json(content_type: Option<&str>) -> bool {
    content_type
        .and_then(|c| c.split(';').next())
        .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
}
```

In main.rs (`mod http;` is already declared from Step 1), replace the body of `scrape` with:

```rust
fn scrape(addr: &str) -> Result<String, String> {
    http::http_get(addr, "/metrics").map(|r| r.body)
}
```

Change main.rs's imports: `use std::io::{Read, Write};` becomes `use std::io::Write;`, and delete `use std::net::TcpStream;` and `use std::time::Duration;` (all three were used only by the old `scrape`; the tests call `std::io::Read::read_exact` fully qualified). Check with `grep -nE '\bDuration\b|TcpStream|\.read_to_string' crates/weir-ctl/src/main.rs`, which should print nothing (the `std::fs::read_to_string` call in Task 7.3's lock-guard test is a function path, not the `Read` method, and does not match).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl http:: && cargo test -p weir-ctl metric`
Expected: 4 new tests pass, and the existing metrics tests still pass.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "refactor(ctl): one bounded HTTP/1.0 GET for metrics and status"`

---

### Task 7.7: weir-ctl `offline.rs`: read-only disk evidence

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 "Disk evidence". It never takes or probes the lock and never calls `check_confirmed`. It collects, per shard:
- undelivered sealed segments and their record counts, from `parse_segment_footer`;
- the age of each undelivered segment, from `parse_segment_header`;
- the age of the active `.wab` segment;
- the last delivery, from the newest `.confirmed`.

It also collects the bytes in `dead_letter/` and `quarantine/`. Those two directories are excluded from the shard scan.

The drain may delete a sealed segment between the listing and the open. That is a delivery, not an error, so `NotFound` skips the segment silently.

**Files:**
- Modify: `crates/weir-ctl/src/offline.rs` (the shell from Task 7.3)
- Test: `crates/weir-ctl/src/offline.rs` `mod tests`

**Interfaces:**
- Consumes:
  - `weir_wab::{list_segment_files, SegmentState}`
  - `weir_wab::format::{confirmed_path_for, parse_segment_header, parse_segment_footer, parse_confirmed, SEGMENT_HEADER_LEN, SEGMENT_FOOTER_LEN, CONFIRMED_LEN}`
  - `crate::{dl_segments, dead_letter_dir, quarantine_segments, quarantine_dir, is_shard_dir_name}` (all in main.rs; `is_shard_dir_name` from Task 7.5)
- Produces:
  - `pub(crate) struct ShardDisk { pub name: String, pub id: Option<u16>, pub undelivered_segments: u64, pub undelivered_records: u64, pub unreadable_segments: u64, pub oldest_undelivered_created_ns: Option<i64>, pub active_segments: u64, pub oldest_active_created_ns: Option<i64>, pub last_delivery_ns: Option<i64> }` (derives `Debug, Clone, Default, PartialEq, Eq`)
  - `pub(crate) struct DiskFacts { pub wab_dir: PathBuf, pub shards: Vec<ShardDisk>, pub dead_letter_files: u64, pub dead_letter_bytes: u64, pub quarantine_files: u64, pub quarantine_bytes: u64, pub scanned_at_ms: u64 }` (derives `Debug, Clone, Default, PartialEq, Eq`)
  - `impl DiskFacts`:
    - `pub fn undelivered_segments(&self) -> u64`
    - `pub fn undelivered_records(&self) -> u64`
    - `pub fn unreadable_segments(&self) -> u64`
    - `pub fn active_segments(&self) -> u64`
    - `pub fn last_delivery_ns(&self) -> Option<i64>`
    - `pub fn oldest_undelivered_age_secs(&self, now_ms: u64) -> Option<f64>`
  - `pub(crate) fn scan_disk(wab_dir: &Path, now_ms: u64) -> Result<DiskFacts, String>`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use weir_wab::format::{Compression, build_confirmed, build_segment_footer, build_segment_header};

    const NOW_MS: u64 = 1_700_000_100_000;
    const T0_NS: i64 = 1_700_000_000_000_000_000; // 100 s before NOW_MS

    fn tmp(label: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("weir_ctl_offline_{label}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Header (with a chosen created_at) + sentinel + footer. No record bodies:
    /// the scan reads only the header and the footer.
    fn segment_bytes(created_ns: i64, records: u64) -> Vec<u8> {
        let mut header = build_segment_header(0, Compression::None);
        header[8..16].copy_from_slice(&created_ns.to_le_bytes());
        let mut b = header.to_vec();
        b.extend_from_slice(&[0u8; 4]);
        b.extend_from_slice(&build_segment_footer(records, 0, 0, created_ns + 1));
        b
    }

    fn tree(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                out.push(p.strip_prefix(dir).unwrap().display().to_string());
                if p.is_dir() {
                    stack.push(p);
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn counts_undelivered_records_ages_and_last_delivery() {
        let wab = tmp("counts");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        // Delivered: sealed with its sidecar (delete pending), and an older sidecar alone.
        std::fs::write(s0.join("seg_00000001.wab.confirmed"), build_confirmed(T0_NS, 4, T0_NS + 10_000_000_000)).unwrap();
        std::fs::write(s0.join("seg_00000002.wab.sealed"), segment_bytes(T0_NS, 9)).unwrap();
        std::fs::write(s0.join("seg_00000002.wab.confirmed"), build_confirmed(T0_NS, 9, T0_NS + 20_000_000_000)).unwrap();
        // Undelivered: two sealed without sidecars.
        std::fs::write(s0.join("seg_00000003.wab.sealed"), segment_bytes(T0_NS + 5_000_000_000, 7)).unwrap();
        std::fs::write(s0.join("seg_00000004.wab.sealed"), segment_bytes(T0_NS + 30_000_000_000, 11)).unwrap();
        // Active.
        std::fs::write(s0.join("seg_00000005.wab"), &segment_bytes(T0_NS + 40_000_000_000, 0)[..24]).unwrap();

        let f = scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(f.shards.len(), 1);
        let s = &f.shards[0];
        assert_eq!((s.name.as_str(), s.id), ("shard_00", Some(0)));
        assert_eq!(s.undelivered_segments, 2);
        assert_eq!(s.undelivered_records, 18);
        assert_eq!(s.oldest_undelivered_created_ns, Some(T0_NS + 5_000_000_000));
        assert_eq!(s.active_segments, 1);
        assert_eq!(s.oldest_active_created_ns, Some(T0_NS + 40_000_000_000));
        assert_eq!(s.last_delivery_ns, Some(T0_NS + 20_000_000_000), "newest sidecar by name");
        let age = f.oldest_undelivered_age_secs(NOW_MS).unwrap();
        assert!((age - 95.0).abs() < 0.001, "{age}");
        assert_eq!(f.scanned_at_ms, NOW_MS);
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn dead_letter_and_quarantine_are_rolled_up_not_scanned_as_shards() {
        let wab = tmp("rollup");
        std::fs::create_dir_all(wab.join("shard_00")).unwrap();
        let dl = wab.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        std::fs::write(dl.join("dl_00000001.wab.sealed"), vec![0u8; 50]).unwrap();
        let q = wab.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        std::fs::write(q.join("shard_00__seg_00000009.wab.sealed"), vec![0u8; 70]).unwrap();
        std::fs::write(wab.join(".weir.lock"), b"pid=1\n").unwrap();

        let f = scan_disk(&wab, NOW_MS).unwrap();
        let names: Vec<&str> = f.shards.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["shard_00"]);
        assert_eq!((f.dead_letter_files, f.dead_letter_bytes), (1, 50));
        assert_eq!((f.quarantine_files, f.quarantine_bytes), (1, 70));
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn a_short_sealed_file_is_undelivered_and_unreadable() {
        let wab = tmp("short");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        std::fs::write(s0.join("seg_00000001.wab.sealed"), b"WEIR").unwrap();
        let f = scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(f.undelivered_segments(), 1);
        assert_eq!(f.undelivered_records(), 0);
        assert_eq!(f.unreadable_segments(), 1);
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn a_missing_wab_dir_is_an_error() {
        let missing = std::env::temp_dir().join(format!("weir_ctl_offline_missing_{}_x", std::process::id()));
        assert!(scan_disk(&missing, NOW_MS).unwrap_err().contains("read "));
    }

    #[test]
    fn the_scan_writes_nothing_and_leaves_the_lock_alone() {
        let wab = tmp("readonly");
        let s0 = wab.join("shard_00");
        std::fs::create_dir_all(&s0).unwrap();
        std::fs::write(s0.join("seg_00000001.wab.sealed"), segment_bytes(T0_NS, 3)).unwrap();
        std::fs::write(wab.join(".weir.lock"), b"pid=1\nhost=h\n").unwrap();
        let before = tree(&wab);
        scan_disk(&wab, NOW_MS).unwrap();
        assert_eq!(tree(&wab), before, "no sidecar, rename or unlink");
        assert_eq!(std::fs::read(wab.join(".weir.lock")).unwrap(), b"pid=1\nhost=h\n");
        std::fs::remove_dir_all(&wab).ok();
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl offline::`
Expected: compile error `cannot find function scan_disk in this scope`.

- [ ] **Step 3: Implement**

Replace the shell doc line in `offline.rs` with:

```rust
//! Read-only facts from a `wab_dir`, for `weir-ctl status` when no daemon
//! answers, or to fill in what an old daemon's /metrics lacks (spec §14).
//!
//! Never takes, probes or releases `.weir.lock`, never calls
//! `check_confirmed`, and never writes: no sidecar, no rename, no unlink. A
//! sealed segment the live drain deletes between the listing and the open was
//! delivered, so `NotFound` skips it silently.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use weir_wab::format::{
    CONFIRMED_LEN, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, confirmed_path_for, parse_confirmed,
    parse_segment_footer, parse_segment_header,
};
use weir_wab::{SegmentState, list_segment_files};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ShardDisk {
    pub name: String,
    /// Parsed from `shard_NN`.
    pub id: Option<u16>,
    /// Sealed segments with no `.confirmed` sidecar.
    pub undelivered_segments: u64,
    /// Sum of their footers' `record_count`.
    pub undelivered_records: u64,
    /// Undelivered segments whose header or footer could not be read.
    pub unreadable_segments: u64,
    pub oldest_undelivered_created_ns: Option<i64>,
    pub active_segments: u64,
    pub oldest_active_created_ns: Option<i64>,
    /// `drained_at` of the newest `.confirmed` (by name: counters are zero-padded).
    pub last_delivery_ns: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DiskFacts {
    pub wab_dir: PathBuf,
    pub shards: Vec<ShardDisk>,
    pub dead_letter_files: u64,
    pub dead_letter_bytes: u64,
    pub quarantine_files: u64,
    pub quarantine_bytes: u64,
    pub scanned_at_ms: u64,
}

impl DiskFacts {
    pub fn undelivered_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.undelivered_segments).sum()
    }
    pub fn undelivered_records(&self) -> u64 {
        self.shards.iter().map(|s| s.undelivered_records).sum()
    }
    pub fn unreadable_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.unreadable_segments).sum()
    }
    pub fn active_segments(&self) -> u64 {
        self.shards.iter().map(|s| s.active_segments).sum()
    }
    pub fn last_delivery_ns(&self) -> Option<i64> {
        self.shards.iter().filter_map(|s| s.last_delivery_ns).max()
    }
    /// An upper bound, like the daemon's (spec §9.1): header `created_at` of
    /// the oldest undelivered segment.
    pub fn oldest_undelivered_age_secs(&self, now_ms: u64) -> Option<f64> {
        let oldest = self.shards.iter().filter_map(|s| s.oldest_undelivered_created_ns).min()?;
        let now_ns = (now_ms as i128) * 1_000_000;
        Some((((now_ns - oldest as i128).max(0)) as f64) / 1e9)
    }
}

pub(crate) fn scan_disk(wab_dir: &Path, now_ms: u64) -> Result<DiskFacts, String> {
    let entries =
        std::fs::read_dir(wab_dir).map_err(|e| format!("read {}: {e}", wab_dir.display()))?;
    let mut shards = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()).map(str::to_string) else {
            continue;
        };
        if !path.is_dir() || name.starts_with('.') || !crate::is_shard_dir_name(&name) {
            continue;
        }
        shards.push(scan_shard(&path, name));
    }
    shards.sort_by(|a, b| a.name.cmp(&b.name));

    let dl = crate::dl_segments(&crate::dead_letter_dir(wab_dir))?;
    let q = crate::quarantine_segments(&crate::quarantine_dir(wab_dir))?;
    Ok(DiskFacts {
        wab_dir: wab_dir.to_path_buf(),
        shards,
        dead_letter_files: dl.len() as u64,
        dead_letter_bytes: dl.iter().map(|(_, s)| *s).sum(),
        quarantine_files: q.len() as u64,
        quarantine_bytes: q.iter().map(|(_, s)| *s).sum(),
        scanned_at_ms: now_ms,
    })
}

fn scan_shard(dir: &Path, name: String) -> ShardDisk {
    let mut s = ShardDisk {
        id: name.strip_prefix("shard_").and_then(|n| n.parse().ok()),
        name,
        ..ShardDisk::default()
    };
    let Ok(files) = list_segment_files(dir) else {
        return s;
    };
    let mut newest_confirmed: Option<PathBuf> = None;
    for (path, state) in files {
        match state {
            SegmentState::Confirmed => newest_confirmed = Some(path), // sorted by path
            SegmentState::Sealed => {
                if confirmed_path_for(&path).exists() {
                    continue; // delivered; only the delete is pending
                }
                match read_sealed_meta(&path) {
                    Ok(Some((created, records))) => {
                        s.undelivered_segments += 1;
                        s.undelivered_records += records;
                        s.oldest_undelivered_created_ns =
                            Some(s.oldest_undelivered_created_ns.map_or(created, |o| o.min(created)));
                    }
                    Ok(None) => {
                        s.undelivered_segments += 1;
                        s.unreadable_segments += 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => {
                        s.undelivered_segments += 1;
                        s.unreadable_segments += 1;
                    }
                }
            }
            SegmentState::Active => match read_header_created(&path) {
                Ok(created) => {
                    s.active_segments += 1;
                    if let Some(c) = created {
                        s.oldest_active_created_ns =
                            Some(s.oldest_active_created_ns.map_or(c, |o| o.min(c)));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => s.active_segments += 1,
            },
        }
    }
    if let Some(p) = newest_confirmed {
        let mut buf = [0u8; CONFIRMED_LEN];
        if File::open(&p).and_then(|mut f| f.read_exact(&mut buf)).is_ok() {
            s.last_delivery_ns = parse_confirmed(&buf).ok().map(|m| m.drained_at);
        }
    }
    s
}

/// `(created_at, record_count)`, or `None` if the header or footer does not
/// parse or is implausible (footer `data_bytes` larger than the file).
fn read_sealed_meta(path: &Path) -> io::Result<Option<(i64, u64)>> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    if len < (SEGMENT_HEADER_LEN + SEGMENT_FOOTER_LEN) as u64 {
        return Ok(None);
    }
    let mut head = [0u8; SEGMENT_HEADER_LEN];
    f.read_exact(&mut head)?;
    let Ok(h) = parse_segment_header(&head) else {
        return Ok(None);
    };
    f.seek(SeekFrom::Start(len - SEGMENT_FOOTER_LEN as u64))?;
    let mut foot = [0u8; SEGMENT_FOOTER_LEN];
    f.read_exact(&mut foot)?;
    match parse_segment_footer(&foot) {
        Ok(m) if m.data_bytes <= len => Ok(Some((h.created_at, m.record_count))),
        _ => Ok(None),
    }
}

fn read_header_created(path: &Path) -> io::Result<Option<i64>> {
    let mut f = File::open(path)?;
    let mut head = [0u8; SEGMENT_HEADER_LEN];
    match f.read_exact(&mut head) {
        Ok(()) => Ok(parse_segment_header(&head).ok().map(|h| h.created_at)),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None), // just created
        Err(e) => Err(e),
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl offline:: && cargo test -p weir-ctl weir_ctl_never_takes_the_wab_dir_lock`
Expected: 5 new tests pass, and the lock guard still passes with `offline.rs` now holding real code.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): read-only disk evidence for offline status"`

---

### Task 7.8: weir-ctl admin target resolution and the pure identity predicates

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 sets the admin socket path chain: `--admin-socket` / `WEIR_ADMIN_SOCKET_PATH`, then derived from `--wab-dir` / `WEIR_WAB_DIR`, then `/var/lib/weir/wab.admin.sock`. Spec §11 sets the client-side server authentication:
- `lstat`: the path is S_IFSOCK with mode 0600, and its parent is not group- or world-writable;
- the peer uid is weir-ctl's euid, the owner of `--wab-dir`, or 0;
- `instance.wab_dir_dev/ino` matches `stat(--wab-dir)`.

This task holds the pure parts, so the spec's "wrong uid via the predicate" test needs no second user.

A named wab_dir is canonicalized like the daemon's `wab_dir` (config/mod.rs:1351), so a symlinked `--wab-dir` derives the same socket path. A wab_dir that does not exist (or cannot be canonicalized) falls back to `std::path::absolute`.

A wab_dir the operator named (flag or env) is *strict*: if it cannot be stat'ed, the identity check fails. The default `/var/lib/weir/wab` is checked only when it exists. An explicit `--admin-socket` with no `--wab-dir` has no wab_dir to compare. A best-effort default compare there would wrongly flag a daemon with a custom `wab_dir`.

**Files:**
- Modify: `crates/weir-ctl/src/admin_client.rs` (the shell from Task 7.3)
- Modify: `crates/weir-ctl/Cargo.toml`. Add `weir-status = { workspace = true }` under `[dependencies]` with a comment: "the shared evaluator, status types and admin protocol helpers (spec §5.1); weir-ctl must evaluate offline without depending on weir-server". This is done in Step 1, so the tests' `weir_status::admin::default_socket_path` resolves.
- Modify: `Cargo.lock` (root), whose weir-ctl dependency list gains `weir-status`.
- Test: `crates/weir-ctl/src/admin_client.rs` `mod tests`

**Interfaces:**
- Consumes: `weir_status::admin::default_socket_path(wab_dir: &Path) -> Option<PathBuf>` (PR 6).
- Produces:
  - `pub(crate) const DEFAULT_WAB_DIR: &str = "/var/lib/weir/wab"`
  - `pub(crate) const DEFAULT_ADMIN_SOCKET: &str = "/var/lib/weir/wab.admin.sock"`
  - `pub(crate) enum AdminPathOrigin { Explicit, DerivedFromWabDir, Default }` (derives `Debug, Clone, Copy, PartialEq, Eq`)
  - `pub(crate) struct AdminTarget { pub socket: PathBuf, pub origin: AdminPathOrigin, pub wab_dir: Option<PathBuf>, pub wab_dir_strict: bool }` (derives `Debug, Clone, PartialEq, Eq`)
  - `pub(crate) fn resolve_admin_target(admin_socket: Option<&Path>, wab_dir: Option<&Path>) -> AdminTarget`
  - `pub(crate) fn check_socket_modes(is_socket: bool, mode: u32, parent_mode: u32) -> Result<(), String>`
  - `pub(crate) fn peer_uid_allowed(peer: u32, euid: u32, wab_owner: Option<u32>) -> bool`
  - `pub(crate) fn check_instance(instance: &serde_json::Value, expected: Option<(u64, u64)>) -> Result<(), String>`

- [ ] **Step 1: Write the failing test**

First add `weir-status = { workspace = true }` to `crates/weir-ctl/Cargo.toml` as described under **Files**, so the only compile error in Step 2 is the missing functions. Then add to `admin_client.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn explicit_socket_wins_and_is_not_compared_to_a_default_wab_dir() {
        let t = resolve_admin_target(Some(Path::new("/run/x/a.sock")), None);
        assert_eq!(t.socket, PathBuf::from("/run/x/a.sock"));
        assert_eq!(t.origin, AdminPathOrigin::Explicit);
        assert_eq!(t.wab_dir, None);
        let t = resolve_admin_target(Some(Path::new("/run/x/a.sock")), Some(Path::new("/data/wab")));
        assert_eq!(t.wab_dir, Some(PathBuf::from("/data/wab")));
        assert!(t.wab_dir_strict);
    }

    #[test]
    fn derived_default_matches_the_daemon_rule() {
        // <parent(wab_dir)>/<basename(wab_dir)>.admin.sock (spec §5.9)
        let t = resolve_admin_target(None, Some(Path::new("/data/weir/wab")));
        assert_eq!(t.socket, PathBuf::from("/data/weir/wab.admin.sock"));
        assert_eq!(t.origin, AdminPathOrigin::DerivedFromWabDir);
        assert!(t.wab_dir_strict);
        let t = resolve_admin_target(None, Some(Path::new("/data/weir/wab/")));
        assert_eq!(t.socket, PathBuf::from("/data/weir/wab.admin.sock"), "a trailing slash is the same dir");
    }

    #[test]
    fn nothing_given_uses_the_packaged_default_best_effort() {
        let t = resolve_admin_target(None, None);
        assert_eq!(t.socket, PathBuf::from(DEFAULT_ADMIN_SOCKET));
        assert_eq!(t.origin, AdminPathOrigin::Default);
        assert_eq!(t.wab_dir, Some(PathBuf::from(DEFAULT_WAB_DIR)));
        assert!(!t.wab_dir_strict);
        assert_eq!(
            weir_status::admin::default_socket_path(Path::new(DEFAULT_WAB_DIR)),
            Some(PathBuf::from(DEFAULT_ADMIN_SOCKET)),
            "the two defaults must agree"
        );
    }

    #[test]
    fn socket_modes_must_be_a_0600_socket_in_a_private_parent() {
        assert!(check_socket_modes(true, 0o140600, 0o040700).is_ok());
        assert!(check_socket_modes(true, 0o140600, 0o040755).is_ok());
        assert!(check_socket_modes(false, 0o100600, 0o040700).unwrap_err().contains("not a socket"));
        assert!(check_socket_modes(true, 0o140660, 0o040700).unwrap_err().contains("600"));
        assert!(check_socket_modes(true, 0o140600, 0o040770).unwrap_err().contains("writable"));
        assert!(check_socket_modes(true, 0o140600, 0o041777).unwrap_err().contains("writable"), "sticky /tmp is still world-writable");
    }

    #[test]
    fn peer_uid_must_be_us_the_wab_owner_or_root() {
        assert!(peer_uid_allowed(1000, 1000, None));
        assert!(peer_uid_allowed(0, 1000, None), "root");
        assert!(peer_uid_allowed(10001, 1000, Some(10001)), "the wab_dir owner (operator is root or in sudo)");
        assert!(!peer_uid_allowed(4242, 1000, Some(10001)), "a stranger is refused: the wrong-uid identity check");
        assert!(!peer_uid_allowed(4242, 1000, None));
    }

    #[test]
    fn instance_must_carry_the_wab_dir_dev_and_ino() {
        let inst = json!({"pid": 7, "wab_dir_dev": 64769, "wab_dir_ino": 131074});
        assert!(check_instance(&inst, None).is_ok());
        assert!(check_instance(&inst, Some((64769, 131074))).is_ok());
        let e = check_instance(&inst, Some((64769, 999))).unwrap_err();
        assert!(e.contains("131074") && e.contains("999"), "{e}");
        assert!(check_instance(&json!({"pid": 7}), Some((1, 2))).is_err(), "no identity is a mismatch");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl admin_client::`
Expected: compile error `cannot find function resolve_admin_target in this scope` (and the other three). No unresolved-crate error: the `weir-status` dependency is already in place from Step 1.

- [ ] **Step 3: Implement**

`admin_client.rs` (above the tests):

```rust
//! Client for the daemon's admin socket (spec §11), with the client-side
//! identity checks: weir-ctl never trusts a payload from a socket it has not
//! authenticated.

use std::path::{Path, PathBuf};

use serde_json::Value;

/// The packaged `wab_dir` default (weir-server config/mod.rs).
pub(crate) const DEFAULT_WAB_DIR: &str = "/var/lib/weir/wab";
/// `default_socket_path(DEFAULT_WAB_DIR)`.
pub(crate) const DEFAULT_ADMIN_SOCKET: &str = "/var/lib/weir/wab.admin.sock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminPathOrigin {
    /// `--admin-socket` or `WEIR_ADMIN_SOCKET_PATH` (clap merges the two).
    Explicit,
    /// Derived from `--wab-dir` / `WEIR_WAB_DIR`.
    DerivedFromWabDir,
    /// Nothing given: the packaged default.
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdminTarget {
    pub socket: PathBuf,
    pub origin: AdminPathOrigin,
    /// The wab_dir whose (dev, ino) the daemon's `instance` must match.
    pub wab_dir: Option<PathBuf>,
    /// The operator named it: a wab_dir that cannot be stat'ed then fails the
    /// identity check instead of skipping it.
    pub wab_dir_strict: bool,
}

pub(crate) fn resolve_admin_target(admin_socket: Option<&Path>, wab_dir: Option<&Path>) -> AdminTarget {
    // Canonicalized like the daemon's wab_dir (config/mod.rs:1351), so a
    // symlinked --wab-dir derives the same socket path the daemon bound.
    let named = wab_dir.map(|d| {
        std::fs::canonicalize(d)
            .or_else(|_| std::path::absolute(d))
            .unwrap_or_else(|_| d.to_path_buf())
    });
    if let Some(sock) = admin_socket {
        return AdminTarget {
            socket: sock.to_path_buf(),
            origin: AdminPathOrigin::Explicit,
            wab_dir_strict: named.is_some(),
            wab_dir: named,
        };
    }
    match named {
        Some(d) => AdminTarget {
            socket: weir_status::admin::default_socket_path(&d)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_ADMIN_SOCKET)),
            origin: AdminPathOrigin::DerivedFromWabDir,
            wab_dir: Some(d),
            wab_dir_strict: true,
        },
        None => AdminTarget {
            socket: PathBuf::from(DEFAULT_ADMIN_SOCKET),
            origin: AdminPathOrigin::Default,
            wab_dir: Some(PathBuf::from(DEFAULT_WAB_DIR)),
            wab_dir_strict: false,
        },
    }
}

/// `lstat` facts of the socket and `stat` of its parent (spec §11).
pub(crate) fn check_socket_modes(is_socket: bool, mode: u32, parent_mode: u32) -> Result<(), String> {
    if !is_socket {
        return Err("not a socket".to_string());
    }
    if mode & 0o777 != 0o600 {
        return Err(format!("mode {:o}, expected 600", mode & 0o777));
    }
    if parent_mode & 0o022 != 0 {
        return Err(format!(
            "its parent directory is group- or world-writable (mode {:o}), so anyone could \
             have put this socket there",
            parent_mode & 0o7777
        ));
    }
    Ok(())
}

/// The daemon runs as weir-ctl's euid, as the wab_dir's owner, or as root.
pub(crate) fn peer_uid_allowed(peer: u32, euid: u32, wab_owner: Option<u32>) -> bool {
    peer == euid || peer == 0 || wab_owner == Some(peer)
}

/// The reply's `instance` must name the same wab_dir inode the operator meant.
pub(crate) fn check_instance(instance: &Value, expected: Option<(u64, u64)>) -> Result<(), String> {
    let Some((dev, ino)) = expected else {
        return Ok(());
    };
    let got_dev = instance.get("wab_dir_dev").and_then(Value::as_u64);
    let got_ino = instance.get("wab_dir_ino").and_then(Value::as_u64);
    match (got_dev, got_ino) {
        (Some(d), Some(i)) if d == dev && i == ino => Ok(()),
        (Some(d), Some(i)) => Err(format!(
            "the daemon serves wab_dir (dev {d}, ino {i}), but --wab-dir is (dev {dev}, ino {ino})"
        )),
        _ => Err("the reply carries no wab_dir identity".to_string()),
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl admin_client::`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src crates/weir-ctl/Cargo.toml Cargo.lock && git commit -m "feat(ctl): resolve the admin socket path and the identity predicates"` (`Cargo.lock` changes only in weir-ctl's dependency list; the `chaos/` and `fuzz/` lockfiles are untouched, since neither depends on weir-ctl.)

---

### Task 7.9: weir-ctl admin call: lstat, bounded connect, peer uid, one JSON line each way, instance check

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Security-review item for the merge gate.

Re-checked against 2491883 (socket/mod.rs:801-821): on non-Linux, ECONNREFUSED can also be a live listener with a full backlog. weir-ctl treats any refusal as "nothing answered" (safe: status falls through, mutations exit 2) but does not call it stale there.

**Files:**
- Modify: `crates/weir-ctl/src/admin_client.rs`
- Modify: `Cargo.lock` (root), whose weir-ctl dependency list gains `libc`.
- Modify: `crates/weir-ctl/Cargo.toml`. Add:
  ```toml
  [target.'cfg(unix)'.dependencies]
  # Peer credentials of the admin socket (SO_PEERCRED / getpeereid) and geteuid,
  # for the client-side identity check (spec §11). std's UnixStream::peer_cred is
  # still unstable at MSRV 1.88; libc is already in the lock tree via weir-server.
  libc = "0.2"
  ```
- Test: `crates/weir-ctl/src/admin_client.rs`. Add `#[cfg(all(test, unix))] pub(crate) mod test_support` (the fake server, reused by Tasks 7.13 and 7.14) and more tests in `mod tests`.

**Interfaces:**
- Consumes: Task 7.8 items; the admin wire format from PR 6.
- Produces:
  - `pub(crate) enum CallError { Unreachable(String), Identity(String), Remote { code: String, message: String, retryable: bool }, Protocol(String) }` (derives `Debug`)
  - `pub(crate) struct AdminReply { pub instance: Value, pub result: Value }` (derives `Debug`)
  - `pub(crate) const CONNECT_TIMEOUT: Duration` (2 s)
  - `pub(crate) const MAX_REPLY_BYTES: u64` (4 MiB)
  - `pub(crate) fn call(target: &AdminTarget, verb: &str, args: Value, reply_timeout: Duration) -> Result<AdminReply, CallError>` (on non-unix it returns `Unreachable("the admin socket is Unix-only")`)
  - `pub(crate) fn parse_reply(line: &str, expected: Option<(u64, u64)>) -> Result<AdminReply, CallError>`
  - `pub(crate) fn instance_line(instance: &Value) -> String`
  - Test-only, in `test_support`:
    - `fn private_dir(label: &str) -> PathBuf`
    - `fn instance_for(wab: &Path) -> Value`
    - `fn fake_admin(sock: &Path, reply: impl Fn(&Value) -> Value + Send + 'static) -> JoinHandle<Value>`

- [ ] **Step 1: Write the failing test**

Test support module, appended to `admin_client.rs`:

```rust
#[cfg(all(test, unix))]
pub(crate) mod test_support {
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};

    /// A 0700 directory: the admin parent rule refuses /tmp itself.
    /// Kept short: macOS sun_path is 104 bytes.
    pub(crate) fn private_dir(label: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("wctl_{label}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
        d
    }

    pub(crate) fn instance_for(wab: &Path) -> Value {
        let m = std::fs::metadata(wab).unwrap();
        json!({"pid": 4242, "started_at_ms": 1, "wab_dir": wab.display().to_string(),
               "socket_path": "/run/weir/weir.sock", "wab_dir_dev": m.dev(), "wab_dir_ino": m.ino()})
    }

    /// Binds a 0600 admin socket, answers ONE request with `reply(request)`,
    /// and returns the request it read.
    pub(crate) fn fake_admin(
        sock: &Path,
        reply: impl Fn(&Value) -> Value + Send + 'static,
    ) -> std::thread::JoinHandle<Value> {
        let l = UnixListener::bind(sock).unwrap();
        std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut line = String::new();
            BufReader::new(&s).read_line(&mut line).unwrap();
            let req: Value = serde_json::from_str(&line).unwrap();
            let mut out = serde_json::to_string(&reply(&req)).unwrap();
            out.push('\n');
            (&s).write_all(out.as_bytes()).unwrap();
            req
        })
    }
}
```

Tests appended to `mod tests` (gated `#[cfg(unix)]` per test):

```rust
    #[cfg(unix)]
    mod call_tests {
        use super::super::test_support::*;
        use super::super::*;
        use serde_json::json;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        fn target(dir: &Path, wab: &Path) -> AdminTarget {
            AdminTarget {
                socket: dir.join("a.sock"),
                origin: AdminPathOrigin::Explicit,
                wab_dir: Some(wab.to_path_buf()),
                wab_dir_strict: true,
            }
        }

        #[test]
        fn ok_reply_returns_the_result_and_the_request_is_a_v1_envelope() {
            let dir = private_dir("ok");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true,
                       "result": {"schema": "weir.status/v1"}})
            });
            let r = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap();
            assert_eq!(r.result["schema"], "weir.status/v1");
            assert_eq!(r.instance["pid"], 4242);
            let req = h.join().unwrap();
            assert_eq!(req["v"], 1);
            assert_eq!(req["verb"], "status");
            assert_eq!(req["args"], json!({}));
            assert!(req.get("id").is_some());
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_wab_dir_ino_mismatch_is_an_identity_error() {
            let dir = private_dir("ino");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let mut inst = instance_for(&wab);
            inst["wab_dir_ino"] = json!(inst["wab_dir_ino"].as_u64().unwrap() + 1);
            let _h = fake_admin(&dir.join("a.sock"), move |req| {
                json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true, "result": {}})
            });
            let e = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap_err();
            assert!(matches!(e, CallError::Identity(_)), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_group_writable_parent_is_refused_before_connecting() {
            let dir = private_dir("gw");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let sock = dir.join("a.sock");
            let _l = std::os::unix::net::UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o770)).unwrap();
            let e = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap_err();
            assert!(matches!(e, CallError::Identity(ref w) if w.contains("writable")), "{e:?}");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_world_accessible_socket_is_refused() {
            let dir = private_dir("mode");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let sock = dir.join("a.sock");
            let _l = std::os::unix::net::UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o666)).unwrap();
            let e = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap_err();
            assert!(matches!(e, CallError::Identity(_)), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn absent_and_stale_sockets_are_unreachable() {
            let dir = private_dir("gone");
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let e = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap_err();
            assert!(matches!(e, CallError::Unreachable(_)), "absent: {e:?}");
            {
                let _l = std::os::unix::net::UnixListener::bind(dir.join("a.sock")).unwrap();
                std::fs::set_permissions(dir.join("a.sock"), std::fs::Permissions::from_mode(0o600)).unwrap();
            } // dropped: the file stays, nobody listens
            let e = call(&target(&dir, &wab), "status", json!({}), Duration::from_secs(5)).unwrap_err();
            assert!(matches!(e, CallError::Unreachable(_)), "stale: {e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn a_strict_wab_dir_that_does_not_exist_fails_identity() {
            let dir = private_dir("nowab");
            // A real 0600 socket, so the only failing check is the wab_dir stat.
            let sock = dir.join("a.sock");
            let _l = std::os::unix::net::UnixListener::bind(&sock).unwrap();
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
            let e = call(&target(&dir, &dir.join("missing")), "status", json!({}), Duration::from_secs(1)).unwrap_err();
            assert!(matches!(e, CallError::Identity(ref w) if w.contains("cannot stat --wab-dir")), "{e:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn parse_reply_maps_errors_and_never_trusts_an_unidentified_success() {
            let inst = json!({"pid": 1, "wab_dir_dev": 1, "wab_dir_ino": 2});
            let err = format!(
                "{}",
                json!({"v":1,"id":1,"instance":inst,"ok":false,
                       "error":{"code":"shutting_down","message":"daemon is shutting down","retryable":false}})
            );
            match parse_reply(&err, Some((1, 2))).unwrap_err() {
                CallError::Remote { code, retryable, .. } => {
                    assert_eq!(code, "shutting_down");
                    assert!(!retryable);
                }
                other => panic!("{other:?}"),
            }
            // forbidden before identification: no instance, the code is still reported.
            let forbidden = r#"{"v":1,"id":null,"ok":false,"error":{"code":"forbidden","message":"peer uid","retryable":false}}"#;
            assert!(matches!(parse_reply(forbidden, None), Err(CallError::Remote { .. })));
            // ok without instance: refused.
            let bare = r#"{"v":1,"id":1,"ok":true,"result":{}}"#;
            assert!(matches!(parse_reply(bare, None), Err(CallError::Protocol(_))));
            assert!(matches!(parse_reply("not json", None), Err(CallError::Protocol(_))));
            assert!(matches!(parse_reply(r#"{"v":2,"ok":true}"#, None), Err(CallError::Protocol(_))));
        }
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl admin_client::`
Expected: compile error `cannot find function call in this scope` (and `parse_reply`, `CallError`).

- [ ] **Step 3: Implement**

Append to `admin_client.rs` (above the test modules):

```rust
use std::time::Duration;

/// Bounded so a wedged daemon runtime cannot hang the CLI (a blocking
/// AF_UNIX connect to a full backlog hangs on Linux).
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// An admin status document carries the full ConfigView and up to 256 shards;
/// 4 MiB is far above that and far below anything that would hurt.
pub(crate) const MAX_REPLY_BYTES: u64 = 4 * 1024 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) enum CallError {
    /// Nothing usable at the path: absent, refused, permission denied, timed out.
    Unreachable(String),
    /// The socket or the daemon failed a client-side identity check (exit 2).
    Identity(String),
    /// The daemon answered `ok: false`.
    Remote { code: String, message: String, retryable: bool },
    /// The reply could not be parsed; it is never trusted.
    Protocol(String),
}

#[derive(Debug)]
pub(crate) struct AdminReply {
    pub instance: Value,
    pub result: Value,
}

/// `daemon pid 4242, wab_dir /var/lib/weir/wab, socket /run/weir/weir.sock`.
pub(crate) fn instance_line(instance: &Value) -> String {
    let s = |k: &str| instance.get(k).map_or_else(|| "?".to_string(), |v| v.as_str().map_or_else(|| v.to_string(), str::to_string));
    format!("daemon pid {}, wab_dir {}, socket {}", s("pid"), s("wab_dir"), s("socket_path"))
}

#[cfg(not(unix))]
pub(crate) fn call(_t: &AdminTarget, _verb: &str, _args: Value, _d: Duration) -> Result<AdminReply, CallError> {
    Err(CallError::Unreachable("the admin socket is Unix-only".to_string()))
}

#[cfg(unix)]
pub(crate) fn call(
    target: &AdminTarget,
    verb: &str,
    args: Value,
    reply_timeout: Duration,
) -> Result<AdminReply, CallError> {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let sock = &target.socket;
    // 1. lstat: absent is "not running here", anything else must be our socket.
    let meta = match std::fs::symlink_metadata(sock) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(CallError::Unreachable(format!("no admin socket at {}", sock.display())));
        }
        Err(e) => return Err(CallError::Unreachable(format!("stat {}: {e}", sock.display()))),
    };
    let parent = sock.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let parent_mode = std::fs::metadata(parent)
        .map_err(|e| CallError::Identity(format!("stat {}: {e}", parent.display())))?
        .mode();
    check_socket_modes(meta.file_type().is_socket(), meta.mode(), parent_mode)
        .map_err(|why| CallError::Identity(format!("{}: {why}", sock.display())))?;

    // 2. What the daemon must prove: the wab_dir inode, and who may serve it.
    let (expected, wab_owner) = match &target.wab_dir {
        None => (None, None),
        Some(d) => match std::fs::metadata(d) {
            Ok(m) => (Some((m.dev(), m.ino())), Some(m.uid())),
            Err(e) if target.wab_dir_strict => {
                return Err(CallError::Identity(format!(
                    "cannot stat --wab-dir {} to check the daemon's identity: {e}",
                    d.display()
                )));
            }
            Err(_) => (None, None),
        },
    };

    // 3. Bounded connect, 4. peer uid.
    let stream = connect_with_timeout(sock, CONNECT_TIMEOUT)?;
    let peer = peer_uid(&stream)
        .map_err(|e| CallError::Identity(format!("peer credentials unavailable: {e}")))?;
    // SAFETY: geteuid has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    if !peer_uid_allowed(peer, euid, wab_owner) {
        return Err(CallError::Identity(format!(
            "{} is served by uid {peer}, which is not weir-ctl's uid {euid}, the wab_dir \
             owner, or root",
            sock.display()
        )));
    }

    // 5. One request line.
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .and_then(|()| stream.set_read_timeout(Some(reply_timeout)))
        .map_err(|e| CallError::Unreachable(format!("set timeout: {e}")))?;
    let req = serde_json::json!({"v": 1, "id": 1, "verb": verb, "args": args});
    let mut line = req.to_string();
    line.push('\n');
    (&stream)
        .write_all(line.as_bytes())
        .map_err(|e| CallError::Unreachable(format!("send {verb}: {e}")))?;

    // 6. One bounded reply line.
    let mut reader = BufReader::new((&stream).take(MAX_REPLY_BYTES + 1));
    let mut reply = String::new();
    let n = reader
        .read_line(&mut reply)
        .map_err(|e| CallError::Unreachable(format!("read the {verb} reply: {e}")))?;
    if n == 0 {
        return Err(CallError::Unreachable(format!(
            "the daemon closed the admin connection without answering {verb}"
        )));
    }
    if !reply.ends_with('\n') {
        return Err(CallError::Protocol(format!(
            "the {verb} reply is not one line under {MAX_REPLY_BYTES} bytes"
        )));
    }
    parse_reply(&reply, expected)
}

pub(crate) fn parse_reply(line: &str, expected: Option<(u64, u64)>) -> Result<AdminReply, CallError> {
    let v: Value = serde_json::from_str(line.trim_end())
        .map_err(|e| CallError::Protocol(format!("the reply is not JSON: {e}")))?;
    if v.get("v").and_then(Value::as_u64) != Some(1) {
        return Err(CallError::Protocol("the reply is not admin protocol v1".to_string()));
    }
    let ok = v
        .get("ok")
        .and_then(Value::as_bool)
        .ok_or_else(|| CallError::Protocol("the reply has no ok flag".to_string()))?;
    let remote = |v: &Value| {
        let e = v.get("error").cloned().unwrap_or(Value::Null);
        CallError::Remote {
            code: e.get("code").and_then(Value::as_str).unwrap_or("unknown").to_string(),
            message: e.get("message").and_then(Value::as_str).unwrap_or("").to_string(),
            retryable: e.get("retryable").and_then(Value::as_bool).unwrap_or(false),
        }
    };
    match v.get("instance") {
        Some(inst) if inst.is_object() => {
            check_instance(inst, expected).map_err(CallError::Identity)?;
            if ok {
                Ok(AdminReply {
                    instance: inst.clone(),
                    result: v.get("result").cloned().unwrap_or(Value::Null),
                })
            } else {
                Err(remote(&v))
            }
        }
        // `forbidden` / `busy` can precede identification: report the code only.
        _ if !ok => Err(remote(&v)),
        _ => Err(CallError::Protocol("the reply carries no instance".to_string())),
    }
}

#[cfg(unix)]
fn connect_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<std::os::unix::net::UnixStream, CallError> {
    let (tx, rx) = std::sync::mpsc::channel();
    let p = path.to_path_buf();
    // A thread, because std has no UnixStream::connect_timeout. On timeout the
    // thread is abandoned; the CLI exits soon after.
    std::thread::spawn(move || {
        let _ = tx.send(std::os::unix::net::UnixStream::connect(&p));
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(CallError::Unreachable(match e.kind() {
            // On macOS and the BSDs a live listener with a full accept backlog
            // also refuses (weir-server socket/mod.rs:801-821), so only Linux
            // may call a refusal stale. Either way it is "nothing answered".
            std::io::ErrorKind::ConnectionRefused => format!(
                "{}: connection refused ({})",
                path.display(),
                if cfg!(target_os = "linux") {
                    "nothing is listening: a stale socket"
                } else {
                    "nothing is listening, or its accept backlog is full"
                }
            ),
            std::io::ErrorKind::PermissionDenied => format!(
                "{}: permission denied (run weir-ctl as the daemon's user or root)",
                path.display()
            ),
            _ => format!("connect {}: {e}", path.display()),
        })),
        Err(_) => Err(CallError::Unreachable(format!(
            "connect {}: no answer within {}s",
            path.display(),
            timeout.as_secs()
        ))),
    }
}

/// Mirrors weir-server's socket/peer.rs (not reachable from here: pub(crate)
/// in another crate). Fails closed where there are no peer credentials.
#[cfg(target_os = "linux")]
fn peer_uid(s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    use std::os::unix::io::AsRawFd;
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: cred/len point at a correctly sized ucred for SO_PEERCRED.
    let ret = unsafe {
        libc::getsockopt(
            s.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(cred.uid)
}

#[cfg(target_os = "macos")]
fn peer_uid(s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    use std::os::unix::io::AsRawFd;
    let (mut uid, mut gid): (libc::uid_t, libc::gid_t) = (0, 0);
    // SAFETY: both out-pointers are valid for the call.
    if unsafe { libc::getpeereid(s.as_raw_fd(), &mut uid, &mut gid) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(uid)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn peer_uid(_s: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "peer credentials are not implemented on this platform",
    ))
}
```

Add the `libc` target dependency to `Cargo.toml` as described under **Files**.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl admin_client:: && cargo test -p weir-ctl weir_ctl_never_takes_the_wab_dir_lock`
Expected: all `call_tests` and the Task 7.8 tests pass, and the lock guard still passes (no lock calls in `admin_client.rs`).

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src crates/weir-ctl/Cargo.toml Cargo.lock && git commit -m "feat(ctl): admin socket client that authenticates the daemon before trusting it"` (`Cargo.lock` changes only in weir-ctl's dependency list; the `chaos/` and `fuzz/` lockfiles are untouched, since neither depends on weir-ctl.)

---

### Task 7.10: weir-ctl status documents from /metrics (+disk) and from disk alone

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 sources 3 and 4.
- If `/metrics` carries `weir_condition_active` (a 4.1 daemon), weir-ctl reads it with `weir_ready`.
- Otherwise it runs `evidence_from_metrics`, merges in disk evidence and runs the same `evaluate` and `readiness` locally.
- Disk alone gives an offline document that leads with `daemon_unreachable`.

Every `weir-status` call this PR makes sits in the functions of this task. A PR 1 rename is fixed here and nowhere else.

**Exit code when only the disk answered** (README decision Q9, binding): exit 2 only when no daemon answered and there are no offline facts (`unreachable_doc`). When disk facts are printed, the exit comes from evaluating the offline document: its `verdict.ready` is false (`daemon_unreachable`), so the exit is 1 (`StatusExit::NotReady`). The readiness script asks `/readyz` first (PR 8 Task 8.4), so a dead daemon is still DEAD there.

**Files:**
- Modify: `crates/weir-ctl/src/status.rs` (the shell from Task 7.3)
- Test: `crates/weir-ctl/src/status.rs` `mod tests`

**Interfaces:**
- Consumes:
  - `weir_status::{SCHEMA, Evidence, Source, BacklogEvidence, Thresholds, evaluate, readiness}`
  - `weir_status::exposition::evidence_from_metrics`
  - `weir_status::active_conditions_from_metrics(body: &str) -> Option<Vec<(ConditionCode, Severity)>>` (PR 1 Task 1.9, the `weir_condition_active` reader for spec §14 step 3; unknown codes read as `unknown`, spec §17)
  - `weir_status::exit::StatusExit` (Task 7.1)
  - `weir_status::offline::{daemon_unreachable, recovery_pending, NoLatches}` (Task 7.2)
  - `crate::offline::DiskFacts` (Task 7.7)
  - `crate::get_metric` (main.rs)
- Produces:
  - `pub(crate) fn conditions_from_metrics(body: &str) -> Option<(bool, Vec<serde_json::Value>)>`
  - `pub(crate) fn metrics_doc(body: &str, disk: Option<&DiskFacts>, now_ms: u64) -> (serde_json::Value, StatusExit)`
  - `pub(crate) fn offline_doc(f: &DiskFacts, now_ms: u64) -> (serde_json::Value, StatusExit)` (the exit is `StatusExit::from_verdict(false, &conditions)`, i.e. `NotReady`, 1; README Q9)
  - `pub(crate) fn unreachable_doc(now_ms: u64) -> serde_json::Value`
  - `fn disk_backlog_evidence(f: &DiskFacts, now_ms: u64) -> weir_status::BacklogEvidence`
  - `fn backlog_json(f: &DiskFacts, now_ms: u64) -> serde_json::Value`
  - `fn shards_json(f: &DiskFacts, now_ms: u64) -> serde_json::Value`
  - The offline document's top-level keys, pinned: `schema, view ("offline"), generated_at_ms, wab_dir, verdict, conditions, unevaluated, backlog, shards`
  - The metrics document's top-level keys, pinned: `schema, view ("metrics"), generated_at_ms, verdict, conditions, unevaluated, backlog, shards`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::offline::{DiskFacts, ShardDisk};
    use serde_json::json;

    const NOW: u64 = 1_700_000_100_000;

    fn facts(undelivered: u64, active: u64) -> DiskFacts {
        DiskFacts {
            wab_dir: "/var/lib/weir/wab".into(),
            shards: vec![ShardDisk {
                name: "shard_00".into(),
                id: Some(0),
                undelivered_segments: undelivered,
                undelivered_records: undelivered * 10,
                oldest_undelivered_created_ns: (undelivered > 0).then_some(1_700_000_000_000_000_000),
                active_segments: active,
                ..ShardDisk::default()
            }],
            scanned_at_ms: NOW,
            ..DiskFacts::default()
        }
    }

    fn keys(v: &serde_json::Value) -> Vec<String> {
        let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    #[test]
    fn offline_doc_shape_and_lead_condition() {
        let (v, exit) = offline_doc(&facts(3, 1), NOW);
        // README Q9: disk facts printed, so the exit comes from evaluating
        // them: not ready (daemon_unreachable), exit 1, never 2.
        assert_eq!(exit, StatusExit::NotReady);
        assert_eq!(
            keys(&v),
            ["backlog", "conditions", "generated_at_ms", "schema", "shards", "unevaluated", "verdict", "view", "wab_dir"]
        );
        assert_eq!(v["schema"], weir_status::SCHEMA);
        assert_eq!(v["view"], "offline");
        assert_eq!(v["verdict"]["ready"], false);
        assert_eq!(v["verdict"]["readiness_failing"], json!(["daemon_unreachable"]));
        assert_eq!(v["conditions"][0]["code"], "daemon_unreachable");
        assert_eq!(v["conditions"][1]["code"], "recovery_pending");
        assert_eq!(v["backlog"]["undelivered_segments"], 3);
        assert_eq!(v["backlog"]["undelivered_records"], 30);
        assert!(v["backlog"]["dead_letter_max_bytes"].is_null(), "offline does not know the cap");
        assert_eq!(v["shards"][0]["shard"], "shard_00");
        // Nothing pending: no recovery_pending.
        let (empty, exit) = offline_doc(&facts(0, 0), NOW);
        assert_eq!(exit, StatusExit::NotReady);
        assert!(!empty["conditions"].as_array().unwrap().iter().any(|c| c["code"] == "recovery_pending"));
    }

    #[test]
    fn condition_series_on_metrics_are_read_with_weir_ready() {
        let body = "\
weir_ready 1
weir_condition_active{code=\"shard_offline\",severity=\"critical\"} 1
weir_condition_active{code=\"drain_paused\",severity=\"warning\"} 0
";
        let (ready, conds) = conditions_from_metrics(body).unwrap();
        assert!(ready);
        assert_eq!(conds.len(), 1, "only series at 1 are active");
        assert_eq!(conds[0]["code"], "shard_offline");
        assert_eq!(conds[0]["severity"], "critical");
        let (doc, exit) = metrics_doc(body, None, NOW);
        assert_eq!(exit, StatusExit::ReadyWithCritical);
        assert_eq!(doc["view"], "metrics");
        assert_eq!(
            keys(&doc),
            ["backlog", "conditions", "generated_at_ms", "schema", "shards", "unevaluated", "verdict", "view"]
        );
        let (_, exit) = metrics_doc(&body.replace("weir_ready 1", "weir_ready 0"), None, NOW);
        assert_eq!(exit, StatusExit::NotReady);
        assert!(conditions_from_metrics("weir_queue_depth 1\n").is_none(), "4.0.x: no condition series");
    }

    #[test]
    fn a_pre_4_1_exposition_is_evaluated_locally_with_parity() {
        // spec §14 parity, on the weir-ctl side: the exit agrees with
        // readiness() over the same Evidence.
        let bodies = [
            "weir_records_accepted_total{tier=\"durable\"} 5\nweir_sink_health{state=\"healthy\"} 1\nweir_sink_info{sink_type=\"http\"} 1\n",
            "weir_records_accepted_total{tier=\"durable\"} 5\nweir_sink_info{sink_type=\"noop\"} 1\nweir_wab_cap_rejections_total 3\n",
        ];
        for body in bodies {
            let e = weir_status::exposition::evidence_from_metrics(body, NOW);
            let ok = weir_status::readiness(&e).ok;
            let (doc, exit) = metrics_doc(body, None, NOW);
            assert_eq!(doc["verdict"]["ready"], ok, "{body}");
            assert_eq!(matches!(exit, StatusExit::Ready | StatusExit::ReadyWithCritical), ok, "{body}");
        }
    }

    #[test]
    fn disk_fills_the_backlog_an_old_daemon_cannot_report() {
        let body = "weir_records_accepted_total{tier=\"durable\"} 5\n";
        let (doc, _) = metrics_doc(body, Some(&facts(2, 0)), NOW);
        assert_eq!(doc["backlog"]["undelivered_segments"], 2);
        let (doc, _) = metrics_doc(body, None, NOW);
        assert!(doc["backlog"].is_null());
    }

    #[test]
    fn unreachable_doc_without_any_facts() {
        let v = unreachable_doc(NOW);
        assert_eq!(v["view"], "offline");
        assert_eq!(v["conditions"][0]["code"], "daemon_unreachable");
        assert_eq!(v["verdict"]["ready"], false);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl status::`
Expected: compile error `cannot find function offline_doc in this scope` (and `metrics_doc`, `conditions_from_metrics`, `unreachable_doc`).

- [ ] **Step 3: Implement**

`status.rs` (replacing the shell line):

```rust
//! `weir-ctl status` (spec §14): the daemon's own status document when a
//! daemon answers, otherwise the same evaluator run locally over /metrics and
//! disk. One exit-code table (weir_status::exit) for every source.

use serde_json::{Value, json};
use weir_status::exit::StatusExit;
use weir_status::offline::{NoLatches, daemon_unreachable, recovery_pending};
use weir_status::{BacklogEvidence, Evidence, Source, Thresholds, evaluate, readiness};

use crate::offline::DiskFacts;

/// `weir_condition_active{code,severity}` at 1, plus `weir_ready`. `None`
/// when the exposition has no condition series (a 4.0.x daemon). The series
/// are read by PR 1's `active_conditions_from_metrics`, so weir-ctl has no
/// second parser to drift, and newer codes read as `unknown` (spec §17).
pub(crate) fn conditions_from_metrics(body: &str) -> Option<(bool, Vec<Value>)> {
    let active = weir_status::active_conditions_from_metrics(body)?;
    let ready = crate::get_metric(body, "weir_ready")? == 1.0;
    let conds = active
        .into_iter()
        .map(|(code, severity)| {
            json!({
                "code": code, "severity": severity,
                "summary": "reported active by weir_condition_active (no detail over /metrics)",
                "runbook": format!("docs/monitoring.md#cond-{}", code.as_str()),
            })
        })
        .collect();
    Some((ready, conds))
}

pub(crate) fn metrics_doc(body: &str, disk: Option<&DiskFacts>, now_ms: u64) -> (Value, StatusExit) {
    let backlog = disk.map(|f| backlog_json(f, now_ms));
    let shards = disk.map(|f| shards_json(f, now_ms));
    if let Some((ready, conditions)) = conditions_from_metrics(body) {
        let any_critical = conditions.iter().any(|c| c["severity"] == "critical");
        let doc = json!({
            "schema": weir_status::SCHEMA, "view": "metrics", "generated_at_ms": now_ms,
            "verdict": {"ready": ready, "live": null, "startup_complete": null, "readiness_failing": null},
            "conditions": conditions, "unevaluated": [],
            "backlog": backlog, "shards": shards,
        });
        return (doc, StatusExit::from_parts(ready, any_critical));
    }
    let mut e = weir_status::exposition::evidence_from_metrics(body, now_ms);
    if e.backlog.is_none()
        && let Some(f) = disk
    {
        e.backlog = Some(disk_backlog_evidence(f, now_ms));
    }
    let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
    let r = readiness(&e);
    let exit = StatusExit::from_verdict(r.ok, &ev.conditions);
    let doc = json!({
        "schema": weir_status::SCHEMA, "view": "metrics", "generated_at_ms": now_ms,
        "verdict": {"ready": r.ok, "live": null, "startup_complete": null, "readiness_failing": r.failing},
        "conditions": ev.conditions, "unevaluated": ev.unevaluated,
        "backlog": backlog, "shards": shards,
    });
    (doc, exit)
}

/// No daemon answered but the disk did. The exit comes from evaluating the
/// offline facts (spec §14, README Q9): `verdict.ready` is false
/// (`daemon_unreachable`), so it is `StatusExit::NotReady` (1), never 2.
pub(crate) fn offline_doc(f: &DiskFacts, now_ms: u64) -> (Value, StatusExit) {
    let mut e = Evidence::empty(now_ms, Source::Disk);
    e.backlog = Some(disk_backlog_evidence(f, now_ms));
    let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
    let mut conditions = vec![daemon_unreachable(now_ms)];
    conditions.extend(recovery_pending(f.undelivered_segments(), f.active_segments()));
    conditions.extend(ev.conditions);
    let exit = StatusExit::from_verdict(false, &conditions);
    let doc = json!({
        "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
        "wab_dir": f.wab_dir.display().to_string(),
        "verdict": {"ready": false, "live": null, "startup_complete": null,
                    "readiness_failing": ["daemon_unreachable"]},
        "conditions": conditions, "unevaluated": ev.unevaluated,
        "backlog": backlog_json(f, now_ms), "shards": shards_json(f, now_ms),
    });
    (doc, exit)
}

/// No daemon answered and there are no offline facts: the only exit-2 document
/// (README Q9).
pub(crate) fn unreachable_doc(now_ms: u64) -> Value {
    json!({
        "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
        "verdict": {"ready": false, "live": null, "startup_complete": null,
                    "readiness_failing": ["daemon_unreachable"]},
        "conditions": [daemon_unreachable(now_ms)], "unevaluated": [],
    })
}

fn disk_backlog_evidence(f: &DiskFacts, now_ms: u64) -> BacklogEvidence {
    BacklogEvidence {
        undelivered_segments: f.undelivered_segments(),
        undelivered_records: f.undelivered_records(),
        oldest_undelivered_age_secs: f.oldest_undelivered_age_secs(now_ms),
        dead_letter_bytes: f.dead_letter_bytes,
        dead_letter_max_bytes: None, // offline does not know the config
        quarantine_bytes: f.quarantine_bytes,
        quarantine_files: f.quarantine_files,
        scanned_at_ms: f.scanned_at_ms,
    }
}

/// The spec §9.1 `backlog` keys, plus the additive `unreadable_segments`,
/// `last_delivery_ms` and `dead_letter_files` that only disk can supply.
fn backlog_json(f: &DiskFacts, now_ms: u64) -> Value {
    json!({
        "undelivered_segments": f.undelivered_segments(),
        "undelivered_records": f.undelivered_records(),
        "unreadable_segments": f.unreadable_segments(),
        "oldest_undelivered_age_secs": f.oldest_undelivered_age_secs(now_ms),
        "last_delivery_ms": f.last_delivery_ns().map(|n| (n.max(0) / 1_000_000) as u64),
        "dead_letter_bytes": f.dead_letter_bytes,
        "dead_letter_files": f.dead_letter_files,
        "dead_letter_max_bytes": null,
        "quarantine_bytes": f.quarantine_bytes,
        "quarantine_files": f.quarantine_files,
        "scanned_at_ms": f.scanned_at_ms,
    })
}

fn shards_json(f: &DiskFacts, now_ms: u64) -> Value {
    let age = |ns: Option<i64>| {
        ns.map(|c| (((now_ms as i128) * 1_000_000 - c as i128).max(0) as f64) / 1e9)
    };
    Value::Array(
        f.shards
            .iter()
            .map(|s| {
                json!({
                    "shard": s.name, "id": s.id,
                    "undelivered_segments": s.undelivered_segments,
                    "undelivered_records": s.undelivered_records,
                    "unreadable_segments": s.unreadable_segments,
                    "active_segments": s.active_segments,
                    "active_segment_age_secs": age(s.oldest_active_created_ns),
                })
            })
            .collect(),
    )
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl status::`
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): status documents from metrics and disk through the shared evaluator"`

---

### Task 7.11: weir-ctl status source chain: first success wins, identity mismatch is terminal

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 auto mode tries, in order: the admin socket (after the identity checks), then `GET /status` (accepted only as JSON with `schema == weir.status/v1`), then `/metrics` plus disk, then disk alone. Each failed source is recorded in `sources_tried` with its reason, and `--json` adds the top-level keys `source` and `sources_tried`.

An identity mismatch stops the chain with exit 2. Quietly falling back to `/status` would hide the impersonation the check exists to catch. The chain is written against a `StatusSources` trait so every branch can be tested without a network.

**Files:**
- Modify: `crates/weir-ctl/src/status.rs`
- Test: `crates/weir-ctl/src/status.rs` `mod tests`

**Interfaces:**
- Consumes: Task 7.10 (`metrics_doc`, `offline_doc`, `unreachable_doc`), `weir_status::exit::StatusExit`, `crate::offline::DiskFacts`.
- Produces:
  - `pub(crate) enum SourceMode { Auto, Admin, Http, Metrics, Disk }`, derives `Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum`, so the CLI values are `auto|admin|http|metrics|disk`
  - `pub(crate) enum SourceKind { Admin, Http, Metrics, MetricsAndDisk, Disk, None }`, with `pub(crate) fn label(self) -> &'static str`
  - `pub(crate) struct SourceAttempt { pub source: &'static str, pub ok: bool, pub reason: Option<String> }`
  - `pub(crate) enum SourceFail { Skip(String), Identity(String) }`, derives `Debug, Clone, PartialEq, Eq`
  - `pub(crate) trait StatusSources { fn admin(&self) -> Result<Value, SourceFail>; fn http_status(&self) -> Result<Value, SourceFail>; fn metrics(&self) -> Result<String, SourceFail>; fn disk(&self) -> Result<DiskFacts, SourceFail>; }`
  - `pub(crate) struct StatusOutcome { pub doc: Value, pub source: SourceKind, pub tried: Vec<SourceAttempt>, pub exit: StatusExit }`
  - `pub(crate) fn daemon_doc_exit(doc: &Value) -> Result<StatusExit, String>`
  - `pub(crate) fn collect_status(src: &dyn StatusSources, mode: SourceMode, now_ms: u64) -> StatusOutcome`
  - `--json` contract: the source document (any `view`) plus `"source": "admin"|"http"|"metrics"|"metrics+disk"|"disk"|"none"` and `"sources_tried": [{"source","ok","reason"}]`. After an identity mismatch, it is a document with an `"identity_error"` key and nothing from the refused socket.

- [ ] **Step 1: Write the failing test**

Append to `status.rs` `mod tests`:

```rust
    use std::cell::RefCell;

    struct Fake {
        admin: Result<serde_json::Value, SourceFail>,
        http: Result<serde_json::Value, SourceFail>,
        metrics: Result<String, SourceFail>,
        disk: Result<DiskFacts, SourceFail>,
        calls: RefCell<Vec<&'static str>>,
    }

    impl Fake {
        fn down() -> Fake {
            let skip = || SourceFail::Skip("down".into());
            Fake { admin: Err(skip()), http: Err(skip()), metrics: Err(skip()), disk: Err(skip()), calls: RefCell::new(Vec::new()) }
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls.borrow().clone()
        }
    }

    impl StatusSources for Fake {
        fn admin(&self) -> Result<serde_json::Value, SourceFail> {
            self.calls.borrow_mut().push("admin");
            self.admin.clone()
        }
        fn http_status(&self) -> Result<serde_json::Value, SourceFail> {
            self.calls.borrow_mut().push("http");
            self.http.clone()
        }
        fn metrics(&self) -> Result<String, SourceFail> {
            self.calls.borrow_mut().push("metrics");
            self.metrics.clone()
        }
        fn disk(&self) -> Result<DiskFacts, SourceFail> {
            self.calls.borrow_mut().push("disk");
            self.disk.clone()
        }
    }

    fn daemon_doc(view: &str, ready: bool, severity: &str) -> serde_json::Value {
        json!({"schema": weir_status::SCHEMA, "view": view,
               "verdict": {"ready": ready, "readiness_failing": if ready { json!([]) } else { json!(["starting"]) }},
               "conditions": [{"code": "noop_sink", "severity": severity, "summary": "s"}],
               "unevaluated": []})
    }

    fn tried(o: &StatusOutcome) -> Vec<&'static str> {
        o.tried.iter().map(|a| a.source).collect()
    }

    #[test]
    fn auto_prefers_the_admin_socket_and_adds_source_keys_only() {
        let f = Fake { admin: Ok(daemon_doc("admin", true, "warning")), ..Fake::down() };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Admin);
        assert_eq!(o.exit, StatusExit::Ready);
        assert_eq!(f.calls(), ["admin"]);
        let mut expected = keys(&daemon_doc("admin", true, "warning"));
        expected.extend(["source".to_string(), "sources_tried".to_string()]);
        expected.sort();
        assert_eq!(keys(&o.doc), expected, "--json is the document plus two keys");
        assert_eq!(o.doc["source"], "admin");
        assert_eq!(o.doc["sources_tried"], json!([{"source": "admin", "ok": true, "reason": null}]));
    }

    #[test]
    fn admin_down_falls_through_to_http_with_the_reason_recorded() {
        let f = Fake {
            admin: Err(SourceFail::Skip("no admin socket at /var/lib/weir/wab.admin.sock".into())),
            http: Ok(daemon_doc("public", false, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Http);
        assert_eq!(o.exit, StatusExit::NotReady);
        assert_eq!(tried(&o), ["admin", "http"]);
        assert!(o.tried[0].reason.as_deref().unwrap().contains("no admin socket"));
    }

    #[test]
    fn an_identity_mismatch_is_terminal_and_exits_2() {
        let f = Fake {
            admin: Err(SourceFail::Identity("the daemon serves wab_dir (dev 1, ino 2)".into())),
            http: Ok(daemon_doc("public", true, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(f.calls(), ["admin"], "no fallback after a failed identity check");
        assert_eq!(o.exit, StatusExit::Unreachable);
        assert_eq!(o.source, SourceKind::None);
        assert!(o.doc["identity_error"].as_str().unwrap().contains("ino 2"));
        assert!(o.tried[0].reason.as_deref().unwrap().starts_with("identity mismatch"));
    }

    #[test]
    fn an_old_daemon_goes_to_metrics_plus_disk() {
        let f = Fake {
            http: Err(SourceFail::Skip("/status is not JSON (application/openmetrics-text); a pre-4.1 daemon".into())),
            metrics: Ok("weir_records_accepted_total{tier=\"durable\"} 5\n".into()),
            disk: Ok(facts(2, 0)),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::MetricsAndDisk);
        assert_eq!(o.doc["source"], "metrics+disk");
        assert_eq!(tried(&o), ["admin", "http", "metrics", "disk"]);
        assert_eq!(o.doc["backlog"]["undelivered_segments"], 2);
    }

    #[test]
    fn disk_only_is_not_ready_with_facts() {
        // README Q9: disk facts printed, so the exit is evaluated from them (1).
        let f = Fake { disk: Ok(facts(1, 1)), ..Fake::down() };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Disk);
        assert_eq!(o.exit, StatusExit::NotReady);
        assert_eq!(o.doc["view"], "offline");
        assert_eq!(tried(&o), ["admin", "http", "metrics", "disk"]);
    }

    #[test]
    fn nothing_at_all_is_unreachable() {
        let o = collect_status(&Fake::down(), SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::None);
        assert_eq!(o.exit, StatusExit::Unreachable);
        assert_eq!(o.doc["source"], "none");
        assert_eq!(o.doc["conditions"][0]["code"], "daemon_unreachable");
    }

    #[test]
    fn a_forced_source_consults_only_that_source() {
        let f = Fake { admin: Ok(daemon_doc("admin", true, "info")), ..Fake::down() };
        let o = collect_status(&f, SourceMode::Http, NOW);
        assert_eq!(f.calls(), ["http"]);
        assert_eq!(o.exit, StatusExit::Unreachable);
        let f = Fake { disk: Ok(facts(0, 0)), ..Fake::down() };
        let o = collect_status(&f, SourceMode::Disk, NOW);
        assert_eq!(f.calls(), ["disk"]);
        assert_eq!(o.source, SourceKind::Disk);
    }

    #[test]
    fn a_daemon_document_must_be_weir_status_v1_with_a_verdict() {
        assert_eq!(daemon_doc_exit(&daemon_doc("admin", true, "critical")), Ok(StatusExit::ReadyWithCritical));
        assert_eq!(daemon_doc_exit(&daemon_doc("admin", false, "critical")), Ok(StatusExit::NotReady));
        assert!(daemon_doc_exit(&json!({"schema": "weir.status/v2", "verdict": {"ready": true}})).is_err());
        assert!(daemon_doc_exit(&json!({"schema": weir_status::SCHEMA})).is_err());
        // A bad admin document falls through instead of being trusted.
        let f = Fake {
            admin: Ok(json!({"hello": 1})),
            http: Ok(daemon_doc("public", true, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Http);
        assert!(o.tried[0].reason.as_deref().unwrap().contains("weir.status/v1"));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl status::`
Expected: compile errors `cannot find trait StatusSources`, `cannot find function collect_status`, `cannot find type SourceMode`.

- [ ] **Step 3: Implement**

Append to `status.rs` (above the tests):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum SourceMode {
    Auto,
    Admin,
    Http,
    Metrics,
    Disk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Admin,
    Http,
    Metrics,
    MetricsAndDisk,
    Disk,
    None,
}

impl SourceKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            SourceKind::Admin => "admin",
            SourceKind::Http => "http",
            SourceKind::Metrics => "metrics",
            SourceKind::MetricsAndDisk => "metrics+disk",
            SourceKind::Disk => "disk",
            SourceKind::None => "none",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceAttempt {
    pub source: &'static str,
    pub ok: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceFail {
    /// Try the next source.
    Skip(String),
    /// Stop: the admin socket failed a client-side identity check.
    Identity(String),
}

pub(crate) trait StatusSources {
    /// The admin `status` result (`AdminStatusDoc`), identity already checked.
    fn admin(&self) -> Result<Value, SourceFail>;
    /// `GET /status` (`PublicStatusDoc`), JSON content type already checked.
    fn http_status(&self) -> Result<Value, SourceFail>;
    /// `GET /metrics` body containing weir series.
    fn metrics(&self) -> Result<String, SourceFail>;
    fn disk(&self) -> Result<DiskFacts, SourceFail>;
}

#[derive(Debug)]
pub(crate) struct StatusOutcome {
    pub doc: Value,
    pub source: SourceKind,
    pub tried: Vec<SourceAttempt>,
    pub exit: StatusExit,
}

/// The exit for a daemon-rendered document: its own `verdict.ready` (the same
/// `readiness` /readyz renders) plus any critical condition.
pub(crate) fn daemon_doc_exit(doc: &Value) -> Result<StatusExit, String> {
    if doc.get("schema").and_then(Value::as_str) != Some(weir_status::SCHEMA) {
        return Err(format!("the answer is not a {} document", weir_status::SCHEMA));
    }
    let ready = doc
        .pointer("/verdict/ready")
        .and_then(Value::as_bool)
        .ok_or_else(|| "the document has no verdict.ready".to_string())?;
    let any_critical = doc
        .get("conditions")
        .and_then(Value::as_array)
        .is_some_and(|cs| cs.iter().any(|c| c["severity"] == "critical"));
    Ok(StatusExit::from_parts(ready, any_critical))
}

fn ok_attempt(source: &'static str) -> SourceAttempt {
    SourceAttempt { source, ok: true, reason: None }
}

fn failed(source: &'static str, why: String) -> SourceAttempt {
    SourceAttempt { source, ok: false, reason: Some(why) }
}

fn finish(mut doc: Value, source: SourceKind, tried: Vec<SourceAttempt>, exit: StatusExit) -> StatusOutcome {
    if let Some(o) = doc.as_object_mut() {
        o.insert("source".into(), json!(source.label()));
        o.insert(
            "sources_tried".into(),
            Value::Array(
                tried
                    .iter()
                    .map(|a| json!({"source": a.source, "ok": a.ok, "reason": a.reason}))
                    .collect(),
            ),
        );
    }
    StatusOutcome { doc, source, tried, exit }
}

pub(crate) fn collect_status(src: &dyn StatusSources, mode: SourceMode, now_ms: u64) -> StatusOutcome {
    let want = |m: SourceMode| mode == SourceMode::Auto || mode == m;
    let mut tried = Vec::new();

    // A daemon-rendered document (admin, then http) is used as-is.
    let daemon_sources: [(SourceMode, SourceKind, &'static str, &dyn Fn() -> Result<Value, SourceFail>); 2] = [
        (SourceMode::Admin, SourceKind::Admin, "admin", &|| src.admin()),
        (SourceMode::Http, SourceKind::Http, "http", &|| src.http_status()),
    ];
    for (m, kind, label, fetch) in daemon_sources {
        if !want(m) {
            continue;
        }
        match fetch() {
            Ok(doc) => match daemon_doc_exit(&doc) {
                Ok(exit) => {
                    tried.push(ok_attempt(label));
                    return finish(doc, kind, tried, exit);
                }
                Err(why) => tried.push(failed(label, why)),
            },
            Err(SourceFail::Identity(why)) => {
                tried.push(failed(label, format!("identity mismatch: {why}")));
                let doc = json!({
                    "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
                    "verdict": {"ready": false, "live": null, "startup_complete": null,
                                "readiness_failing": null},
                    "conditions": [], "unevaluated": [],
                    "identity_error": why,
                });
                return finish(doc, SourceKind::None, tried, StatusExit::Unreachable);
            }
            Err(SourceFail::Skip(why)) => tried.push(failed(label, why)),
        }
    }

    if want(SourceMode::Metrics) {
        match src.metrics() {
            Ok(body) => {
                tried.push(ok_attempt("metrics"));
                let disk = match src.disk() {
                    Ok(f) => {
                        tried.push(ok_attempt("disk"));
                        Some(f)
                    }
                    Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => {
                        tried.push(failed("disk", w));
                        None
                    }
                };
                let kind = if disk.is_some() { SourceKind::MetricsAndDisk } else { SourceKind::Metrics };
                let (doc, exit) = metrics_doc(&body, disk.as_ref(), now_ms);
                return finish(doc, kind, tried, exit);
            }
            Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => tried.push(failed("metrics", w)),
        }
    }

    if want(SourceMode::Disk) {
        match src.disk() {
            Ok(f) => {
                tried.push(ok_attempt("disk"));
                // README Q9: disk facts are printed, so the exit is evaluated
                // from them (not ready, 1), not "unreachable" (2).
                let (doc, exit) = offline_doc(&f, now_ms);
                return finish(doc, SourceKind::Disk, tried, exit);
            }
            Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => tried.push(failed("disk", w)),
        }
    }

    // No daemon answered and there are no offline facts: the exit-2 case.
    finish(unreachable_doc(now_ms), SourceKind::None, tried, StatusExit::Unreachable)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl status::`
Expected: the 8 new tests and the 5 Task 7.10 tests pass.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): status tries admin, /status, metrics+disk, disk; identity mismatch is final"`

---

### Task 7.12: `weir-ctl status`: CLI, the real sources and human output

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 human output, in order:
1. A verdict line: `READY` / `NOT READY: <codes>` (a disk-only document reads `NOT READY: daemon_unreachable`, README Q9) / `UNREACHABLE` (nothing answered, no offline facts), or `IDENTITY MISMATCH: …`.
2. Conditions sorted by severity.
3. The drain, sink, backlog and shard blocks.
4. An `instance:` line (admin source only).
5. `unevaluated:` codes, which are never shown as healthy.
6. A `source:` footer.

The admin socket defaults to `WEIR_ADMIN_SOCKET_PATH`, then to the path derived from `--wab-dir` / `WEIR_WAB_DIR`, then to `/var/lib/weir/wab.admin.sock` (Task 7.8).

**Files:**
- Modify: `crates/weir-ctl/src/status.rs` (add `RealSources`, `render_human`, `status_run`, `cmd_status`, `now_ms`)
- Modify: `crates/weir-ctl/src/main.rs`:
  - add `Command::Status { admin_socket, addr, wab_dir, source }`
  - add the match arm `Command::Status { .. } => return status::cmd_status(..)`
- Test: `crates/weir-ctl/src/status.rs` `mod tests`; `crates/weir-ctl/src/main.rs` `mod tests` (CLI parse)

**Interfaces:**
- Consumes:
  - `crate::admin_client::{AdminTarget, resolve_admin_target, call, CallError, instance_line, DEFAULT_WAB_DIR}`
  - `crate::http::{http_get, is_json}`
  - `crate::{scrape, has_weir_metrics, write_json_to, fmt_bytes}`
  - `crate::offline::scan_disk`
- Produces:
  - `pub(crate) const STATUS_REPLY_TIMEOUT: Duration` (5 s)
  - `pub(crate) struct RealSources { pub admin: AdminTarget, pub addr: String, pub wab_dir: PathBuf }`, with `impl StatusSources for RealSources`
  - `pub(crate) fn now_ms() -> u64`
  - `pub(crate) fn render_human(o: &StatusOutcome) -> Vec<String>`
  - `pub(crate) fn status_run(src: &dyn StatusSources, mode: SourceMode, json: bool, now_ms: u64, out: &mut impl std::io::Write) -> u8`
  - `pub(crate) fn cmd_status(admin_socket: Option<&Path>, addr: &str, wab_dir: Option<&Path>, mode: SourceMode, json: bool) -> std::process::ExitCode`
  - CLI: `weir-ctl status [--admin-socket P] [--addr H:P] [--wab-dir D] [--source auto|admin|http|metrics|disk] [--json]`

- [ ] **Step 1: Write the failing test**

In `status.rs` `mod tests`:

```rust
    fn outcome(doc: serde_json::Value, source: SourceKind) -> StatusOutcome {
        let exit = daemon_doc_exit(&doc).unwrap_or(StatusExit::Unreachable);
        finish(doc, source, vec![ok_attempt(source.label())], exit)
    }

    #[test]
    fn human_verdict_lines() {
        let ready = render_human(&outcome(daemon_doc("admin", true, "info"), SourceKind::Admin));
        assert_eq!(ready[0], "READY");
        let not = render_human(&outcome(daemon_doc("public", false, "info"), SourceKind::Http));
        assert_eq!(not[0], "NOT READY: starting");
        // Disk facts printed: the offline document is evaluated (README Q9).
        let off = render_human(&outcome(offline_doc(&facts(1, 0), NOW).0, SourceKind::Disk));
        assert_eq!(off[0], "NOT READY: daemon_unreachable");
        // Nothing answered at all: the only UNREACHABLE verdict.
        let none = render_human(&finish(unreachable_doc(NOW), SourceKind::None, vec![], StatusExit::Unreachable));
        assert_eq!(none[0], "UNREACHABLE");
        let f = Fake { admin: Err(SourceFail::Identity("ino 9".into())), ..Fake::down() };
        let id = render_human(&collect_status(&f, SourceMode::Auto, NOW));
        assert!(id[0].starts_with("IDENTITY MISMATCH: ino 9"), "{id:?}");
    }

    #[test]
    fn human_conditions_sorted_by_severity_and_unevaluated_never_healthy() {
        let mut doc = daemon_doc("admin", true, "info");
        doc["conditions"] = json!([
            {"code": "sink_degraded", "severity": "info", "summary": "i"},
            {"code": "shard_offline", "severity": "critical", "summary": "c"},
            {"code": "drain_paused", "severity": "warning", "summary": "w"},
        ]);
        doc["unevaluated"] = json!(["wab_scan_stale", "delivery_lagging"]);
        // PR 1's AdminStatusDoc shape: `pid` is top-level, `instance` has none.
        doc["pid"] = json!(7);
        doc["instance"] = json!({"wab_dir": "/var/lib/weir/wab", "socket_path": "/run/weir/weir.sock"});
        let lines = render_human(&outcome(doc.clone(), SourceKind::Admin));
        let pos = |needle: &str| lines.iter().position(|l| l.contains(needle)).unwrap_or_else(|| panic!("{needle} missing: {lines:?}"));
        assert!(pos("shard_offline") < pos("drain_paused") && pos("drain_paused") < pos("sink_degraded"));
        let unev = &lines[pos("unevaluated")];
        assert!(unev.contains("wab_scan_stale") && unev.contains("not known to be healthy"), "{unev}");
        assert!(lines.iter().any(|l| l.starts_with("instance: daemon pid 7")));
        assert!(lines.last().unwrap().starts_with("source: admin"));
        // The public view has no instance line.
        let pub_lines = render_human(&outcome(doc, SourceKind::Http));
        assert!(!pub_lines.iter().any(|l| l.starts_with("instance:")));
    }

    #[test]
    fn status_run_offline_end_to_end_against_a_temp_wab_dir() {
        let wab = std::env::temp_dir().join(format!("weir_ctl_status_e2e_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&wab);
        std::fs::create_dir_all(wab.join("shard_00")).unwrap();
        // A closed port and an absent admin socket: only the disk answers.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let src = RealSources {
            admin: crate::admin_client::resolve_admin_target(Some(&wab.join("nope.sock")), Some(&wab)),
            addr: format!("127.0.0.1:{port}"),
            wab_dir: wab.clone(),
        };
        let mut out = Vec::new();
        let code = status_run(&src, SourceMode::Auto, true, NOW, &mut out);
        // README Q9: the disk answered, so its facts are evaluated: exit 1, not 2.
        assert_eq!(code, 1);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["source"], "disk");
        assert_eq!(v["sources_tried"].as_array().unwrap().len(), 4);
        std::fs::remove_dir_all(&wab).ok();
    }
```

In `main.rs` `mod tests`:

```rust
    #[test]
    fn status_cli_parses_every_source_and_defaults_to_auto() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["weir-ctl", "status"]).unwrap();
        assert!(matches!(cli.command, Command::Status { source: status::SourceMode::Auto, .. }));
        for (arg, mode) in [
            ("admin", status::SourceMode::Admin),
            ("http", status::SourceMode::Http),
            ("metrics", status::SourceMode::Metrics),
            ("disk", status::SourceMode::Disk),
        ] {
            let cli = Cli::try_parse_from(["weir-ctl", "--json", "status", "--source", arg, "--wab-dir", "/x"]).unwrap();
            assert!(matches!(cli.command, Command::Status { source, .. } if source == mode), "{arg}");
        }
        assert!(Cli::try_parse_from(["weir-ctl", "status", "--source", "carrier-pigeon"]).is_err());
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl status`
Expected: compile errors `cannot find function render_human`, `cannot find struct RealSources`, `no variant named Status`.

- [ ] **Step 3: Implement**

In `status.rs`:

```rust
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use crate::admin_client::{self, AdminTarget, CallError};

pub(crate) const STATUS_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

pub(crate) struct RealSources {
    pub admin: AdminTarget,
    pub addr: String,
    pub wab_dir: PathBuf,
}

impl StatusSources for RealSources {
    fn admin(&self) -> Result<Value, SourceFail> {
        match admin_client::call(&self.admin, "status", json!({}), STATUS_REPLY_TIMEOUT) {
            Ok(r) => {
                let mut doc = r.result;
                if let Some(o) = doc.as_object_mut() {
                    o.entry("instance").or_insert(r.instance);
                }
                Ok(doc)
            }
            Err(CallError::Identity(w)) => Err(SourceFail::Identity(w)),
            Err(CallError::Unreachable(w) | CallError::Protocol(w)) => Err(SourceFail::Skip(w)),
            Err(CallError::Remote { code, message, .. }) => {
                Err(SourceFail::Skip(format!("{code}: {message}")))
            }
        }
    }

    fn http_status(&self) -> Result<Value, SourceFail> {
        let r = crate::http::http_get(&self.addr, "/status").map_err(SourceFail::Skip)?;
        if !crate::http::is_json(r.content_type.as_deref()) {
            return Err(SourceFail::Skip(format!(
                "/status at {} is not JSON ({}); a pre-4.1 daemon",
                self.addr,
                r.content_type.as_deref().unwrap_or("no content type")
            )));
        }
        if r.status != Some(200) {
            return Err(SourceFail::Skip(format!(
                "/status answered HTTP {}",
                r.status.map_or_else(|| "?".to_string(), |s| s.to_string())
            )));
        }
        serde_json::from_str(&r.body)
            .map_err(|e| SourceFail::Skip(format!("/status is not valid JSON: {e}")))
    }

    fn metrics(&self) -> Result<String, SourceFail> {
        let body = crate::scrape(&self.addr).map_err(SourceFail::Skip)?;
        if !crate::has_weir_metrics(&body) {
            return Err(SourceFail::Skip(format!("no weir metrics at {}", self.addr)));
        }
        Ok(body)
    }

    fn disk(&self) -> Result<DiskFacts, SourceFail> {
        crate::offline::scan_disk(&self.wab_dir, now_ms()).map_err(SourceFail::Skip)
    }
}

fn field(v: &Value, k: &str) -> String {
    match v.get(k) {
        None | Some(Value::Null) => "?".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn severity_rank(s: Option<&str>) -> u8 {
    match s {
        Some("critical") => 0,
        Some("warning") => 1,
        Some("info") => 2,
        _ => 3,
    }
}

fn codes(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|c| c.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

pub(crate) fn render_human(o: &StatusOutcome) -> Vec<String> {
    let d = &o.doc;
    let mut out = Vec::new();
    let conds: Vec<&Value> = d.get("conditions").and_then(Value::as_array).map(|a| a.iter().collect()).unwrap_or_default();

    if let Some(why) = d.get("identity_error").and_then(Value::as_str) {
        out.push(format!("IDENTITY MISMATCH: {why}"));
        out.push("refusing to trust the admin socket; nothing it sent was used".to_string());
    } else if o.source == SourceKind::None {
        // Nothing answered and there are no offline facts (README Q9). A
        // disk-only document falls through to its own NOT READY verdict.
        out.push("UNREACHABLE".to_string());
    } else if d.pointer("/verdict/ready").and_then(Value::as_bool) == Some(true) {
        out.push("READY".to_string());
    } else {
        let mut failing = codes(d.pointer("/verdict/readiness_failing"));
        if failing.is_empty() {
            failing = conds.iter().map(|c| field(c, "code")).collect();
        }
        out.push(format!("NOT READY: {}", failing.join(", ")));
    }

    let mut sorted = conds.clone();
    sorted.sort_by_key(|c| severity_rank(c.get("severity").and_then(Value::as_str)));
    if !sorted.is_empty() {
        out.push(String::new());
        for c in sorted {
            out.push(format!("  [{}] {} — {}", field(c, "severity"), field(c, "code"), field(c, "summary")));
        }
        out.push(String::new());
    }

    if let Some(dr) = d.get("drain").filter(|v| v.is_object()) {
        out.push(format!(
            "drain:    state {}  paused {}  pending {}  backlog {}  channel {}/{}",
            field(dr, "state"), field(dr, "paused"), field(dr, "pending_segments"),
            field(dr, "backlog_segments"), field(dr, "channel_depth"), field(dr, "channel_capacity"),
        ));
    }
    if let Some(s) = d.get("sink").filter(|v| v.is_object()) {
        out.push(format!(
            "sink:     {}  health {}  consecutive_failures {}",
            field(s, "type"), field(s, "health"), field(s, "consecutive_failures"),
        ));
    }
    if let Some(b) = d.get("backlog").filter(|v| v.is_object()) {
        let bytes = |k: &str| b.get(k).and_then(Value::as_u64).map_or_else(|| "?".to_string(), crate::fmt_bytes);
        out.push(format!(
            "backlog:  {} undelivered segment(s), {} record(s), oldest {}s, dead-letter {}, quarantine {}",
            field(b, "undelivered_segments"), field(b, "undelivered_records"),
            b.get("oldest_undelivered_age_secs").and_then(Value::as_f64).map_or_else(|| "?".to_string(), |s| format!("{s:.0}")),
            bytes("dead_letter_bytes"), bytes("quarantine_bytes"),
        ));
    }
    for s in d.get("shards").and_then(Value::as_array).into_iter().flatten() {
        let id = s.get("id").filter(|v| !v.is_null()).map_or_else(|| field(s, "shard"), |v| v.to_string());
        let mut parts = vec![format!("shard {id}:")];
        for k in ["state", "reason", "undelivered_segments", "active_segments"] {
            if s.get(k).is_some_and(|v| !v.is_null()) {
                parts.push(format!("{k} {}", field(s, k)));
            }
        }
        out.push(format!("  {}", parts.join("  ")));
    }
    if o.source == SourceKind::Admin
        && let Some(inst) = d.get("instance")
    {
        // PR 1's AdminStatusDoc carries `pid` at the top level and an
        // `InstanceView` without it; fold it in for the instance line.
        let mut inst = inst.clone();
        if let (Some(pid), Some(obj)) = (d.get("pid"), inst.as_object_mut()) {
            obj.entry("pid").or_insert(pid.clone());
        }
        out.push(format!("instance: {}", admin_client::instance_line(&inst)));
    }
    let unev = codes(d.get("unevaluated"));
    if !unev.is_empty() {
        out.push(format!("unevaluated (not known to be healthy): {}", unev.join(", ")));
    }
    let tried: Vec<String> = o
        .tried
        .iter()
        .map(|a| if a.ok { format!("{} ok", a.source) } else { format!("{} failed: {}", a.source, a.reason.as_deref().unwrap_or("?")) })
        .collect();
    out.push(format!("source: {} (tried: {})", o.source.label(), tried.join("; ")));
    out
}

pub(crate) fn status_run(
    src: &dyn StatusSources,
    mode: SourceMode,
    json: bool,
    now_ms: u64,
    out: &mut impl Write,
) -> u8 {
    let o = collect_status(src, mode, now_ms);
    if json {
        crate::write_json_to(out, &o.doc);
    } else {
        for l in render_human(&o) {
            let _ = writeln!(out, "{l}");
        }
    }
    o.exit.code()
}

pub(crate) fn cmd_status(
    admin_socket: Option<&Path>,
    addr: &str,
    wab_dir: Option<&Path>,
    mode: SourceMode,
    json: bool,
) -> ExitCode {
    let src = RealSources {
        admin: admin_client::resolve_admin_target(admin_socket, wab_dir),
        addr: addr.to_string(),
        wab_dir: wab_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(admin_client::DEFAULT_WAB_DIR)),
    };
    ExitCode::from(status_run(&src, mode, json, now_ms(), &mut std::io::stdout()))
}
```

In `main.rs`, add to `enum Command`:

```rust
    /// Can the daemon take ingest, and if not, why: conditions, drain, sink,
    /// backlog, shards. Tries the admin socket, then GET /status, then
    /// /metrics plus disk, then disk alone. Exit: 0 ready, 1 not ready
    /// (including a dead daemon whose wab_dir was read from disk), 2 unreachable
    /// with no offline facts, or identity mismatch, 3 ready with a critical
    /// condition.
    Status {
        /// The daemon's admin socket. Default: derived from --wab-dir
        /// (<parent>/<basename>.admin.sock), then /var/lib/weir/wab.admin.sock.
        #[arg(long, env = "WEIR_ADMIN_SOCKET_PATH")]
        admin_socket: Option<PathBuf>,
        /// host:port of the daemon's metrics listener (serves /status and /metrics).
        #[arg(long, default_value = DEFAULT_METRICS_ADDR)]
        addr: String,
        /// The daemon's wab_dir: the disk facts, the derived admin socket, and
        /// the identity the daemon must prove.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: Option<PathBuf>,
        /// Use only this source instead of the first that answers.
        #[arg(long, value_enum, default_value_t = status::SourceMode::Auto)]
        source: status::SourceMode,
    },
```

And in `main()`'s match, next to the attest-verify early return:

```rust
        Command::Status { admin_socket, addr, wab_dir, source } => {
            // Its own exit table (0/1/2/3), like `attest verify`.
            return status::cmd_status(admin_socket.as_deref(), &addr, wab_dir.as_deref(), source, json);
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl status && cargo test -p weir-ctl cli_definition_is_valid`
Expected: the new tests pass, and `cli_definition_is_valid` (clap's `debug_assert`) passes with the new subcommand.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): weir-ctl status with offline-first evidence and the shared exit table"`

---

### Task 7.13: `weir-ctl reload`

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14: `weir-ctl reload [--admin-socket P] [--json]` exits 0 for applied or noop and 1 for failed. Without a socket it exits 2 with the hint "`kill -HUP <pid>` uses the same code path". The admin `reload` verb calls the safety floor's `Reloader::reload(ReloadTrigger::Admin)` (reload.rs:158), which is the same function SIGHUP reaches through `reload_loop`.

This command also takes `--wab-dir`, for two reasons:
- The admin path derives from it (spec §14's default chain, which `status` uses too).
- The identity check compares against it.

The flag is additive and optional.

Today's report spells success `Ok` (label `ok`, reload.rs:76-84), while spec §5.10 spells it `applied`. weir-ctl accepts both. Any other outcome string exits 1, because weir-ctl cannot vouch for a success it does not recognise.

**Files:**
- Create: `crates/weir-ctl/src/admin_cmds.rs`
- Modify: `crates/weir-ctl/src/main.rs`:
  - add `mod admin_cmds;`
  - add `#[derive(clap::Args)] struct AdminArgs` with `fn target(&self)`
  - add `Command::Reload { admin: AdminArgs }` and its match arm
- Test: `crates/weir-ctl/src/admin_cmds.rs` `mod tests`

**Interfaces:**
- Consumes: `crate::admin_client::{call, AdminTarget, CallError, instance_line, resolve_admin_target}`, `crate::admin_client::test_support` (tests), `crate::{write_json_to, error_json}`.
- Produces:
  - `pub(crate) const RELOAD_REPLY_TIMEOUT: Duration` (15 s)
  - `pub(crate) fn reload_outcome_exit(result: &Value) -> (u8, String)`
  - `pub(crate) fn reload_item_lines(result: &Value) -> Vec<String>`
  - `pub(crate) fn report_call_error(verb: &str, e: &CallError, json: bool, err: &mut impl Write, unreachable_hint: Option<&str>) -> u8`
  - `pub(crate) fn reload_run(target: &AdminTarget, json: bool, out: &mut impl Write, err: &mut impl Write) -> u8`
  - `pub(crate) fn cmd_reload(target: &AdminTarget, json: bool) -> ExitCode`
  - `struct AdminArgs { admin_socket: Option<PathBuf> /* --admin-socket, env WEIR_ADMIN_SOCKET_PATH */, wab_dir: Option<PathBuf> /* --wab-dir, env WEIR_WAB_DIR */ }` with `fn target(&self) -> admin_client::AdminTarget` (main.rs)
  - CLI: `weir-ctl reload [--admin-socket P] [--wab-dir D] [--json]`

- [ ] **Step 1: Write the failing test**

Declare `mod admin_cmds;` in main.rs, so the new file's tests are compiled. Then create `crates/weir-ctl/src/admin_cmds.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    #[test]
    fn reload_outcomes_map_to_the_documented_exits() {
        for (outcome, code) in [("applied", 0), ("ok", 0), ("noop", 0), ("failed", 1), ("exploded", 1)] {
            assert_eq!(reload_outcome_exit(&json!({"outcome": outcome})).0, code, "{outcome}");
        }
        assert_eq!(reload_outcome_exit(&json!({})).0, 1, "a missing outcome is not a success");
    }

    #[test]
    fn reload_items_render_from_either_report_shape() {
        let spec_shape = json!({"outcome": "failed",
            "items": [{"item": "tls_certificates", "outcome": "failed", "detail": "bad key"}],
            "not_reloaded": [{"item": "config_file", "reason_code": "restart_required"}]});
        let l = reload_item_lines(&spec_shape);
        assert!(l.iter().any(|x| x == "tls_certificates: failed (bad key)"), "{l:?}");
        assert!(l.iter().any(|x| x == "config_file: not reloaded (restart_required)"), "{l:?}");
        let floor_shape = json!({"outcome": "noop", "changed": [], "rejected": [["tls", "unreadable"]], "note": "nothing_reloadable"});
        let l = reload_item_lines(&floor_shape);
        assert!(l.iter().any(|x| x == "tls: rejected (unreadable)"), "{l:?}");
        assert!(l.iter().any(|x| x == "note: nothing_reloadable"), "{l:?}");
    }

    #[test]
    fn unreachable_reload_exits_2_with_the_sighup_hint() {
        let t = crate::admin_client::resolve_admin_target(
            Some(Path::new("/nonexistent/weir/wab.admin.sock")),
            None,
        );
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(reload_run(&t, false, &mut out, &mut err), 2);
        let err = String::from_utf8(err).unwrap();
        assert!(err.contains("`kill -HUP <pid>` uses the same code path"), "{err}");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(reload_run(&t, true, &mut out, &mut err), 2);
        let e: serde_json::Value = serde_json::from_slice(&err).unwrap();
        assert!(e["error"].as_str().unwrap().contains("unreachable"), "--json keeps error_json");
    }

    #[cfg(unix)]
    #[test]
    fn reload_against_a_daemon_maps_failed_to_1_and_noop_to_0() {
        use crate::admin_client::test_support::*;
        for (outcome, code) in [("failed", 1u8), ("noop", 0u8)] {
            let dir = private_dir(&format!("rl{code}"));
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true,
                       "result": {"trigger": "admin", "outcome": outcome, "items": []}})
            });
            let t = crate::admin_client::resolve_admin_target(Some(&dir.join("a.sock")), Some(&wab));
            let (mut out, mut err) = (Vec::new(), Vec::new());
            assert_eq!(reload_run(&t, false, &mut out, &mut err), code);
            let out = String::from_utf8(out).unwrap();
            assert!(out.starts_with(&format!("reload: {outcome}")), "{out}");
            assert_eq!(h.join().unwrap()["verb"], "reload");
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl admin_cmds::`
Expected: compile errors `cannot find function reload_outcome_exit`, `reload_item_lines`, `reload_run`.

- [ ] **Step 3: Implement**

`admin_cmds.rs` (above the tests):

```rust
//! Mutating admin-socket commands: `reload` and `drain *` (spec §14). No
//! offline fallback: a mutation needs a daemon that proved who it is.

use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};

use crate::admin_client::{self, AdminTarget, CallError, instance_line};

/// A reload loads certificate files in `spawn_blocking`; generous but bounded.
pub(crate) const RELOAD_REPLY_TIMEOUT: Duration = Duration::from_secs(15);

const SIGHUP_HINT: &str = "`kill -HUP <pid>` uses the same code path";

pub(crate) fn reload_outcome_exit(result: &Value) -> (u8, String) {
    match result.get("outcome").and_then(Value::as_str) {
        Some(o @ ("applied" | "ok" | "noop")) => (0, o.to_string()),
        Some(other) => (1, other.to_string()),
        None => (1, "missing".to_string()),
    }
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("?").to_string()
}

/// Renders spec §9.1's `items` / `not_reloaded` and the safety floor's
/// `changed` / `rejected` / `note`, whichever the daemon sent.
pub(crate) fn reload_item_lines(r: &Value) -> Vec<String> {
    let arr = |k: &str| r.get(k).and_then(Value::as_array).cloned().unwrap_or_default();
    let mut out = Vec::new();
    for it in arr("items") {
        let detail = it.get("detail").and_then(Value::as_str).map(|d| format!(" ({d})")).unwrap_or_default();
        out.push(format!("{}: {}{detail}", s(&it, "item"), s(&it, "outcome")));
    }
    for it in arr("not_reloaded") {
        let why = it.get("reason_code").or_else(|| it.get("reason")).and_then(Value::as_str).unwrap_or("?");
        out.push(format!("{}: not reloaded ({why})", s(&it, "item")));
    }
    for c in arr("changed") {
        if let Some(name) = c.as_str() {
            out.push(format!("{name}: reloaded"));
        }
    }
    for rj in arr("rejected") {
        let (name, why) = match &rj {
            Value::Array(p) if p.len() == 2 => (p[0].as_str().unwrap_or("?").to_string(), p[1].as_str().unwrap_or("?").to_string()),
            Value::Object(_) => (s(&rj, "item"), s(&rj, "reason")),
            _ => continue,
        };
        out.push(format!("{name}: rejected ({why})"));
    }
    if let Some(n) = r.get("note").and_then(Value::as_str) {
        out.push(format!("note: {n}"));
    }
    out
}

/// Exit: 2 unreachable / identity / unusable reply, 1 error response.
pub(crate) fn report_call_error(
    verb: &str,
    e: &CallError,
    json: bool,
    err: &mut impl Write,
    unreachable_hint: Option<&str>,
) -> u8 {
    let (code, msg) = match e {
        CallError::Unreachable(w) => (
            2,
            match unreachable_hint {
                Some(h) => format!("{verb}: admin socket unreachable: {w}\n  hint: {h}"),
                None => format!("{verb}: admin socket unreachable: {w}"),
            },
        ),
        CallError::Identity(w) => (2, format!("{verb}: identity mismatch: {w}")),
        CallError::Protocol(w) => (2, format!("{verb}: unusable reply: {w}")),
        CallError::Remote { code, message, retryable } => (
            1,
            format!("{verb} failed: {code}: {message}{}", if *retryable { " (retryable)" } else { "" }),
        ),
    };
    if json {
        let _ = writeln!(err, "{}", crate::error_json(&msg));
    } else {
        let _ = writeln!(err, "weir-ctl: {msg}");
    }
    code
}

pub(crate) fn reload_run(target: &AdminTarget, json: bool, out: &mut impl Write, err: &mut impl Write) -> u8 {
    match admin_client::call(target, "reload", json!({}), RELOAD_REPLY_TIMEOUT) {
        Ok(r) => {
            let (code, outcome) = reload_outcome_exit(&r.result);
            if json {
                crate::write_json_to(out, &r.result);
            } else {
                let _ = writeln!(out, "reload: {outcome} ({})", instance_line(&r.instance));
                for l in reload_item_lines(&r.result) {
                    let _ = writeln!(out, "  {l}");
                }
            }
            code
        }
        Err(e) => report_call_error("reload", &e, json, err, Some(SIGHUP_HINT)),
    }
}

pub(crate) fn cmd_reload(target: &AdminTarget, json: bool) -> ExitCode {
    ExitCode::from(reload_run(target, json, &mut std::io::stdout(), &mut std::io::stderr()))
}
```


In `main.rs` (`mod admin_cmds;` is already declared from Step 1), add:

```rust
/// Where the daemon's admin socket is, and which wab_dir it must prove it serves.
#[derive(clap::Args)]
struct AdminArgs {
    /// The daemon's admin socket. Default: derived from --wab-dir
    /// (<parent>/<basename>.admin.sock), then /var/lib/weir/wab.admin.sock.
    #[arg(long, env = "WEIR_ADMIN_SOCKET_PATH")]
    admin_socket: Option<PathBuf>,
    /// The daemon's wab_dir: derives the socket path, and the daemon's reply
    /// must name this directory's inode.
    #[arg(long, env = "WEIR_WAB_DIR")]
    wab_dir: Option<PathBuf>,
}

impl AdminArgs {
    fn target(&self) -> admin_client::AdminTarget {
        admin_client::resolve_admin_target(self.admin_socket.as_deref(), self.wab_dir.as_deref())
    }
}
```

Add to `enum Command`:

```rust
    /// Reload TLS certificates on the running daemon over its admin socket
    /// (the same code path as SIGHUP). Exit: 0 applied or noop, 1 failed,
    /// 2 no admin socket or identity mismatch.
    Reload {
        #[command(flatten)]
        admin: AdminArgs,
    },
```

And add the match arm `Command::Reload { admin } => return admin_cmds::cmd_reload(&admin.target(), json),`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl admin_cmds:: && cargo test -p weir-ctl cli_definition_is_valid`
Expected: 4 tests pass, and the clap self-check passes.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): weir-ctl reload over the admin socket"`

---

### Task 7.14: `weir-ctl drain pause|resume|retry-stranded`

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §14 for `weir-ctl drain pause|resume|retry-stranded [--wait[=SECS]] [--admin-socket P] [--wab-dir D] [--json]`.

Exit codes:
- 0: the action was observed.
- 4: requested, but not effective yet.
- 1: an error response.
- 2: unreachable, or an identity mismatch.

Pause always prints "in-memory only; a daemon restart resumes delivery". `--json` prints the `result`, and errors keep `error_json`. There is no offline fallback.

Wait semantics:
- With no `--wait`, `wait_ms` is omitted, so the daemon applies its defaults: 5000 for pause and resume, 10000 for retry-stranded (spec §11).
- `--wait` alone waits 30 s, the protocol maximum.
- `--wait=N` waits `min(N, 30)` seconds.

**Files:**
- Modify: `crates/weir-ctl/src/admin_cmds.rs`
- Modify: `crates/weir-ctl/src/main.rs`:
  - add `#[derive(Subcommand)] enum DrainCommand`
  - add `#[derive(clap::Args)] struct DrainArgs`
  - add `Command::Drain(DrainCommand)` and its match arm
- Test: `crates/weir-ctl/src/admin_cmds.rs` `mod tests`; `crates/weir-ctl/src/main.rs` `mod tests` (CLI parse)

**Interfaces:**
- Consumes:
  - Task 7.13's `report_call_error`
  - PR 6's `drain.*` results:
    - pause: `generation, pause_requested, paused, held, paused_since_ms, held_segment{segment,processed_records}|null, warnings[], note`
    - resume: `generation, pause_requested, was_paused, paused_for_secs`
    - retry_stranded: `ticket, completed, coalesced, requeued, already_queued, backoff_cut, scan_error, paused, note`
    - resume also carries PR 6's additive `resumed: bool`; the "in effect" signals are pause `paused`/`held`, resume `resumed`, retry `completed` (see the consumed-interfaces table); a generic `observed: bool` is honoured if present
- Produces:
  - `pub(crate) enum DrainVerb { Pause, Resume, RetryStranded }`, derives `Debug, Clone, Copy, PartialEq, Eq`, with:
    - `pub(crate) fn wire(self) -> &'static str` (`"drain.pause"`, `"drain.resume"`, `"drain.retry_stranded"`)
    - `pub(crate) fn default_wait_ms(self) -> u64`
  - `pub(crate) const MAX_WAIT_MS: u64 = 30_000`
  - `pub(crate) const PAUSE_IN_MEMORY: &str = "in-memory only; a daemon restart resumes delivery"`
  - `pub(crate) fn drain_args(wait_secs: Option<u64>) -> Value`
  - `pub(crate) fn drain_reply_timeout(verb: DrainVerb, wait_secs: Option<u64>) -> Duration`
  - `pub(crate) fn drain_observed(verb: DrainVerb, result: &Value) -> bool`
  - `pub(crate) fn drain_lines(verb: DrainVerb, result: &Value, observed: bool) -> Vec<String>`
  - `pub(crate) fn drain_run(verb: DrainVerb, wait_secs: Option<u64>, target: &AdminTarget, json: bool, out: &mut impl Write, err: &mut impl Write) -> u8`
  - `pub(crate) fn cmd_drain(verb: DrainVerb, wait_secs: Option<u64>, target: &AdminTarget, json: bool) -> ExitCode`
  - `enum DrainCommand { Pause(DrainArgs), Resume(DrainArgs), #[command(name = "retry-stranded")] RetryStranded(DrainArgs) }` (main.rs)
  - `struct DrainArgs { wait: Option<u64>, admin: AdminArgs }` (main.rs)

- [ ] **Step 1: Write the failing test**

In `admin_cmds.rs` `mod tests`:

```rust
    #[test]
    fn wait_maps_to_wait_ms_with_the_protocol_cap() {
        assert_eq!(drain_args(None), json!({}), "absent: the daemon's default applies");
        assert_eq!(drain_args(Some(5)), json!({"wait_ms": 5000}));
        assert_eq!(drain_args(Some(0)), json!({"wait_ms": 0}));
        assert_eq!(drain_args(Some(99)), json!({"wait_ms": 30000}));
        assert_eq!(drain_reply_timeout(DrainVerb::RetryStranded, None), Duration::from_millis(15_000));
        assert_eq!(drain_reply_timeout(DrainVerb::Pause, Some(99)), Duration::from_millis(35_000));
    }

    #[test]
    fn observed_prefers_the_daemons_flag_and_falls_back_conservatively() {
        assert!(drain_observed(DrainVerb::Pause, &json!({"observed": true, "paused": false})));
        assert!(!drain_observed(DrainVerb::Pause, &json!({"observed": false, "paused": true})));
        assert!(drain_observed(DrainVerb::Pause, &json!({"paused": false, "held": true})));
        assert!(!drain_observed(DrainVerb::Pause, &json!({"pause_requested": true})));
        assert!(!drain_observed(DrainVerb::Resume, &json!({"was_paused": true})), "no observed flag: exit 4, not a guess");
        assert!(drain_observed(DrainVerb::Resume, &json!({"was_paused": true, "resumed": true})));
        assert!(!drain_observed(DrainVerb::Resume, &json!({"was_paused": true, "resumed": false})));
        assert!(drain_observed(DrainVerb::RetryStranded, &json!({"completed": true})));
        assert!(!drain_observed(DrainVerb::RetryStranded, &json!({"completed": false, "ticket": 3})));
    }

    #[test]
    fn pause_always_says_it_is_in_memory_only() {
        for (observed, result) in [
            (true, json!({"paused": true, "held": true, "held_segment": {"segment": "shard_00/seg_00000007", "processed_records": 1200},
                          "warnings": ["wab_max_bytes is 0: the WAB grows without bound while paused"]})),
            (false, json!({"pause_requested": true, "paused": false, "held": false, "warnings": []})),
        ] {
            let l = drain_lines(DrainVerb::Pause, &result, observed);
            assert!(l.iter().any(|x| x == PAUSE_IN_MEMORY), "{l:?}");
            if observed {
                assert!(l[0].contains("mid-segment") && l[0].contains("1200"), "{l:?}");
                assert!(l.iter().any(|x| x.starts_with("⚠ wab_max_bytes is 0")), "{l:?}");
            } else {
                assert!(l[0].contains("not in effect yet"), "{l:?}");
            }
        }
    }

    #[test]
    fn retry_stranded_lines_report_counts_or_the_pending_ticket() {
        let done = drain_lines(DrainVerb::RetryStranded,
            &json!({"ticket": 4, "completed": true, "coalesced": false, "requeued": 3, "already_queued": 1,
                    "backoff_cut": true, "scan_error": null, "paused": true,
                    "note": "requeued segments restart from record 0"}), true);
        assert!(done[0].contains("requeued 3") && done[0].contains("already queued 1") && done[0].contains("backoff cut"), "{done:?}");
        assert!(done.iter().any(|x| x.contains("paused")), "a paused drain will not deliver them yet: {done:?}");
        assert!(done.iter().any(|x| x.contains("record 0")), "{done:?}");
        let pending = drain_lines(DrainVerb::RetryStranded, &json!({"ticket": 5, "completed": false}), false);
        assert!(pending[0].contains("ticket 5") && pending[0].contains("not completed"), "{pending:?}");
    }

    #[cfg(unix)]
    #[test]
    fn drain_run_exit_codes_against_a_daemon() {
        use crate::admin_client::test_support::*;
        let cases: [(DrainVerb, Option<u64>, serde_json::Value, u8); 4] = [
            (DrainVerb::Pause, Some(5), json!({"ok": true, "result": {"observed": true, "paused": true, "held": false, "warnings": []}}), 0),
            (DrainVerb::Pause, None, json!({"ok": true, "result": {"observed": false, "paused": false, "held": false, "warnings": []}}), 4),
            (DrainVerb::RetryStranded, Some(1), json!({"ok": true, "result": {"ticket": 9, "completed": false}}), 4),
            (DrainVerb::Resume, None, json!({"ok": false, "error": {"code": "shutting_down", "message": "shutting down", "retryable": false}}), 1),
        ];
        for (i, (verb, wait, body, code)) in cases.into_iter().enumerate() {
            let dir = private_dir(&format!("dr{i}"));
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                let mut r = body.clone();
                r["v"] = json!(1);
                r["id"] = req["id"].clone();
                r["instance"] = inst.clone();
                r
            });
            let t = crate::admin_client::resolve_admin_target(Some(&dir.join("a.sock")), Some(&wab));
            let (mut out, mut err) = (Vec::new(), Vec::new());
            assert_eq!(drain_run(verb, wait, &t, false, &mut out, &mut err), code, "case {i}");
            let req = h.join().unwrap();
            assert_eq!(req["verb"], verb.wire());
            assert_eq!(req["args"], drain_args(wait));
            if verb == DrainVerb::Pause {
                assert!(String::from_utf8(out).unwrap().contains(PAUSE_IN_MEMORY));
            }
            if code == 1 {
                assert!(String::from_utf8(err).unwrap().contains("drain.resume failed: shutting_down"));
            }
            std::fs::remove_dir_all(&dir).ok();
        }
        // No socket: 2, no offline fallback.
        let t = crate::admin_client::resolve_admin_target(Some(Path::new("/nonexistent/x.sock")), None);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(drain_run(DrainVerb::Pause, None, &t, false, &mut out, &mut err), 2);
    }
```

In `main.rs` `mod tests`:

```rust
    #[test]
    fn drain_cli_parses_verbs_and_the_optional_wait() {
        use clap::Parser;
        let wait_of = |args: &[&str]| match Cli::try_parse_from(args).unwrap().command {
            Command::Drain(DrainCommand::Pause(a) | DrainCommand::Resume(a) | DrainCommand::RetryStranded(a)) => a.wait,
            _ => panic!("not a drain command"),
        };
        assert_eq!(wait_of(&["weir-ctl", "drain", "pause"]), None);
        assert_eq!(wait_of(&["weir-ctl", "drain", "pause", "--wait"]), Some(30));
        assert_eq!(wait_of(&["weir-ctl", "drain", "resume", "--wait=5"]), Some(5));
        assert_eq!(wait_of(&["weir-ctl", "--json", "drain", "retry-stranded", "--admin-socket", "/a.sock"]), None);
        assert!(Cli::try_parse_from(["weir-ctl", "drain", "retry_stranded"]).is_err(), "the CLI spelling is kebab-case");
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl drain`
Expected: compile errors `cannot find type DrainVerb`, `cannot find function drain_run`, `no variant named Drain`.

- [ ] **Step 3: Implement**

Append to `admin_cmds.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainVerb {
    Pause,
    Resume,
    RetryStranded,
}

impl DrainVerb {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            DrainVerb::Pause => "drain.pause",
            DrainVerb::Resume => "drain.resume",
            DrainVerb::RetryStranded => "drain.retry_stranded",
        }
    }
    /// The daemon's default `wait_ms` when weir-ctl sends none (spec §11).
    pub(crate) fn default_wait_ms(self) -> u64 {
        match self {
            DrainVerb::Pause | DrainVerb::Resume => 5_000,
            DrainVerb::RetryStranded => 10_000,
        }
    }
}

/// The admin protocol's `wait_ms` range is 0..=30000.
pub(crate) const MAX_WAIT_MS: u64 = 30_000;
pub(crate) const PAUSE_IN_MEMORY: &str = "in-memory only; a daemon restart resumes delivery";
/// Margin over the daemon-side wait for the reply to arrive.
const REPLY_MARGIN_MS: u64 = 5_000;

pub(crate) fn drain_args(wait_secs: Option<u64>) -> Value {
    match wait_secs {
        None => json!({}),
        Some(s) => json!({"wait_ms": s.saturating_mul(1000).min(MAX_WAIT_MS)}),
    }
}

pub(crate) fn drain_reply_timeout(verb: DrainVerb, wait_secs: Option<u64>) -> Duration {
    let wait = wait_secs.map_or(verb.default_wait_ms(), |s| s.saturating_mul(1000).min(MAX_WAIT_MS));
    Duration::from_millis(wait + REPLY_MARGIN_MS)
}

fn flag(v: &Value, k: &str) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(false)
}

/// The daemon's own `observed` flag when present. Otherwise the conservative
/// reading of the verb's result keys: unknown means "not yet" (exit 4).
pub(crate) fn drain_observed(verb: DrainVerb, result: &Value) -> bool {
    if let Some(b) = result.get("observed").and_then(Value::as_bool) {
        return b;
    }
    match verb {
        DrainVerb::Pause => flag(result, "paused") || flag(result, "held"),
        // PR 6's ResumeResult: `resumed` = observed no longer paused or held.
        DrainVerb::Resume => flag(result, "resumed"),
        DrainVerb::RetryStranded => flag(result, "completed"),
    }
}

pub(crate) fn drain_lines(verb: DrainVerb, r: &Value, observed: bool) -> Vec<String> {
    let num = |k: &str| r.get(k).filter(|v| !v.is_null()).map_or_else(|| "?".to_string(), Value::to_string);
    let mut out = Vec::new();
    match verb {
        DrainVerb::Pause => {
            out.push(if !observed {
                "drain.pause: requested; not in effect yet (it takes effect before the next commit batch)".to_string()
            } else if let Some(h) = r.get("held_segment").filter(|v| v.is_object()) {
                format!(
                    "drain.pause: paused mid-segment ({} at record {})",
                    h.get("segment").and_then(Value::as_str).unwrap_or("?"),
                    h.get("processed_records").map_or_else(|| "?".to_string(), Value::to_string)
                )
            } else {
                "drain.pause: paused".to_string()
            });
            for w in r.get("warnings").and_then(Value::as_array).into_iter().flatten() {
                if let Some(w) = w.as_str() {
                    out.push(format!("⚠ {w}"));
                }
            }
            out.push(PAUSE_IN_MEMORY.to_string());
        }
        DrainVerb::Resume => out.push(if !observed {
            "drain.resume: requested; the drain has not picked it up yet".to_string()
        } else if r.get("was_paused").and_then(Value::as_bool) == Some(false) {
            "drain.resume: delivery was not paused".to_string()
        } else {
            match r.get("paused_for_secs").and_then(Value::as_f64) {
                Some(s) => format!("drain.resume: delivery resumed (paused for {s:.0}s)"),
                None => "drain.resume: delivery resumed".to_string(),
            }
        }),
        DrainVerb::RetryStranded => {
            if observed {
                let mut l = format!(
                    "drain.retry_stranded: requeued {}, already queued {}",
                    num("requeued"),
                    num("already_queued")
                );
                if flag(r, "backoff_cut") {
                    l.push_str(", backoff cut");
                }
                if flag(r, "coalesced") {
                    l.push_str(" (coalesced with a retry already running)");
                }
                out.push(l);
                if let Some(e) = r.get("scan_error").and_then(Value::as_str) {
                    out.push(format!("⚠ the stranded-segment scan failed: {e}"));
                }
                if flag(r, "paused") {
                    out.push("⚠ delivery is paused: requeued segments wait for `weir-ctl drain resume`".to_string());
                }
            } else {
                out.push(format!(
                    "drain.retry_stranded: requested (ticket {}); not completed within the wait",
                    num("ticket")
                ));
            }
            if let Some(n) = r.get("note").and_then(Value::as_str) {
                out.push(n.to_string());
            }
        }
    }
    if !observed {
        out.push("re-run `weir-ctl status` to see when it takes effect (exit 4)".to_string());
    }
    out
}

pub(crate) fn drain_run(
    verb: DrainVerb,
    wait_secs: Option<u64>,
    target: &AdminTarget,
    json: bool,
    out: &mut impl Write,
    err: &mut impl Write,
) -> u8 {
    match admin_client::call(target, verb.wire(), drain_args(wait_secs), drain_reply_timeout(verb, wait_secs)) {
        Ok(r) => {
            let observed = drain_observed(verb, &r.result);
            if json {
                crate::write_json_to(out, &r.result);
            } else {
                for l in drain_lines(verb, &r.result, observed) {
                    let _ = writeln!(out, "{l}");
                }
                let _ = writeln!(out, "{}", instance_line(&r.instance));
            }
            if observed { 0 } else { 4 }
        }
        Err(e) => report_call_error(verb.wire(), &e, json, err, None),
    }
}

pub(crate) fn cmd_drain(verb: DrainVerb, wait_secs: Option<u64>, target: &AdminTarget, json: bool) -> ExitCode {
    ExitCode::from(drain_run(verb, wait_secs, target, json, &mut std::io::stdout(), &mut std::io::stderr()))
}
```

In `main.rs`:

```rust
/// Subcommands under `weir-ctl drain`.
#[derive(Subcommand)]
enum DrainCommand {
    /// Stop delivery before the next commit batch; ingest keeps buffering.
    /// In-memory only: a daemon restart resumes delivery.
    Pause(DrainArgs),
    /// Restart delivery after a pause.
    Resume(DrainArgs),
    /// Requeue segments that exhausted their retries now (each restarts from
    /// record 0), instead of waiting for a healthy sink probe.
    #[command(name = "retry-stranded")]
    RetryStranded(DrainArgs),
}

#[derive(clap::Args)]
struct DrainArgs {
    /// Wait up to SECS (default 30, max 30) for the drain to act. Without it
    /// the daemon's default wait applies. Exit 4 if it has not acted yet.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "30", value_name = "SECS")]
    wait: Option<u64>,
    #[command(flatten)]
    admin: AdminArgs,
}
```

Add to `enum Command` (the `#[command(subcommand)]` attribute is required: without it clap's derive treats the single field as `Args` to flatten, and `DrainCommand` derives `Subcommand`, so it would not compile; `Dl`, `Quarantine` and `Attest` carry the same attribute):

```rust
    /// Pause, resume or retry delivery on the running daemon (admin socket).
    /// Exit: 0 done, 4 requested but not yet in effect, 1 error, 2 unreachable
    /// or identity mismatch.
    #[command(subcommand)]
    Drain(DrainCommand),
```

Add the match arm:

```rust
        Command::Drain(d) => {
            let (verb, a) = match d {
                DrainCommand::Pause(a) => (admin_cmds::DrainVerb::Pause, a),
                DrainCommand::Resume(a) => (admin_cmds::DrainVerb::Resume, a),
                DrainCommand::RetryStranded(a) => (admin_cmds::DrainVerb::RetryStranded, a),
            };
            return admin_cmds::cmd_drain(verb, a.wait, &a.admin.target(), json);
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl admin_cmds:: && cargo test -p weir-ctl drain_cli_parses && cargo test -p weir-ctl cli_definition_is_valid`
Expected: the 5 new admin_cmds tests plus Task 7.13's 4, and `drain_cli_parses_verbs_and_the_optional_wait`, pass, and the clap self-check passes (`require_equals` with `num_args = 0..=1` is valid).

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src && git commit -m "feat(ctl): weir-ctl drain pause, resume and retry-stranded"`

---

### Task 7.15: weir-ctl header comment and README name the new commands (with a drift guard)

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

The main.rs header (lines 1-5) still says "No new daemon-side API is required", which stops being true in this PR (spec §14 "The header comment (weir-ctl main.rs:1-5) is updated"). The crate README lists the subcommands by hand. A guard keeps that list honest.

**Files:**
- Modify: `crates/weir-ctl/src/main.rs` lines 1-5 (module doc)
- Modify: `crates/weir-ctl/README.md`
- Test: `crates/weir-ctl/src/main.rs` `mod tests`

**Interfaces:**
- Consumes: `Cli` (clap `CommandFactory`).
- Produces: test `readme_names_every_subcommand`.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn readme_names_every_subcommand() {
        use clap::CommandFactory;
        let readme = include_str!("../README.md");
        for sub in Cli::command().get_subcommands() {
            let name = sub.get_name();
            assert!(
                readme.contains(&format!("`{name}`")),
                "crates/weir-ctl/README.md must name the `{name}` subcommand"
            );
        }
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-ctl readme_names_every_subcommand`
Expected: FAIL with "crates/weir-ctl/README.md must name the `status` subcommand".

- [ ] **Step 3: Implement**

Replace main.rs lines 1-5 with:

```rust
//! `weir-ctl` — admin and inspection CLI for the weir daemon.
//!
//! An operator tool over the daemon's surfaces: the producer Unix socket
//! (HealthCheck / Push frames, via `weir-client`), the metrics listener
//! (`/metrics`, and `/status` from 4.1), the admin Unix socket (`status`,
//! `reload`, `drain *`, authenticated client-side before any answer is
//! trusted), and the `wab_dir` on disk, read-only. weir-ctl never takes,
//! probes or releases the daemon's `.weir.lock`.
```

In `crates/weir-ctl/README.md`, replace the first paragraph after the title with:

```markdown
An operator tool over the daemon's surfaces: the producer Unix socket, the
metrics listener (`/metrics`, `/status`), the admin Unix socket, and the
`wab_dir` on disk (read-only). Subcommands: `status` (can the daemon take
ingest, and why not — admin socket, then `/status`, then `/metrics` plus disk,
then disk alone; exit 0 ready, 1 not ready (including a dead daemon whose
`wab_dir` was read from disk), 2 unreachable with no offline facts, or identity
mismatch, 3 ready with a critical condition), `reload` (TLS certificates, the same path
as SIGHUP), `drain` (`pause` / `resume` / `retry-stranded`; pause is in-memory
only), `health`, `push`, `metrics`, `segments` (per-shard WAB inspect,
`--files` for every segment file), `dl` (dead-letter `list` / `drop` /
`requeue`), and `quarantine` (`list` / `inspect` / `requeue`) for segments
that crash recovery set aside after a corrupt read, and `attest` (`seal` /
`verify` / `head`) over a sealed segment's `weir-attest` hash chain.

The admin socket defaults to `WEIR_ADMIN_SOCKET_PATH`, then to
`<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` derived from `--wab-dir` /
`WEIR_WAB_DIR`, then to `/var/lib/weir/wab.admin.sock`. Before trusting an
answer, weir-ctl checks that the path is a 0600 socket in a directory nobody
else can write to, that the daemon runs as weir-ctl's uid, the wab_dir's owner
or root, and that the daemon serves the same `wab_dir` inode.
```

Keep the `--json` paragraph and the workspace link. Add `status`, `reload` and `drain` to the list of subcommands that honour `--json`.

Verify by mutation: delete "`drain`" from the README, run the test, see it fail naming `drain`, then restore.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-ctl readme_names_every_subcommand`
Expected: PASS.

- [ ] **Step 5: Commit**

`git add crates/weir-ctl/src/main.rs crates/weir-ctl/README.md && git commit -m "docs(ctl): name status, reload and drain in the header and README, and guard the list"`

---

### Task 7.16: system tests: `weir-ctl status` via admin, http and offline with `/readyz` parity; drain and reload round trip

*PR-level merge gate: PR 7's **Merge gate** (top of the PR 7 section).*

Spec §16 system test 9: "`weir-ctl status --json` via admin, http and offline, plus the parity check". The drain/reload round trip exercises the real admin socket from PR 6 end to end through the CLI, including:
- the derived default path, computed independently by both binaries;
- the identity check against a real daemon's `instance`.

**Files:**
- Modify: `crates/weir-server/tests/system.rs` (two new tests next to `weir_ctl_beside_a_live_daemon_neither_sees_nor_disturbs_the_lock`, ~line 2825; reuse `weir_ctl_binary()`, ~line 3902)

**Interfaces:**
- Consumes:
  - `weir_testkit::WeirServer::{wab_dir, metrics_port, client, sigterm}`
  - `weir_ctl_binary()`
  - `ureq` and `serde_json` (both already dev-dependencies of weir-server)
  - PR 5's `/readyz` and `/status`, and PR 6's admin socket at the derived default path
- Produces:
  - test `weir_ctl_status_via_admin_http_and_offline_agrees_with_readyz`
  - test `weir_ctl_drain_and_reload_round_trip_over_the_admin_socket`

- [ ] **Step 1: Write the failing test**

```rust
/// Foundations §16 system test 9: `weir-ctl status --json` via the admin
/// socket, GET /status and offline (disk), and the exit agrees with /readyz:
/// exit ∈ {0,3} ⇔ /readyz 200.
#[test]
fn weir_ctl_status_via_admin_http_and_offline_agrees_with_readyz() {
    let mut srv = weir_server!("ctl_status").start();
    let ctl = weir_ctl_binary();
    let wab = srv.wab_dir.clone();
    let addr = format!("127.0.0.1:{}", srv.metrics_port);
    let run = |source: &str| -> (Option<i32>, serde_json::Value) {
        let out = Command::new(&ctl)
            .args(["--json", "status", "--source", source, "--addr", &addr, "--wab-dir"])
            .arg(&wab)
            .env_remove("WEIR_ADMIN_SOCKET_PATH")
            .output()
            .expect("run weir-ctl");
        let v = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
            panic!("status --source {source}: {e}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr))
        });
        (out.status.code(), v)
    };
    let readyz_200 = || match ureq::get(&format!("http://{addr}/readyz")).call() {
        Ok(r) => r.status() == 200,
        Err(ureq::Error::Status(_, _)) => false,
        Err(e) => panic!("/readyz: {e}"),
    };

    assert!(readyz_200(), "a started daemon is ready");

    let (code, v) = run("admin");
    assert_eq!(v["source"], "admin", "{v}");
    assert_eq!(v["view"], "admin");
    assert_eq!(v["schema"], "weir.status/v1");
    assert!(v["instance"]["wab_dir_ino"].is_u64(), "the admin view names its wab_dir: {v}");
    assert_eq!(matches!(code, Some(0) | Some(3)), readyz_200(), "parity (admin): exit {code:?}");

    let (code, v) = run("http");
    assert_eq!(v["source"], "http");
    assert_eq!(v["view"], "public");
    assert!(v.get("instance").is_none(), "the public document carries no instance");
    assert_eq!(matches!(code, Some(0) | Some(3)), readyz_200(), "parity (http): exit {code:?}");

    let (_, v) = run("auto");
    assert_eq!(v["source"], "admin", "auto prefers the admin socket");

    srv.sigterm();
    let (code, v) = run("auto");
    assert_eq!(code, Some(1), "no daemon answers, disk facts printed: exit 1 (README Q9)\n{v}");
    assert_eq!(v["source"], "disk");
    assert_eq!(v["conditions"][0]["code"], "daemon_unreachable");
    let tried: Vec<&str> = v["sources_tried"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["source"].as_str().unwrap())
        .collect();
    assert_eq!(tried, ["admin", "http", "metrics", "disk"]);
    assert!(v["backlog"]["undelivered_segments"].is_u64());
}

/// The admin verbs end to end through the CLI against a real daemon: pause is
/// observed and says it is in-memory only, status shows drain_paused, resume
/// is observed, retry-stranded returns a ticket, reload on a non-TLS daemon is
/// a noop (exit 0).
#[test]
fn weir_ctl_drain_and_reload_round_trip_over_the_admin_socket() {
    let srv = weir_server!("ctl_drain").start();
    let ctl = weir_ctl_binary();
    let wab = srv.wab_dir.clone();
    let run = |args: &[&str]| -> (Option<i32>, String, String) {
        let out = Command::new(&ctl)
            .args(args)
            .arg("--wab-dir")
            .arg(&wab)
            .env_remove("WEIR_ADMIN_SOCKET_PATH")
            .output()
            .expect("run weir-ctl");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };

    let (code, out, err) = run(&["drain", "pause", "--wait=10"]);
    assert_eq!(code, Some(0), "pause observed within 10 s\nstdout: {out}\nstderr: {err}");
    assert!(out.contains("in-memory only; a daemon restart resumes delivery"), "{out}");

    let mut client = srv.client();
    for i in 0..50 {
        client.push(format!("paused-{i}").as_bytes(), Durability::Durable).unwrap();
    }
    let (_, out, _) = run(&["--json", "status", "--source", "admin"]);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(
        v["conditions"].as_array().unwrap().iter().any(|c| c["code"] == "drain_paused"),
        "a paused drain shows drain_paused: {v}"
    );

    let (code, out, err) = run(&["drain", "resume", "--wait=10"]);
    assert_eq!(code, Some(0), "resume observed\nstdout: {out}\nstderr: {err}");

    let (code, out, _) = run(&["--json", "drain", "retry-stranded", "--wait=10"]);
    assert!(matches!(code, Some(0) | Some(4)), "exit {code:?}: {out}");
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(v.get("ticket").is_some(), "{v}");

    let (code, out, err) = run(&["--json", "reload"]);
    assert_eq!(code, Some(0), "no TLS listener configured: noop, exit 0\nstdout: {out}\nstderr: {err}");
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["outcome"], "noop", "{v}");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-server --test system weir_ctl_ -- --test-threads=1`
Expected: before Tasks 7.12-7.14 are wired in, the weir-ctl binary rejects `status` / `drain` with `error: unrecognized subcommand`, so `serde_json::from_slice` panics. If this task runs last as planned, temporarily revert the `Command::Status` arm to confirm the failure mode, then restore it.

- [ ] **Step 3: Implement**

No production code: the tests run Tasks 7.1-7.15 end to end. If the drain/reload test's resume exits 4, check that PR 6's `drain.resume` result carries `resumed: true` once the board shows the drain no longer paused or held (PR 6 Task 6.10); weir-ctl reads exactly that key.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-server --test system weir_ctl_ -- --test-threads=1`
Expected: `weir_ctl_status_via_admin_http_and_offline_agrees_with_readyz`, `weir_ctl_drain_and_reload_round_trip_over_the_admin_socket` and the existing `weir_ctl_beside_a_live_daemon_neither_sees_nor_disturbs_the_lock` pass.

Then run the merge gate:
- `cargo test -p weir-status`
- `cargo test -p weir-ctl`
- `cargo test --workspace --exclude weir-server --all-features`
- `act -W .github/workflows/ci.yml -j lint`
- `act -W .github/workflows/ci.yml -j test`

- [ ] **Step 5: Commit**

`git add crates/weir-server/tests/system.rs && git commit -m "test(system): weir-ctl status agrees with /readyz across sources; drain and reload round trip"`
