# Operability Foundations — PR 1: Status foundations: weir-status, the status board, shard registry, cap refresh and SP-D logging

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 1: Status foundations: weir-status, the status board, shard registry, cap refresh and SP-D logging

**Scope (spec §16, PR table row 1).** weir-status types and evaluator; StatusBoard (no channel handle), ShardRegistry, WorkerCell, `FlusherExit`; ConfigView; additive metrics (channel depth registered in PR 3); poll tasks extracted; `wab::cap::evaluate`, poll refresh, synchronous first scan before `socket::run`; SP-D logging; the SIGTERM-exit system test. Spec sections 5.1, 5.2, 5.3, 5.5 (cap part), 5.11, 5.13, 5.14, 9, 13. Size M.

**Depends on.** The complete safety floor already in this tree (`feat/operability-foundations` at 3adc5f5: 2491883 plus the spec): `wab/lock.rs`, `preflight.rs`, `reload.rs`, `socket::{probe_socket, BoundSocket, bind_hardened, remove_own_socket}`, `drain/ledger.rs`, `DrainHeartbeat::busy()` / `BusyGuard` / `BusyKind`, and `drain_stall_verdict` with `SlowSinkCall` / `SlowLocalIo`. Nothing else. PRs 2 and 3 start from this PR and run in parallel.

**Behaviour change (release-noted).**
1. **Cap refresh.** The 5 s poll task now calls `wab::cap::evaluate` after every successful scan, so `wab_cap_rejecting` clears without a push arriving. A failed scan leaves the flag unchanged (fail-closed).
2. **Startup cap fail-closed.** A synchronous size scan plus cap evaluation runs after recovery and replay, before `socket::run`. If that scan fails and `wab_max_bytes > 0`, the flag starts `true`: pushes nack until the first successful poll scan.
3. **Gauge cadence.** `weir_dead_letter_bytes_on_disk` and `weir_quarantine_bytes_on_disk` are now also set by the 5 s task; the drain's own writes stay.
4. **Logging (SP-D).** One "weir starting" line now carries `version`, `wire_version` and the ConfigView. ANSI colour only when stdout is a terminal. An invalid `log_level` WARNs instead of silently falling back. Commit-path log lines carry the segment identity. A dead-letter-blocked episode WARNs when it starts.
5. **Additive metrics.** 34 new families (listed in Task 1.18), each with a `docs/monitoring.md` row. No renames, and no labels added to existing families.

Everything else is additive and inert until a later PR writes it: the drain and sink cells (PR 2), the hand-off marker and channel depth (PR 3), lifecycle levels and the drain deadline (PR 4), the admin and reload cells (PR 6).

**Merge gate.**
1. `cargo test -p weir-status`.
2. `cargo test -p weir-server --bins -- --test-threads=1`, and again with `--all-features`.
3. The two CI lines verbatim (ci.yml:218 and :223): `cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test load --test load_tls --test tls_client --test tls_listener`, and the same with `--all-features`. CI passes no `--test-threads=1` to these integration targets, so the new system tests must pass in parallel (each spawns its own daemon process, so `bind_hardened`'s umask swap does not cross tests). This includes the new `sigterm_exits_within_the_bound_with_an_idle_connection_open` (spec §16 system test 13) and `a_serving_daemon_reports_ready_through_the_new_gauges`.
4. `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`. The DST call sites changed.
5. `cargo clippy --all-targets -- -D warnings`, plus the `--all-features`, `--no-default-features` and `--features s3-sink` variants. `cargo fmt --check`.
6. `cargo check -p weir-core -p weir-wab -p weir-sink-sdk -p weir-status --all-features --locked`. This is the Windows-library line, which gains `weir-status`.
7. `(cd fuzz && cargo metadata --format-version 1 >/dev/null) && git diff --exit-code fuzz/Cargo.lock`. Run it after committing the refreshed lock: fuzz depends on weir-server by path, so its lock must carry weir-status and the unconditional arc-swap.
8. `act -W .github/workflows/ci.yml -j <job>` for every job, container jobs included (the Dockerfile gains a member), before pushing.
9. Owner-run on beast, before merge: `tests/load.rs` Durable herd and Buffered, before and after, within ±3%. The only additions near the ack path are a drop guard per worker and one registry store per flusher at counter-scan completion. The measurement is still the gate.

**Reconciliations with the spec.** Code wins for current behaviour; spec wins for target.
1. **Line numbers.** The spec's `main.rs` numbers predate the safety floor. The current anchors (at 3adc5f5; locate by the quoted text) are:
   - poll-task helpers: main.rs:70-740;
   - 500 ms task: :1476-1486;
   - 5 s task: :1488-1542;
   - `wab_bytes_now` / `wab_cap_rejecting` creation: :1496 / :1502;
   - signal task: :1635-1659;
   - `socket::run` call: :1679;
   - tracing init: :919-924;
   - "weir starting": :953-959 (the `info!(` at :953, the message at :958);
   - `over_wab_cap`: socket/connection.rs:867-886;
   - supervisor: wab/mod.rs:172-226;
   - counter-scan give-up: wab/mod.rs:861-870;
   - `flusher_thread`: wab/mod.rs:791.
2. **`drain_stall_verdict` "gains the commit-in-flight exception" (§5.11 step 8) is already done.** The safety floor's busy signal (`BusyKind::SinkCommit` → `DrainStallVerdict::SlowSinkCall`, main.rs:283-323) implements it. This PR moves that code unchanged.
3. **Where the workers live.** §5.2 puts `workers: Box<[WorkerCell]>` on the board. Here the `WorkerCell`s live inside `ShardRegistry`, because "ingest-capable" (§5.3) reads both a shard and the worker that owns it on every `pick_live`. The board reaches them as `board.shards.worker_evidence()`.
4. **`board.shards` is `Arc<ShardRegistry>`**, not a plain field. Flusher and worker threads hold the registry without holding the whole board. Method calls (`board.shards.pick_live(n)`) are unchanged by the `Arc`.
5. **Drain-panic payload logging (§5.13) moves to PR 2.** PR 2 rewrites `run_drain_supervised` and already plans to log the payload through `wab::panic_message_str` (§5.4), so doing it here would put two PRs into the same function. The other §5.13 items stay here.
6. **Segment identity in `commit_batch` log lines (§5.13)** is attached as a `tracing` span around the two `commit_batch(..).await` calls in `process_segment`. Adding a parameter would change `commit_batch`'s signature and the test shim at drain/mod.rs:4071-4090 that all six test calls (drain/mod.rs:4400, 4413, 6097, 6129, 6159, 7776) go through, which PR 2's additions-only gate protects.
7. **`weir_records_nack_internal_total{cause}`** gains one cause beyond §13's list: `ownership_lost`. The safety floor added a nack site for it (connection.rs:363-385, `AdmissionRefusal::OwnershipLost`).
8. **`with_ansi(stderr().is_terminal())` (§5.13) becomes `stdout()`.** `tracing_subscriber::fmt()` writes to stdout, and colour should follow the stream actually written.
9. **Evidence fields beyond §5.1.**
   - Added: `identity`, `started_at_unix_ms`, `ever_served`, `shutting_down_since_unix_ms`, `forced`, `drain_deadline_unix_ms`, `drain_budget_secs`, `workers`, `queue_depth`, `channel`.
   - Reason: §9.1's document and the §9.3 triggers need these facts, and nothing in §5.1 carries them.
   - Constructors: `Evidence::empty(now, source)` and `Evidence::default()`. Missing evidence is `None`, which makes a condition unevaluated (never "false").
10. **Unpublished sentinels.** Nothing writes `drain_depth` before PR 3, and the drain and sink cells stay unwritten until PR 2. Each starts at an explicit "unpublished" value (`usize::MAX`, or `published == false`), and that value renders as `None` evidence. So `drain_backpressure`, the drain and sink conditions, `delivery_stalled` and `segments_stranded` are reported **unevaluated** in PR 1 rather than falsely healthy.
11. **Transition logging (§5.11 step 7).**
    - Raising a `warning` or `critical` condition logs at WARN.
    - Raising an `info` condition logs at INFO, so `starting` does not WARN on every start.
    - Clearing logs at INFO.
12. **Public rendering.** `PublicStatusDoc` and `weir_status::public` are PR 5's (its Task 5.1). This PR ships `AdminStatusDoc` with the §9.1 field names, `StatusSnapshot::admin_doc`, and a golden admin document whose `Option` fields are populated for PR 5's sentinel test. `drain`, `sink` and `backlog` are `Option` on the admin type, because they are unknown until their writers publish (§9.1: "an unknown value is `null`"). PR 5 maps them `Option` → `Option`. Likewise `channel_depth: Option<u64>`.
13. **Crate name.** crates.io returned 404 for `weir-status` on 2026-10-15. That shows the name is unclaimed, **not** that the owner holds it. Reserve it before the release tag (see the open questions).
14. **Safety-floor fixes.** Checked against 2491883 (3adc5f5): the lock-integrity std thread is started at main.rs:989-1004 (its comment block at :989, `spawn_lock_integrity_thread(` at :999 under the `if` at :998), right after acquisition (`acquire_wab_dir_lock`, :973) and before preflight and `wab::spawn` (:1087), so it covers recovery and replay. Task 1.23 must keep that order and must not add a `wab_ownership_lost: ` field (the source guard `ownership_flag_is_one_arc_shared_by_the_integrity_thread_and_the_listeners`, main.rs:1847-1885, counts exactly two). The `(dev, ino)` pin lives in drain/ledger.rs (`admit` :275, `PinnedCarry` :172, checks at :295, :337, :515); Task 1.25's span wraps only the `commit_batch` calls at drain/mod.rs:2150 and :2170 and touches no ledger call. The non-Linux probe is socket/mod.rs:810-874 (`probe_socket`, `probe_socket_attached`, `probe_socket_once`); Task 1.23 edits `socket::run` (:119) after `bind_hardened` (:422 Linux, :594 other) and leaves `probe_socket` alone.
15. **`drain_wedged` reads the safety floor's busy signal.** §9.3 code 13's trigger names only the commit timer and the progress timer, but the merged floor's `classify_drain_stall` (main.rs:302-323) classifies a frozen heartbeat inside local I/O as `SlowLocalIo` (never a wedge) and a sink call within `commit_timeout + 15 s` as `SlowSinkCall`. So that the log line, the condition and an opted-in `/livez` give one verdict (G2), `DrainEvidence` gains `busy: Option<BusyEvidence>` (Task 1.2), the rule applies the same table (Task 1.5), the 5 s poll task publishes the reading on the board (`drain_busy_since_ms`, `drain_busy_sink_call`, Tasks 1.16 and 1.22), and the exposition parser reads `weir_drain_busy_seconds{kind}` (Task 1.9).

**Names other plans code against.** pr-2-drain-control.md, pr-3-startup-split.md, pr-5-http-health.md and pr-7-weir-ctl.md list what they consume from PR 1, and this plan adopts those spellings:
- the `DrainCell` / `SinkCell` field and method names, and `InFlight`, `RequeueResult`, `DrainExitReason`, `DrainExitInfo`, `SinkFailureKind` (PR 2);
- `board.shards`, `board.drain_depth`, `ShardRegistry::{pick_live, all_settled, set_handoff_blocked_since_ms}`, and the `wab_segments_not_handed_off` / `connections_refused_no_live_shard` counters (PR 3);
- `Condition.since_unix_ms` serialised as `since_ms`, `Verdict.livez_checks: Vec<LivezCheck>`, `LivezCheck::ALL` / `as_str`, the §9.1 view names, the `WEIR_BLESS` golden convention, and `latches` implementing `SinceLookup` (PR 5);
- `Evidence::empty`, `Source::Disk`, `BacklogEvidence { …, dead_letter_max_bytes: Option<u64>, scanned_at_ms }` and `NoLatches` (PR 7).

---
### Task 1.1: Create the `weir-status` crate with the Conditions catalogue

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/Cargo.toml`, `crates/weir-status/README.md`, `crates/weir-status/src/lib.rs`, `crates/weir-status/src/codes.rs`
- Modify: `Cargo.toml` (root): `members` (after `"crates/weir-wab"`), `[workspace.dependencies]` (after `weir-wab`)
- Modify: `deploy/docker/Dockerfile:13-54`: one `COPY` line, one `mkdir` entry, one `touch` entry
- Modify: `.github/workflows/ci.yml:498`: add `-p weir-status` to the Windows library check
- Modify: `RELEASING.md:22-29` (publish order and the sentence after it) and `RELEASING.md:57-62` ("seven" → "eight" occurrences)
- Modify: `docs/platform-support.md:119-135` (library table and the sentence after it), `CONTRIBUTING.md:142-145`, `README.md` (crate table, :213; the Windows sentence, :261-262), `docs/architecture.md` (crate-boundary table, :314): name `weir-status` wherever the published libraries are enumerated
- Modify: `demo/crates.html`: a table row after the `weir-wab` row (:45) and a card after the `weir-wab` card (:98-104). `the_demo_lists_every_published_crate` (crates/weir-server/tests/demo_drift.rs:236-268) fails for any published workspace member the page does not name, and `weir-status` has no `publish = false`.
- Test: `crates/weir-status/src/codes.rs` (`mod tests`)

**Interfaces:**
- Consumes: nothing (serde only).
- Produces:
  ```rust
  pub enum Severity { Info, Warning, Critical }                 // Ord, serde snake_case
  pub enum Affects { Ingest, Delivery, Durability }             // serde snake_case
  pub enum Gate { Readiness, Liveness }                         // serde snake_case
  pub enum ConditionCode { Starting, ShuttingDown, ListenerDown, NoLiveShard, ShardOffline,
      WorkerExited, WabCapRejecting, DrainBackpressure, AckTimeoutsRecent, WabScanStale,
      WabGrowingUnbounded, DrainStopped, DrainWedged, DrainPaused, DrainBlockedDeadLetterFull,
      DeliveryStalled, DeliveryLagging, SegmentsStranded, SinkDown, SinkDegraded, SinkFailing,
      DeadLetterNearCap, NoopSink, ActiveSegmentAging, FsyncFailuresRecent, FlusherRespawnedRecent,
      QuarantineNonempty, RecoveryIncomplete, TlsReloadFailed, AdminUnavailable,
      DaemonUnreachable, RecoveryPending, Unknown }             // Copy, Ord, Hash
  impl ConditionCode { pub fn as_str(self) -> &'static str; pub fn parse_lossy(s: &str) -> Self; }
  // Serialize = as_str(); Deserialize = parse_lossy (an unknown string becomes Unknown)
  pub struct CatalogueEntry { pub code: ConditionCode, pub severity: Severity, pub affects: Affects,
                              pub gates: &'static [Gate], pub runbook: &'static str }
  pub const CATALOGUE: &[CatalogueEntry];      // the 30 daemon codes, §9.3 order
  pub const OFFLINE_ONLY: &[CatalogueEntry];   // daemon_unreachable, recovery_pending
  pub const ALL: &[ConditionCode];             // == CATALOGUE codes, same order (30)
  pub fn entry(code: ConditionCode) -> Option<&'static CatalogueEntry>;
  ```

- [ ] **Step 1: Write the failing test.** Create `crates/weir-status/src/codes.rs` with only the test module below, and `crates/weir-status/src/lib.rs` containing `pub mod codes;`.
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn catalogue_has_the_thirty_daemon_codes_in_spec_order() {
          assert_eq!(CATALOGUE.len(), 30);
          assert_eq!(ALL.len(), 30);
          for (e, c) in CATALOGUE.iter().zip(ALL) {
              assert_eq!(e.code, *c, "ALL and CATALOGUE disagree at {c:?}");
          }
          assert_eq!(ALL[0], ConditionCode::Starting);
          assert_eq!(ALL[29], ConditionCode::AdminUnavailable);
      }

      #[test]
      fn every_code_round_trips_through_its_wire_name() {
          for c in ALL.iter().chain(OFFLINE_ONLY.iter().map(|e| &e.code)) {
              assert_eq!(ConditionCode::parse_lossy(c.as_str()), *c);
              let json = serde_json::to_string(c).unwrap();
              assert_eq!(json, format!("\"{}\"", c.as_str()));
              assert_eq!(serde_json::from_str::<ConditionCode>(&json).unwrap(), *c);
          }
      }

      #[test]
      fn a_code_from_a_newer_daemon_parses_as_unknown() {
          let c: ConditionCode = serde_json::from_str("\"config_pending_restart\"").unwrap();
          assert_eq!(c, ConditionCode::Unknown);
          assert_eq!(ConditionCode::parse_lossy("tls_cert_expiring"), ConditionCode::Unknown);
      }

      #[test]
      fn runbook_anchor_is_derived_from_the_code() {
          for e in CATALOGUE.iter().chain(OFFLINE_ONLY) {
              assert_eq!(e.runbook, format!("docs/monitoring.md#cond-{}", e.code.as_str()));
          }
      }

      #[test]
      fn gates_match_spec_9_3() {
          use ConditionCode as C;
          let gated = |g: Gate| -> Vec<ConditionCode> {
              CATALOGUE.iter().filter(|e| e.gates.contains(&g)).map(|e| e.code).collect()
          };
          assert_eq!(
              gated(Gate::Readiness),
              vec![C::Starting, C::ShuttingDown, C::ListenerDown, C::NoLiveShard, C::WabCapRejecting]
          );
          assert_eq!(
              gated(Gate::Liveness),
              vec![C::ListenerDown, C::NoLiveShard, C::WorkerExited, C::DrainStopped, C::DrainWedged]
          );
      }

      #[test]
      fn severities_and_affects_match_spec_9_3() {
          use ConditionCode as C;
          let critical: Vec<ConditionCode> = CATALOGUE
              .iter()
              .filter(|e| e.severity == Severity::Critical)
              .map(|e| e.code)
              .collect();
          assert_eq!(
              critical,
              vec![
                  C::ListenerDown, C::NoLiveShard, C::ShardOffline, C::WorkerExited,
                  C::WabCapRejecting, C::DrainStopped, C::DrainWedged,
                  C::DrainBlockedDeadLetterFull, C::FsyncFailuresRecent, C::RecoveryIncomplete,
              ]
          );
          let info: Vec<ConditionCode> =
              CATALOGUE.iter().filter(|e| e.severity == Severity::Info).map(|e| e.code).collect();
          assert_eq!(info, vec![C::Starting, C::ShuttingDown, C::SinkDegraded, C::ActiveSegmentAging]);
          assert_eq!(entry(C::WabGrowingUnbounded).unwrap().affects, Affects::Durability);
          assert_eq!(entry(C::FlusherRespawnedRecent).unwrap().affects, Affects::Ingest);
          assert_eq!(entry(C::DaemonUnreachable).unwrap().severity, Severity::Critical);
          assert!(entry(C::Unknown).is_none());
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status`. Expected: `error: package ID specification `weir-status` did not match any packages` (the crate is not a member yet).
- [ ] **Step 3: Implement.**
  `crates/weir-status/Cargo.toml`:
  ```toml
  [package]
  name = "weir-status"
  description = "Status model, Conditions evaluator and probe predicates shared by the weir daemon and weir-ctl"
  version.workspace = true
  edition.workspace = true
  rust-version.workspace = true
  license.workspace = true
  repository.workspace = true
  homepage.workspace = true
  keywords = ["weir", "status", "health", "readiness", "monitoring"]
  categories = ["development-tools::debugging"]

  [dependencies]
  # The status document is a serde type that both the daemon and weir-ctl must
  # (de)serialise identically. serde is already in the lock tree (weir-server's
  # config parsing). No I/O, no clock, no tokio: `now` is always passed in.
  serde = { version = "1", features = ["derive"] }

  [dev-dependencies]
  # Golden documents and round-trip tests only. Already in the lock tree.
  serde_json = "1"
  ```
  `crates/weir-status/README.md`:
  ```markdown
  # weir-status

  The `weir.status/v1` status model for [weir](https://github.com/miki-przygoda/weir):
  the Conditions catalogue, the pure evaluator, the readiness/liveness/startup
  predicates and the Prometheus-exposition parser. The daemon and `weir-ctl`
  share it so that every consumer computes the same answer from the same facts.
  It performs no I/O and reads no clock.
  ```
  `crates/weir-status/src/lib.rs`:
  ```rust
  //! The weir status model (`weir.status/v1`, spec §5.1, §9).
  //!
  //! One pure evaluator turns [`Evidence`] into [`Evaluation`]; every consumer
  //! (`/status`, `/readyz`, `/livez`, the admin `status` verb, the
  //! `weir_condition_active` gauge, `weir-ctl status`, the readiness script)
  //! calls it, so their answers cannot drift. Missing evidence makes a condition
  //! *unevaluated*, never "false". Nothing here performs I/O or reads a clock.
  #![forbid(unsafe_code)]

  pub mod codes;

  pub use codes::{ALL, Affects, CATALOGUE, CatalogueEntry, ConditionCode, Gate, OFFLINE_ONLY, Severity, entry};
  ```
  `crates/weir-status/src/codes.rs`, above the test module:
  ```rust
  //! The Conditions catalogue (spec §9.3): one entry per code, in catalogue order.

  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  /// How bad an active condition is. Ordered: `Info < Warning < Critical`.
  #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum Severity {
      Info,
      Warning,
      Critical,
  }

  /// What a condition endangers.
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum Affects {
      Ingest,
      Delivery,
      Durability,
  }

  /// Which probe a condition can fail: `Readiness` always forces `/readyz` to
  /// 503; `Liveness` marks a check selectable in `livez_checks` (spec §9.3 R, L*).
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum Gate {
      Readiness,
      Liveness,
  }

  macro_rules! condition_codes {
      ($( $variant:ident => $name:literal ),* $(,)?) => {
          /// A condition code. `Unknown` is what an older reader makes of a code a
          /// newer daemon emits; it is shown verbatim (spec §5.1), never dropped.
          #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
          pub enum ConditionCode { $( $variant, )* Unknown }

          impl ConditionCode {
              /// The wire name (snake_case).
              pub fn as_str(self) -> &'static str {
                  match self { $( Self::$variant => $name, )* Self::Unknown => "unknown" }
              }

              /// Parses a wire name; anything unrecognised is `Unknown`.
              pub fn parse_lossy(s: &str) -> Self {
                  match s { $( $name => Self::$variant, )* _ => Self::Unknown }
              }
          }
      };
  }

  condition_codes! {
      Starting => "starting",
      ShuttingDown => "shutting_down",
      ListenerDown => "listener_down",
      NoLiveShard => "no_live_shard",
      ShardOffline => "shard_offline",
      WorkerExited => "worker_exited",
      WabCapRejecting => "wab_cap_rejecting",
      DrainBackpressure => "drain_backpressure",
      AckTimeoutsRecent => "ack_timeouts_recent",
      WabScanStale => "wab_scan_stale",
      WabGrowingUnbounded => "wab_growing_unbounded",
      DrainStopped => "drain_stopped",
      DrainWedged => "drain_wedged",
      DrainPaused => "drain_paused",
      DrainBlockedDeadLetterFull => "drain_blocked_dead_letter_full",
      DeliveryStalled => "delivery_stalled",
      DeliveryLagging => "delivery_lagging",
      SegmentsStranded => "segments_stranded",
      SinkDown => "sink_down",
      SinkDegraded => "sink_degraded",
      SinkFailing => "sink_failing",
      DeadLetterNearCap => "dead_letter_near_cap",
      NoopSink => "noop_sink",
      ActiveSegmentAging => "active_segment_aging",
      FsyncFailuresRecent => "fsync_failures_recent",
      FlusherRespawnedRecent => "flusher_respawned_recent",
      QuarantineNonempty => "quarantine_nonempty",
      RecoveryIncomplete => "recovery_incomplete",
      TlsReloadFailed => "tls_reload_failed",
      AdminUnavailable => "admin_unavailable",
      DaemonUnreachable => "daemon_unreachable",
      RecoveryPending => "recovery_pending",
  }

  impl Serialize for ConditionCode {
      fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
          s.serialize_str(self.as_str())
      }
  }

  impl<'de> Deserialize<'de> for ConditionCode {
      fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
          let s = String::deserialize(d)?;
          Ok(Self::parse_lossy(&s))
      }
  }

  /// One catalogue row (spec §9.3).
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct CatalogueEntry {
      pub code: ConditionCode,
      pub severity: Severity,
      pub affects: Affects,
      pub gates: &'static [Gate],
      pub runbook: &'static str,
  }

  const NONE: &[Gate] = &[];
  const R: &[Gate] = &[Gate::Readiness];
  const L: &[Gate] = &[Gate::Liveness];
  const RL: &[Gate] = &[Gate::Readiness, Gate::Liveness];

  macro_rules! entry {
      ($code:ident, $sev:ident, $aff:ident, $gates:expr, $anchor:literal) => {
          CatalogueEntry {
              code: ConditionCode::$code,
              severity: Severity::$sev,
              affects: Affects::$aff,
              gates: $gates,
              runbook: concat!("docs/monitoring.md#cond-", $anchor),
          }
      };
  }

  /// The 30 codes the daemon evaluates, in spec §9.3 order. Thresholds are
  /// constants (owner decision), see `Thresholds`.
  pub const CATALOGUE: &[CatalogueEntry] = &[
      entry!(Starting, Info, Ingest, R, "starting"),
      entry!(ShuttingDown, Info, Ingest, R, "shutting_down"),
      entry!(ListenerDown, Critical, Ingest, RL, "listener_down"),
      entry!(NoLiveShard, Critical, Ingest, RL, "no_live_shard"),
      entry!(ShardOffline, Critical, Ingest, NONE, "shard_offline"),
      entry!(WorkerExited, Critical, Ingest, L, "worker_exited"),
      entry!(WabCapRejecting, Critical, Ingest, R, "wab_cap_rejecting"),
      entry!(DrainBackpressure, Warning, Ingest, NONE, "drain_backpressure"),
      entry!(AckTimeoutsRecent, Warning, Ingest, NONE, "ack_timeouts_recent"),
      entry!(WabScanStale, Warning, Ingest, NONE, "wab_scan_stale"),
      entry!(WabGrowingUnbounded, Warning, Durability, NONE, "wab_growing_unbounded"),
      entry!(DrainStopped, Critical, Delivery, L, "drain_stopped"),
      entry!(DrainWedged, Critical, Delivery, L, "drain_wedged"),
      entry!(DrainPaused, Warning, Delivery, NONE, "drain_paused"),
      entry!(DrainBlockedDeadLetterFull, Critical, Delivery, NONE, "drain_blocked_dead_letter_full"),
      entry!(DeliveryStalled, Warning, Delivery, NONE, "delivery_stalled"),
      entry!(DeliveryLagging, Warning, Delivery, NONE, "delivery_lagging"),
      entry!(SegmentsStranded, Warning, Delivery, NONE, "segments_stranded"),
      entry!(SinkDown, Warning, Delivery, NONE, "sink_down"),
      entry!(SinkDegraded, Info, Delivery, NONE, "sink_degraded"),
      entry!(SinkFailing, Warning, Delivery, NONE, "sink_failing"),
      entry!(DeadLetterNearCap, Warning, Delivery, NONE, "dead_letter_near_cap"),
      entry!(NoopSink, Warning, Delivery, NONE, "noop_sink"),
      entry!(ActiveSegmentAging, Info, Delivery, NONE, "active_segment_aging"),
      entry!(FsyncFailuresRecent, Critical, Durability, NONE, "fsync_failures_recent"),
      entry!(FlusherRespawnedRecent, Warning, Ingest, NONE, "flusher_respawned_recent"),
      entry!(QuarantineNonempty, Warning, Durability, NONE, "quarantine_nonempty"),
      entry!(RecoveryIncomplete, Critical, Durability, NONE, "recovery_incomplete"),
      entry!(TlsReloadFailed, Warning, Ingest, NONE, "tls_reload_failed"),
      entry!(AdminUnavailable, Warning, Ingest, NONE, "admin_unavailable"),
  ];

  /// Emitted only by `weir-ctl` when it evaluates offline (spec §9.3).
  /// `recovery_pending` has no `affects` in §9.3; `durability` is chosen
  /// because it reports unsealed segments that recovery will seal.
  pub const OFFLINE_ONLY: &[CatalogueEntry] = &[
      entry!(DaemonUnreachable, Critical, Ingest, NONE, "daemon_unreachable"),
      entry!(RecoveryPending, Info, Durability, NONE, "recovery_pending"),
  ];

  /// Every code the daemon evaluates, in catalogue order (spec §5.1 `ALL`).
  pub const ALL: &[ConditionCode] = &[
      ConditionCode::Starting, ConditionCode::ShuttingDown, ConditionCode::ListenerDown,
      ConditionCode::NoLiveShard, ConditionCode::ShardOffline, ConditionCode::WorkerExited,
      ConditionCode::WabCapRejecting, ConditionCode::DrainBackpressure,
      ConditionCode::AckTimeoutsRecent, ConditionCode::WabScanStale,
      ConditionCode::WabGrowingUnbounded, ConditionCode::DrainStopped, ConditionCode::DrainWedged,
      ConditionCode::DrainPaused, ConditionCode::DrainBlockedDeadLetterFull,
      ConditionCode::DeliveryStalled, ConditionCode::DeliveryLagging,
      ConditionCode::SegmentsStranded, ConditionCode::SinkDown, ConditionCode::SinkDegraded,
      ConditionCode::SinkFailing, ConditionCode::DeadLetterNearCap, ConditionCode::NoopSink,
      ConditionCode::ActiveSegmentAging, ConditionCode::FsyncFailuresRecent,
      ConditionCode::FlusherRespawnedRecent, ConditionCode::QuarantineNonempty,
      ConditionCode::RecoveryIncomplete, ConditionCode::TlsReloadFailed,
      ConditionCode::AdminUnavailable,
  ];

  /// The catalogue row for `code` (daemon or offline-only); `None` for `Unknown`.
  pub fn entry(code: ConditionCode) -> Option<&'static CatalogueEntry> {
      CATALOGUE.iter().chain(OFFLINE_ONLY).find(|e| e.code == code)
  }
  ```
  Root `Cargo.toml`: in `members`, add `"crates/weir-status",` after `"crates/weir-wab",`. In `[workspace.dependencies]`, add after the `weir-wab` line:
  ```toml
  weir-status = { path = "crates/weir-status", version = "4.0.1" }
  ```
  `deploy/docker/Dockerfile`:
  - add `COPY crates/weir-status/Cargo.toml   crates/weir-status/Cargo.toml` after the `weir-wab` COPY (line 14);
  - add `crates/weir-status/src \` to the `mkdir -p` list after `crates/weir-wab/src \`;
  - add `crates/weir-status/src/lib.rs \` to the `touch` list after `crates/weir-wab/src/lib.rs \`.

  The `every_workspace_member_is_in_the_docker_builder` drift guard (tests/docs_drift.rs:496) enforces all three.

  `.github/workflows/ci.yml:498`: `cargo check -p weir-core -p weir-wab -p weir-sink-sdk -p weir-status --all-features --locked`. In the comment above it (:485-496), change "The three published libraries have no Unix API surface" to "These published libraries have no Unix API surface", change "since all three are its dependencies" to "since the first three are its dependencies", and append before the `#` blank line: `# weir-status is pure Rust (serde only, no I/O) and is checked here because weir-ctl's evaluator depends on it and must build anywhere weir-ctl might.`

  `RELEASING.md`:
  - line 8 `**Nine crates.**` becomes `**Ten crates.**`;
  - publish order (the fenced block at lines 22-23): write the order the RELEASING.md python snippet prints once weir-ctl and weir-server both depend on weir-status (this PR adds both dependencies; re-run the snippet at Task 1.28 to confirm). Expected, wrapped as today:
    ```
    weir-core → weir-wab → weir-sink-sdk → weir-attest → weir-client
              → weir-status → weir-ctl → weir-rs → weir-sink-s3 → weir-server
    ```
  - line 57 "all seven occurrences" becomes "all eight occurrences";
  - "the six intra-workspace" becomes "the seven intra-workspace";
  - `# expect 7 replacements` becomes `# expect 8 replacements`;
  - "This is the order the snippet below prints, so the two agree." stays as it is: the block above is the snippet's output.

  The other documents that enumerate the published libraries:
  - `docs/platform-support.md`: add the row `| `weir-status` | yes | **yes** | same |` after the `weir-sink-sdk` row, and make the sentence below the table read "A Rust developer on Windows can depend on `weir-core`, `weir-wab`, `weir-sink-sdk` and `weir-status` for the wire codec, the on-disk format, to write a sink, or to evaluate a daemon's status, …" (rest unchanged).
  - `CONTRIBUTING.md:142-145`: "`cargo check`s the three published libraries (`weir-core`, `weir-wab`, `weir-sink-sdk`). **Cfg-gate any Unix-only code in those four crates**" becomes "`cargo check`s the four published libraries (`weir-core`, `weir-wab`, `weir-sink-sdk`, `weir-status`). **Cfg-gate any Unix-only code in those five crates**".
  - `README.md:261-262`: "a portability check of the three published libraries (`weir-core`, `weir-wab`, `weir-sink-sdk`)" becomes "a portability check of the four published libraries (`weir-core`, `weir-wab`, `weir-sink-sdk`, `weir-status`)".
  - `README.md` crate table: after the `weir-wab` row add `| `weir-status`   | lib        | The `weir.status/v1` model, Conditions evaluator and probe predicates shared by the daemon and `weir-ctl`. Pure Rust, no I/O. Cross-platform. |`.
  - `docs/architecture.md` crate-boundary table: after the `weir-wab` row add `| `weir-status` | — (just `serde`) | The `weir.status/v1` model, Conditions evaluator and probe predicates shared by the daemon and `weir-ctl`; pure Rust, no I/O. Evaluate a daemon from its metrics exposition or from disk facts without linking the daemon. |`.
  - `demo/crates.html`, the "You want to…" table: after the `weir-wab` row (:45) insert
    ```html
          <tr><td>Interpret a daemon's status and Conditions (offline or live)</td><td><code class="inline">weir-status</code></td><td>no</td></tr>
    ```
    and after the `weir-wab` card (the `<div class="card">` whose `<h3>` is `weir-wab`, :98-104) insert
    ```html
        <div class="card">
          <h3>weir-status</h3>
          <div class="chips"><span class="chip">lib</span><span class="chip">Cross-platform</span><span class="chip">deps: serde</span></div>
          <p class="card-sub">The status model the daemon and <code class="inline">weir-ctl</code> share: the Conditions catalogue, the pure evaluator and probe predicates, and the <code class="inline">weir.status/v1</code> document types. No I/O, no clock.</p>
        </div>
    ```
    The chip says `deps: serde` because `serde_json` is only a dev-dependency here; PR 6 Task 6.1 moves it into `[dependencies]` and changes the chip to `deps: serde + serde_json`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: 6 passed. Then run `cargo test -p weir-server --test docs_drift every_workspace_member_is_in_the_docker_builder` and `cargo test -p weir-server --test demo_drift`. Expected: both pass (`the_demo_lists_every_published_crate` now finds `weir-status`).
- [ ] **Step 5: Commit.** `git add Cargo.toml Cargo.lock crates/weir-status deploy/docker/Dockerfile .github/workflows/ci.yml RELEASING.md docs/platform-support.md CONTRIBUTING.md README.md docs/architecture.md demo/crates.html && git commit -m "feat(status): add the weir-status crate with the Conditions catalogue"`

---

### Task 1.2: Evidence model, the closed-set enums, `SegmentId` and the public config subset

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/evidence.rs`, `crates/weir-status/src/segment.rs`, `crates/weir-status/src/config.rs`
- Modify: `crates/weir-status/src/lib.rs`
- Test: `mod tests` in each new file

**Interfaces:**
- Consumes: Task 1.1.
- Produces (all `pub`, re-exported from the crate root):
  ```rust
  // Closed-set enums. Each: Copy, Ord, Hash, serde snake_case, plus
  // `const ALL: &[Self]`, `fn as_str(self) -> &'static str`, `fn as_u8(self) -> u8`,
  // `fn from_u8(u8) -> Option<Self>` (index into ALL).
  pub enum Phase { Initializing, Recovering, Handoff, Binding, Serving, ShuttingDown } // + fn before_serving(self) -> bool
  pub enum Source { Daemon, Metrics, Disk, MetricsAndDisk }                         // Default = Daemon
  pub enum ListenerKind { Unix, Tcp }
  pub enum ListenerState { NotConfigured, Pending, Bound, Closed, Failed }
  pub enum ShardState { Starting, Up, Respawning, Offline, Stopped }
  pub enum OfflineReason { RespawnExhausted, CounterScanFailed, FlusherExited }
  pub enum DrainStateCode { Draining, RetryingTransient, BlockedDeadLetterFull, Stopped }
  pub enum DrainExitCode { Running, Stopped, GaveUp }
  pub enum SinkHealthCode { Healthy, Degraded, Down }
  pub enum SinkFailureKind { Transient, Permanent, Timeout, ContractViolation }
  pub enum AdminState { Disabled, Bound, Unavailable }
  pub enum AdminReason { ParentInsecure, PathTooLong, InUse, BindFailed }
  pub enum ReloadTriggerCode { Sighup, Admin }
  pub enum ReloadOutcomeCode { Applied, Noop, Failed }
  pub enum ReloadItem { TlsCertificates, ConfigFile }
  pub enum ReloadItemOutcome { Reloaded, Failed, NotConfigured }
  pub enum NotReloadedReason { RestartRequired }

  pub struct SegmentId { pub shard: u16, pub counter: u64 }   // Display "shard_03/seg_00000042"
  impl SegmentId { pub fn parse(s: &str) -> Option<Self>; pub fn from_path(p: &std::path::Path) -> Option<Self>; }

  pub enum ConfigValue { Bool(bool), U64(u64), I64(i64), Str(String), Null }    // serde untagged
  pub struct PublicConfig { pub shard_count, worker_count, wab_max_bytes, dead_letter_max_bytes,
      wab_segment_max_bytes, wab_segment_max_age_secs, wab_segment_max_lifetime_secs,
      sink_max_retries, sink_timeout_secs, health_poll_interval_secs, shutdown_timeout_secs,
      drain_shutdown_budget_secs, max_connections: Option<u64>,
      pub peer_uid_check: Option<bool>, pub tls_enabled: Option<bool> }   // Default; no String field
  impl PublicConfig { pub fn from_view(view: &BTreeMap<String, ConfigValue>) -> Self; }

  pub struct Evidence { /* field list in Step 3 */ }
  impl Evidence { pub fn empty(now_unix_ms: u64, source: Source) -> Self; }
  pub struct IdentityEvidence, ListenerEvidence, ShardEvidence, WorkerEvidence, CapEvidence,
      ChannelEvidence, DrainEvidence, BusyEvidence, InFlightEvidence, RequeueEvidence, SinkEvidence,
      BacklogEvidence, RecentCounters, ReloadEvidence, ReloadItemEvidence, NotReloadedEvidence,
      AdminEvidence, ConfigEvidence;
  impl ShardEvidence { pub fn ingest_capable(&self) -> bool; }
  ```

- [ ] **Step 1: Write the failing tests.**
  `crates/weir-status/src/segment.rs` (test module only for now):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use std::path::Path;

      #[test]
      fn segment_id_renders_and_parses_the_wab_layout() {
          let id = SegmentId { shard: 3, counter: 42 };
          assert_eq!(id.to_string(), "shard_03/seg_00000042");
          for s in ["shard_03/seg_00000042", "shard_03/seg_00000042.wab", "shard_03/seg_00000042.wab.sealed"] {
              assert_eq!(SegmentId::parse(s), Some(id), "{s}");
          }
          assert_eq!(
              SegmentId::from_path(Path::new("/var/lib/weir/wab/shard_03/seg_00000042.wab.sealed")),
              Some(id)
          );
          assert_eq!(SegmentId::parse("shard_255/seg_1").unwrap().shard, 255);
      }

      #[test]
      fn anything_else_is_not_a_segment_id() {
          for s in [
              "", "seg_00000001", "shard_00/", "shard_00/seg_", "shard_xx/seg_00000001",
              "dead_letter/dl_00000001.wab.sealed", "shard_00/seg_00000001.wab.confirmed",
              "shard_00/seg_00000001.wab.confirmed.tmp", "shard_00/seg_00000001 extra",
          ] {
              assert_eq!(SegmentId::parse(s), None, "{s:?}");
          }
      }

      #[test]
      fn segment_id_serialises_as_its_display_string() {
          let id = SegmentId { shard: 0, counter: 7 };
          let json = serde_json::to_string(&id).unwrap();
          assert_eq!(json, "\"shard_00/seg_00000007\"");
          assert_eq!(serde_json::from_str::<SegmentId>(&json).unwrap(), id);
          assert!(serde_json::from_str::<SegmentId>("\"not a segment\"").is_err());
      }
  }
  ```
  `crates/weir-status/src/evidence.rs` (test module only for now):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      fn all_wire_names<T: Copy + Serialize + std::fmt::Debug>(all: &[T], name: fn(T) -> &'static str) {
          for v in all {
              assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{}\"", name(*v)), "{v:?}");
          }
      }

      #[test]
      fn closed_set_enums_serialise_as_their_as_str() {
          all_wire_names(Phase::ALL, Phase::as_str);
          all_wire_names(ListenerState::ALL, ListenerState::as_str);
          all_wire_names(ShardState::ALL, ShardState::as_str);
          all_wire_names(OfflineReason::ALL, OfflineReason::as_str);
          all_wire_names(DrainStateCode::ALL, DrainStateCode::as_str);
          all_wire_names(DrainExitCode::ALL, DrainExitCode::as_str);
          all_wire_names(SinkHealthCode::ALL, SinkHealthCode::as_str);
          all_wire_names(SinkFailureKind::ALL, SinkFailureKind::as_str);
          all_wire_names(AdminState::ALL, AdminState::as_str);
          all_wire_names(AdminReason::ALL, AdminReason::as_str);
          all_wire_names(ReloadItem::ALL, ReloadItem::as_str);
          assert_eq!(Phase::ShuttingDown.as_str(), "shutting_down");
          assert_eq!(SinkFailureKind::ContractViolation.as_str(), "contract_violation");
      }

      #[test]
      fn u8_encoding_round_trips_and_rejects_out_of_range() {
          for p in Phase::ALL {
              assert_eq!(Phase::from_u8(p.as_u8()), Some(*p));
          }
          assert_eq!(Phase::from_u8(6), None);
          assert_eq!(ListenerState::from_u8(ListenerState::Failed.as_u8()), Some(ListenerState::Failed));
      }

      #[test]
      fn only_the_four_startup_phases_are_before_serving() {
          let before: Vec<Phase> = Phase::ALL.iter().copied().filter(|p| p.before_serving()).collect();
          assert_eq!(before, vec![Phase::Initializing, Phase::Recovering, Phase::Handoff, Phase::Binding]);
      }

      #[test]
      fn a_shard_is_ingest_capable_only_when_up_or_respawning_with_its_worker_running() {
          let shard = |state, worker_running| ShardEvidence {
              id: 0, state, reason: None, since_unix_ms: 0, respawns: 0, worker_running,
              handoff_blocked_since_unix_ms: None, active_segment_age_secs: None,
          };
          assert!(shard(ShardState::Up, true).ingest_capable());
          assert!(shard(ShardState::Respawning, true).ingest_capable());
          assert!(!shard(ShardState::Up, false).ingest_capable());
          for s in [ShardState::Starting, ShardState::Offline, ShardState::Stopped] {
              assert!(!shard(s, true).ingest_capable(), "{s:?}");
          }
      }

      #[test]
      fn empty_evidence_knows_only_the_time_and_the_source() {
          let e = Evidence::empty(1_000, Source::Disk);
          assert_eq!(e.now_unix_ms, 1_000);
          assert_eq!(e.source, Source::Disk);
          assert!(e.phase.is_none() && e.shards.is_none() && e.drain.is_none() && e.config.is_none());
          assert_eq!(Evidence::default().source, Source::Daemon);
      }
  }
  ```
  `crates/weir-status/src/config.rs` (test module only for now):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn public_config_takes_only_the_fixed_public_keys() {
          let mut view = BTreeMap::new();
          view.insert("shard_count".to_string(), ConfigValue::U64(4));
          view.insert("peer_uid_check".to_string(), ConfigValue::Bool(true));
          view.insert("tls_enabled".to_string(), ConfigValue::Bool(false));
          view.insert("sink_url".to_string(), ConfigValue::Str("https://h:443".into()));
          view.insert("wab_dir".to_string(), ConfigValue::Str("/var/lib/weir/wab".into()));
          let p = PublicConfig::from_view(&view);
          assert_eq!(p.shard_count, Some(4));
          assert_eq!(p.peer_uid_check, Some(true));
          assert_eq!(p.tls_enabled, Some(false));
          assert_eq!(p.worker_count, None);
          let json = serde_json::to_string(&p).unwrap();
          assert!(!json.contains("https") && !json.contains("/var/lib"), "{json}");
      }

      #[test]
      fn config_values_serialise_untagged() {
          let v = vec![ConfigValue::Bool(true), ConfigValue::U64(5), ConfigValue::I64(-1),
                       ConfigValue::Str("x".into()), ConfigValue::Null];
          assert_eq!(serde_json::to_string(&v).unwrap(), r#"[true,5,-1,"x",null]"#);
      }
  }
  ```
  Add `pub mod config; pub mod evidence; pub mod segment;` to lib.rs.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status`. Expected: compile errors `cannot find type `SegmentId` in this scope`, `cannot find type `Phase``, `cannot find type `PublicConfig``.
- [ ] **Step 3: Implement.**
  `segment.rs`, above the tests:
  ```rust
  //! A sealed or active segment's identity, `shard_NN/seg_NNNNNNNN`, as the
  //! drain's `segment_identity` names it (wab/segment.rs:341) minus the
  //! extension. A structured type, so a public document can carry a segment
  //! without carrying a free-form path.
  use serde::{Deserialize, Deserializer, Serialize, Serializer};
  use std::fmt;
  use std::path::Path;

  #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub struct SegmentId {
      pub shard: u16,
      pub counter: u64,
  }

  impl fmt::Display for SegmentId {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          write!(f, "shard_{:02}/seg_{:08}", self.shard, self.counter)
      }
  }

  impl SegmentId {
      /// Parses `shard_<n>/seg_<digits>` with an optional `.wab` or
      /// `.wab.sealed` suffix. Anything else (dead-letter files, sidecars,
      /// temp files, free text) is `None`.
      pub fn parse(s: &str) -> Option<Self> {
          let (shard, seg) = s.split_once('/')?;
          let shard: u16 = shard.strip_prefix("shard_")?.parse().ok()?;
          let digits = seg.strip_prefix("seg_")?;
          let end = digits.find(|c: char| !c.is_ascii_digit()).unwrap_or(digits.len());
          if end == 0 {
              return None;
          }
          let rest = &digits[end..];
          if !(rest.is_empty() || rest == ".wab" || rest == ".wab.sealed") {
              return None;
          }
          Some(Self { shard, counter: digits[..end].parse().ok()? })
      }

      /// The identity of a segment file `<..>/shard_NN/seg_NNNNNNNN[.wab[.sealed]]`.
      pub fn from_path(path: &Path) -> Option<Self> {
          let file = path.file_name()?.to_str()?;
          let shard = path.parent()?.file_name()?.to_str()?;
          Self::parse(&format!("{shard}/{file}"))
      }
  }

  impl Serialize for SegmentId {
      fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
          s.collect_str(self)
      }
  }

  impl<'de> Deserialize<'de> for SegmentId {
      fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
          let s = String::deserialize(d)?;
          Self::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("not a segment id: {s:?}")))
      }
  }
  ```
  `config.rs`, above the tests:
  ```rust
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
  ```
  `evidence.rs`, above the tests:
  ```rust
  //! The facts the evaluator reads (spec §5.1 `Evidence`). Every sub-record is
  //! `Option`: `None` means "not known", which makes the conditions that need
  //! it unevaluated. Evidence may carry admin-only free text (`last_panic`,
  //! `last_error`, paths); it deliberately does NOT implement `Serialize`, so it
  //! cannot be sent anywhere except through a document type.
  use crate::config::{ConfigValue, PublicConfig};
  use crate::segment::SegmentId;
  use serde::{Deserialize, Serialize};
  use std::collections::BTreeMap;

  macro_rules! closed_set {
      ($(#[$m:meta])* $name:ident { $($v:ident => $s:literal),* $(,)? }) => {
          $(#[$m])*
          #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
          #[serde(rename_all = "snake_case")]
          pub enum $name { $($v),* }

          impl $name {
              pub const ALL: &'static [$name] = &[$($name::$v),*];
              pub fn as_str(self) -> &'static str { match self { $($name::$v => $s),* } }
              pub fn as_u8(self) -> u8 { self as u8 }
              pub fn from_u8(v: u8) -> Option<Self> { Self::ALL.get(usize::from(v)).copied() }
          }
      };
  }

  closed_set!(
      /// Startup and shutdown phases (spec §6), in order.
      Phase { Initializing => "initializing", Recovering => "recovering", Handoff => "handoff",
              Binding => "binding", Serving => "serving", ShuttingDown => "shutting_down" });
  closed_set!(Source { Daemon => "daemon", Metrics => "metrics", Disk => "disk",
                       MetricsAndDisk => "metrics_and_disk" });
  closed_set!(ListenerKind { Unix => "unix", Tcp => "tcp" });
  closed_set!(ListenerState { NotConfigured => "not_configured", Pending => "pending",
                              Bound => "bound", Closed => "closed", Failed => "failed" });
  closed_set!(ShardState { Starting => "starting", Up => "up", Respawning => "respawning",
                           Offline => "offline", Stopped => "stopped" });
  closed_set!(OfflineReason { RespawnExhausted => "respawn_exhausted",
                              CounterScanFailed => "counter_scan_failed",
                              FlusherExited => "flusher_exited" });
  closed_set!(DrainStateCode { Draining => "draining", RetryingTransient => "retrying_transient",
                               BlockedDeadLetterFull => "blocked_dead_letter_full",
                               Stopped => "stopped" });
  closed_set!(DrainExitCode { Running => "running", Stopped => "stopped", GaveUp => "gave_up" });
  closed_set!(SinkHealthCode { Healthy => "healthy", Degraded => "degraded", Down => "down" });
  closed_set!(SinkFailureKind { Transient => "transient", Permanent => "permanent",
                                Timeout => "timeout", ContractViolation => "contract_violation" });
  closed_set!(AdminState { Disabled => "disabled", Bound => "bound", Unavailable => "unavailable" });
  closed_set!(AdminReason { ParentInsecure => "parent_insecure", PathTooLong => "path_too_long",
                            InUse => "in_use", BindFailed => "bind_failed" });
  closed_set!(ReloadTriggerCode { Sighup => "sighup", Admin => "admin" });
  closed_set!(ReloadOutcomeCode { Applied => "applied", Noop => "noop", Failed => "failed" });
  closed_set!(ReloadItem { TlsCertificates => "tls_certificates", ConfigFile => "config_file" });
  closed_set!(ReloadItemOutcome { Reloaded => "reloaded", Failed => "failed",
                                  NotConfigured => "not_configured" });
  closed_set!(NotReloadedReason { RestartRequired => "restart_required" });

  impl Phase {
      /// `phase < serving` (spec §9.3 `starting`). `ShuttingDown` is not.
      pub fn before_serving(self) -> bool {
          self < Phase::Serving
      }
  }

  // closed_set! cannot place `#[default]` on a variant, so this one is by hand.
  #[allow(clippy::derivable_impls)]
  impl Default for Source {
      fn default() -> Self {
          Source::Daemon
      }
  }

  #[derive(Clone, Debug, Default, PartialEq)]
  pub struct Evidence {
      pub now_unix_ms: u64,
      pub source: Source,
      pub identity: Option<IdentityEvidence>,
      pub started_at_unix_ms: Option<u64>,
      pub phase: Option<Phase>,
      pub phase_since_unix_ms: Option<u64>,
      pub ever_served: Option<bool>,
      pub shutting_down: Option<bool>,
      pub shutting_down_since_unix_ms: Option<u64>,
      pub forced: Option<bool>,
      pub drain_deadline_unix_ms: Option<u64>,
      pub drain_budget_secs: Option<u64>,
      pub listeners: Option<Vec<ListenerEvidence>>,
      pub shards: Option<Vec<ShardEvidence>>,
      pub workers: Option<Vec<WorkerEvidence>>,
      pub queue_depth: Option<u64>,
      pub wab_cap: Option<CapEvidence>,
      pub channel: Option<ChannelEvidence>,
      pub drain: Option<DrainEvidence>,
      pub sink: Option<SinkEvidence>,
      pub backlog: Option<BacklogEvidence>,
      pub recent: Option<RecentCounters>,
      pub reload: Option<ReloadEvidence>,
      pub admin: Option<AdminEvidence>,
      pub config: Option<ConfigEvidence>,
  }

  impl Evidence {
      /// Nothing known except the time and where the (missing) facts came from.
      pub fn empty(now_unix_ms: u64, source: Source) -> Self {
          Evidence { now_unix_ms, source, ..Evidence::default() }
      }
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct IdentityEvidence {
      pub version: String,
      pub wire_version: u8,
      pub pid: u32,
      pub started_at_unix_ms: u64,
      pub wab_dir: String,
      pub socket_path: String,
      pub wab_dir_dev: u64,
      pub wab_dir_ino: u64,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct ListenerEvidence {
      pub kind: ListenerKind,
      pub state: ListenerState,
  }

  #[derive(Clone, Debug, PartialEq)]
  pub struct ShardEvidence {
      pub id: u16,
      pub state: ShardState,
      pub reason: Option<OfflineReason>,
      pub since_unix_ms: u64,
      pub respawns: u32,
      pub worker_running: bool,
      pub handoff_blocked_since_unix_ms: Option<u64>,
      pub active_segment_age_secs: Option<f64>,
  }

  impl ShardEvidence {
      /// Spec §5.3: `Up|Respawning` AND the owning worker is running.
      /// `Respawning` counts because the bounded batch queue survives a respawn.
      pub fn ingest_capable(&self) -> bool {
          matches!(self.state, ShardState::Up | ShardState::Respawning) && self.worker_running
      }
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct WorkerEvidence {
      pub id: u16,
      pub running: bool,
      /// Set once the worker thread has exited; `None` for one never started.
      pub exited_at_unix_ms: Option<u64>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct CapEvidence {
      pub max_bytes: u64,
      pub low_water_bytes: u64,
      pub bytes_now: u64,
      pub rejecting: bool,
      pub first_scan_ok_unix_ms: Option<u64>,
      pub last_scan_ok_unix_ms: Option<u64>,
      /// The existing `should_warn_wab_growth` verdict on the last window.
      pub growing_unbounded: Option<bool>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct ChannelEvidence {
      pub depth: u64,
      pub capacity: u64,
      /// When the depth last rose to >= the high-water threshold; `None` below it.
      pub high_since_unix_ms: Option<u64>,
  }

  #[derive(Clone, Debug, PartialEq)]
  pub struct DrainEvidence {
      pub state: DrainStateCode,
      pub state_since_unix_ms: u64,
      pub pause_requested: bool,
      pub paused: bool,
      pub held: bool,
      pub paused_since_unix_ms: Option<u64>,
      pub paused_by_uid: Option<u32>,
      pub generation: u64,
      pub observed_requeue: u64,
      pub in_flight: Option<InFlightEvidence>,
      pub commit_in_flight_since_unix_ms: Option<u64>,
      /// The safety floor's busy signal (`DrainHeartbeat::busy()`): what the
      /// drain thread is inside right now; `None` between operations or unknown.
      pub busy: Option<BusyEvidence>,
      pub last_progress_unix_ms: Option<u64>,
      pub heartbeat_age_secs: Option<f64>,
      pub pending_segments: u64,
      pub backlog_segments: u64,
      pub replay_total: u64,
      pub replay_remaining: u64,
      pub blocked_since_unix_ms: Option<u64>,
      pub last_requeue: Option<RequeueEvidence>,
      pub exit: DrainExitCode,
      /// Admin-only free text: the last panic payload.
      pub last_panic: Option<String>,
  }

  /// One reading of the drain's busy signal. `sink_call` is true inside a sink
  /// commit or health probe, false inside local I/O (segment read, fsync,
  /// dead-letter write, quarantine move, directory scan), which is never a
  /// wedge (main.rs `classify_drain_stall`, `SlowLocalIo`).
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct BusyEvidence {
      pub sink_call: bool,
      pub since_unix_ms: u64,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct InFlightEvidence {
      pub segment: SegmentId,
      pub attempt: u32,
      pub retries_left: u32,
      pub processed_records: u64,
      pub next_retry_at_unix_ms: Option<u64>,
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct RequeueEvidence {
      pub ticket: u64,
      pub requeued: u64,
      pub already_queued: u64,
      pub backoff_cut: bool,
      pub coalesced: bool,
      /// Admin-only free text.
      pub scan_error: Option<String>,
      pub at_unix_ms: u64,
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct SinkEvidence {
      pub health: Option<SinkHealthCode>,
      pub health_since_unix_ms: Option<u64>,
      pub last_success_unix_ms: Option<u64>,
      pub last_failure_unix_ms: Option<u64>,
      pub last_failure_kind: Option<SinkFailureKind>,
      pub consecutive_failures: u64,
      /// Admin-only free text (already redacted and capped by the writer).
      pub last_error: Option<String>,
      /// Admin-only free text.
      pub probe_reason: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq)]
  pub struct BacklogEvidence {
      pub undelivered_segments: u64,
      pub undelivered_records: u64,
      /// Upper bound: measured from the segment header's `created_at`.
      pub oldest_undelivered_age_secs: Option<f64>,
      pub dead_letter_bytes: u64,
      pub dead_letter_max_bytes: Option<u64>,
      pub quarantine_bytes: u64,
      pub quarantine_files: u64,
      pub scanned_at_ms: u64,
  }

  /// Counter deltas over fixed windows, computed by the producer of the
  /// evidence (the daemon's 5 s poll task). `None` = no window available
  /// (a single offline scrape has no history).
  #[derive(Clone, Debug, Default, PartialEq, Eq)]
  pub struct RecentCounters {
      pub ack_timeouts_5m: Option<u64>,
      pub fsync_failures_15m: Option<u64>,
      pub flusher_panics_15m: Option<u64>,
      pub stranded_since_last_success: Option<u64>,
      /// Absolute, this process.
      pub drain_panics_total: Option<u64>,
      pub recovery_segments_failed: Option<u64>,
      pub recovery_quarantine_copy_failed: Option<u64>,
      pub coalesce_ewma_us: Option<u64>,
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct ReloadEvidence {
      pub trigger: ReloadTriggerCode,
      pub at_unix_ms: u64,
      pub outcome: ReloadOutcomeCode,
      /// The last TLS reload attempt failed and none has succeeded since (sticky).
      pub tls_last_failed: bool,
      pub items: Vec<ReloadItemEvidence>,
      pub not_reloaded: Vec<NotReloadedEvidence>,
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct ReloadItemEvidence {
      pub item: ReloadItem,
      pub outcome: ReloadItemOutcome,
      /// Admin-only free text.
      pub detail: Option<String>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub struct NotReloadedEvidence {
      pub item: ReloadItem,
      pub reason_code: NotReloadedReason,
  }

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct AdminEvidence {
      pub enabled: bool,
      pub state: AdminState,
      pub reason: Option<AdminReason>,
      /// Admin-only (an absolute path).
      pub path: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq)]
  pub struct ConfigEvidence {
      /// A closed-set code (`noop`, `http`, ...), not a URL.
      pub sink_type: String,
      /// `max(2 × sink_timeout_secs, 60)`, the drain's backstop.
      pub commit_timeout_secs: Option<u64>,
      pub public: PublicConfig,
      /// The full ConfigView (admin only).
      pub view: BTreeMap<String, ConfigValue>,
  }
  ```
  lib.rs re-exports:
  ```rust
  pub mod config;
  pub mod evidence;
  pub mod segment;

  pub use config::{ConfigValue, PublicConfig};
  pub use evidence::*;
  pub use segment::SegmentId;
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: 16 passed.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): the evidence model, closed-set codes and SegmentId"` (stage the new files first: `git add crates/weir-status/src`).

---

### Task 1.3: `Condition`, `EvidenceValue`, `Evaluation`, `Thresholds`, `SinceLookup` and typed summary templates

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/condition.rs`, `crates/weir-status/src/summary.rs`
- Modify: `crates/weir-status/src/lib.rs`
- Test: `mod tests` in both

**Interfaces:**
- Consumes: Tasks 1.1-1.2.
- Produces:
  ```rust
  #[serde(untagged)] pub enum EvidenceValue { U64(u64), F64(f64), Bool(bool), Segment(SegmentId),
      Shards(Vec<u16>), Code(String), Text(String) /* admin only */ }
  pub struct Condition { pub code: ConditionCode, pub severity: Severity, pub affects: Affects,
      pub since_unix_ms: Option<u64> /* JSON key "since_ms" */, pub summary: String,
      pub detail: Option<String>, pub evidence: BTreeMap<String, EvidenceValue>,
      pub runbook: Cow<'static, str>, pub gates: Vec<Gate> }      // Clone, Debug, PartialEq, serde
  pub struct Evaluation { pub conditions: Vec<Condition>, pub unevaluated: Vec<ConditionCode> }
  impl Evaluation { pub fn is_active(&self, c: ConditionCode) -> bool; pub fn get(&self, c) -> Option<&Condition>; }
  pub trait SinceLookup { fn since(&self, code: ConditionCode) -> Option<u64>; }
  pub struct NoLatches;  impl SinceLookup for NoLatches
  pub struct Thresholds { pub handoff_blocked_ms: u64, pub channel_high_depth: u64,
      pub channel_high_ms: u64, pub wab_scan_stale_ms: u64, pub wedge_grace_ms: u64,
      pub delivery_stalled_ms: u64, pub delivery_lagging_secs: f64,
      pub sink_failing_consecutive: u64, pub dead_letter_near_cap_percent: u64,
      pub active_segment_aging_secs: f64, pub coalesce_slow_us: u64 }
  impl Thresholds { pub const DEFAULT: Thresholds; }  impl Default for Thresholds
  pub const ACK_TIMEOUT_WINDOW_SECS: u64 = 300;
  pub const FSYNC_WINDOW_SECS: u64 = 900;
  pub const PANIC_WINDOW_SECS: u64 = 900;
  pub const DETAIL_PANIC_MAX_BYTES: usize = 256;
  ```

- [ ] **Step 1: Write the failing tests.** `crates/weir-status/src/condition.rs` (test module only):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      fn sample(code: ConditionCode) -> Condition {
          let mut evidence = BTreeMap::new();
          evidence.insert("shards".to_string(), EvidenceValue::Shards(vec![1, 2]));
          evidence.insert("bytes".to_string(), EvidenceValue::U64(5));
          Condition {
              code, severity: Severity::Critical, affects: Affects::Ingest, since_unix_ms: Some(10),
              summary: "s".into(), detail: Some("d".into()), evidence,
              runbook: Cow::Borrowed("docs/monitoring.md#cond-shard_offline"), gates: vec![],
          }
      }

      #[test]
      fn a_condition_serialises_since_as_since_ms() {
          let json = serde_json::to_value(sample(ConditionCode::ShardOffline)).unwrap();
          assert_eq!(json["since_ms"], 10);
          assert!(json.get("since_unix_ms").is_none());
          assert_eq!(json["code"], "shard_offline");
          assert_eq!(json["evidence"]["shards"], serde_json::json!([1, 2]));
      }

      #[test]
      fn a_known_condition_round_trips() {
          let c = sample(ConditionCode::ShardOffline);
          let back: Condition = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
          assert_eq!(back, c);
      }

      #[test]
      fn an_unknown_code_is_kept_verbatim_with_its_wire_severity() {
          let json = r#"{"code":"brand_new_code","severity":"warning","affects":"delivery",
              "since_ms":null,"summary":"x","evidence":{},"runbook":"r","gates":[]}"#;
          let c: Condition = serde_json::from_str(json).unwrap();
          assert_eq!(c.code, ConditionCode::Unknown);
          assert_eq!(c.severity, Severity::Warning);
          assert_eq!(c.evidence.get("code_raw"), Some(&EvidenceValue::Code("brand_new_code".into())));
          let again = serde_json::to_value(&c).unwrap();
          assert_eq!(again["code"], "brand_new_code", "re-emitted verbatim, never as \"unknown\"");
      }

      #[test]
      fn defaults_are_the_spec_constants() {
          let t = Thresholds::default();
          assert_eq!(t, Thresholds::DEFAULT);
          assert_eq!(t.channel_high_depth, 230);
          assert_eq!(t.wedge_grace_ms, 15_000);
          assert_eq!(t.delivery_lagging_secs, 900.0);
          assert_eq!(NoLatches.since(ConditionCode::Starting), None);
      }
  }
  ```
  `crates/weir-status/src/summary.rs`:
  ```rust
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
              let Some(sig) = line.strip_prefix("pub(crate) fn ") else { continue };
              n += 1;
              let (Some(open), Some(close)) = (sig.find('('), sig.find(") -> String {")) else {
                  panic!("keep the signature on one line: {line}");
              };
              assert!(!sig[..open].contains('<'), "a template cannot be generic: {line}");
              // Whole words, not substrings: `&'a str`, `Cow<'_, str>` and
              // `Box<str>` are caught; a parameter named `stranded` is not.
              for word in sig[open + 1..close].split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
                  for banned in ["str", "String", "Path", "PathBuf", "OsStr", "OsString", "Cow", "dyn", "impl"] {
                      assert_ne!(word, banned, "free-text parameter `{banned}` in: {line}");
                  }
              }
          }
          assert!(n == 0 || n >= 1, "parsed {n} templates");
      }
  }
  ```
  (The final assertion becomes `n == 30` in Task 1.6, once every template exists.)

  Add `pub mod condition;` to lib.rs (summary.rs stays undeclared until Step 3), so the new tests compile and fail.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status`. Expected: compile errors `cannot find type `Condition`` / `Thresholds` / `NoLatches` (summary.rs is not yet a module, so its test does not run).
- [ ] **Step 3: Implement.** `condition.rs`, above the tests:
  ```rust
  //! Conditions and their evaluation result (spec §5.1).
  use crate::codes::{Affects, ConditionCode, Gate, Severity};
  use crate::segment::SegmentId;
  use serde::{Deserialize, Serialize};
  use std::borrow::Cow;
  use std::collections::BTreeMap;

  /// One evidence value attached to a condition. `Text` is admin-only free
  /// text; the public rendering (PR 5) has no variant to put it in.
  /// Untagged; on deserialise a plain string reads as `Segment` when it is a
  /// segment id, else `Code` (a deserialised document is for display only).
  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  #[serde(untagged)]
  pub enum EvidenceValue {
      U64(u64),
      F64(f64),
      Bool(bool),
      Segment(SegmentId),
      Shards(Vec<u16>),
      Code(String),
      Text(String),
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  #[serde(from = "RawCondition", into = "RawCondition")]
  pub struct Condition {
      pub code: ConditionCode,
      pub severity: Severity,
      pub affects: Affects,
      pub since_unix_ms: Option<u64>,
      /// PUBLIC: rendered from a typed template (summary.rs).
      pub summary: String,
      /// ADMIN ONLY: may carry free text (panic payload, uid, sink error).
      pub detail: Option<String>,
      pub evidence: BTreeMap<String, EvidenceValue>,
      pub runbook: Cow<'static, str>,
      pub gates: Vec<Gate>,
  }

  /// The wire shape: the code as a raw string, so an unknown code survives a
  /// parse/serialise round trip unchanged (spec §5.1 rules).
  #[derive(Serialize, Deserialize)]
  struct RawCondition {
      code: String,
      severity: Severity,
      affects: Affects,
      since_ms: Option<u64>,
      summary: String,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      detail: Option<String>,
      #[serde(default)]
      evidence: BTreeMap<String, EvidenceValue>,
      runbook: String,
      #[serde(default)]
      gates: Vec<Gate>,
  }

  impl From<RawCondition> for Condition {
      fn from(r: RawCondition) -> Self {
          let code = ConditionCode::parse_lossy(&r.code);
          let mut evidence = r.evidence;
          if code == ConditionCode::Unknown {
              evidence.insert("code_raw".to_string(), EvidenceValue::Code(r.code));
          }
          Condition {
              code, severity: r.severity, affects: r.affects, since_unix_ms: r.since_ms,
              summary: r.summary, detail: r.detail, evidence,
              runbook: Cow::Owned(r.runbook), gates: r.gates,
          }
      }
  }

  impl From<Condition> for RawCondition {
      fn from(c: Condition) -> Self {
          let code = match (c.code, c.evidence.get("code_raw")) {
              (ConditionCode::Unknown, Some(EvidenceValue::Code(raw))) => raw.clone(),
              (code, _) => code.as_str().to_string(),
          };
          RawCondition {
              code, severity: c.severity, affects: c.affects, since_ms: c.since_unix_ms,
              summary: c.summary, detail: c.detail, evidence: c.evidence,
              runbook: c.runbook.into_owned(), gates: c.gates,
          }
      }
  }

  #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
  pub struct Evaluation {
      /// Active conditions, in catalogue order.
      pub conditions: Vec<Condition>,
      /// Codes whose evidence was missing. Never shown as healthy.
      pub unevaluated: Vec<ConditionCode>,
  }

  impl Evaluation {
      pub fn is_active(&self, code: ConditionCode) -> bool {
          self.conditions.iter().any(|c| c.code == code)
      }
      pub fn get(&self, code: ConditionCode) -> Option<&Condition> {
          self.conditions.iter().find(|c| c.code == code)
      }
  }

  /// First-seen time of an active condition (the daemon's latches).
  pub trait SinceLookup {
      fn since(&self, code: ConditionCode) -> Option<u64>;
  }

  /// No latches (offline evaluation): `since` is unknown.
  #[derive(Clone, Copy, Debug, Default)]
  pub struct NoLatches;

  impl SinceLookup for NoLatches {
      fn since(&self, _code: ConditionCode) -> Option<u64> {
          None
      }
  }

  /// Condition thresholds. Constants, not knobs (owner decision, spec §9.3).
  #[derive(Clone, Copy, Debug, PartialEq)]
  pub struct Thresholds {
      pub handoff_blocked_ms: u64,
      pub channel_high_depth: u64,
      pub channel_high_ms: u64,
      pub wab_scan_stale_ms: u64,
      pub wedge_grace_ms: u64,
      pub delivery_stalled_ms: u64,
      pub delivery_lagging_secs: f64,
      pub sink_failing_consecutive: u64,
      pub dead_letter_near_cap_percent: u64,
      pub active_segment_aging_secs: f64,
      pub coalesce_slow_us: u64,
  }

  impl Thresholds {
      pub const DEFAULT: Thresholds = Thresholds {
          handoff_blocked_ms: 1_000,
          channel_high_depth: 230,
          channel_high_ms: 10_000,
          wab_scan_stale_ms: 30_000,
          wedge_grace_ms: 15_000,
          delivery_stalled_ms: 300_000,
          delivery_lagging_secs: 900.0,
          sink_failing_consecutive: 3,
          dead_letter_near_cap_percent: 80,
          active_segment_aging_secs: 3_600.0,
          coalesce_slow_us: 1_000_000,
      };
  }

  impl Default for Thresholds {
      fn default() -> Self {
          Self::DEFAULT
      }
  }

  /// Windows the daemon applies when it computes `RecentCounters`.
  pub const ACK_TIMEOUT_WINDOW_SECS: u64 = 300;
  pub const FSYNC_WINDOW_SECS: u64 = 900;
  pub const PANIC_WINDOW_SECS: u64 = 900;
  /// `drain_stopped`'s admin detail carries at most this much panic text.
  pub const DETAIL_PANIC_MAX_BYTES: usize = 256;
  ```
  lib.rs: add `mod summary;` and `pub use condition::*;`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: 21 passed (including `summary_templates_take_no_free_text`).
- [ ] **Step 5: Commit.** `git add crates/weir-status/src && git commit -m "feat(status): conditions, thresholds and the typed-summary rule"`

---
### Task 1.4: The evaluator and the ingest and lifecycle rules (§9.3 codes 1-11)

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/rules.rs`, `crates/weir-status/src/testing.rs`
- Modify: `crates/weir-status/src/summary.rs`: add templates 1-11 above `#[cfg(test)]`
- Modify: `crates/weir-status/src/lib.rs`
- Test: `crates/weir-status/src/rules.rs` (`mod tests`)

**Interfaces:**
- Consumes: Tasks 1.1-1.3.
- Produces:
  ```rust
  pub fn evaluate(e: &Evidence, t: &Thresholds, latches: &dyn SinceLookup) -> Evaluation;
  #[doc(hidden)] pub mod testing { pub fn serving_evidence(now_unix_ms: u64) -> Evidence; }
  // crate-internal, used by probes (Task 1.7):
  pub(crate) enum Outcome { Unevaluated, Inactive, Active(Fired) }
  pub(crate) struct Fired { pub(crate) summary: String, pub(crate) detail: Option<String>,
                            pub(crate) evidence: BTreeMap<String, EvidenceValue> }
  pub(crate) fn rule(code: ConditionCode, e: &Evidence, t: &Thresholds) -> Outcome;
  pub(crate) enum AckTimeoutCause { DeliveryStallingIngest, StorageSlowOrFailing, FlusherNotCompleting }
  ```

- [ ] **Step 1: Write the failing tests.** `crates/weir-status/src/testing.rs` (the healthy fixture the tables mutate):
  ```rust
  //! Fixtures shared by this crate's tests, weir-server's tests and weir-ctl's
  //! tests. `#[doc(hidden)]`: not part of the supported API.
  use crate::*;
  use std::collections::BTreeMap;

  /// A healthy two-shard daemon, ten minutes up, serving; every sub-record
  /// present, so no condition is active and none is unevaluated.
  pub fn serving_evidence(now: u64) -> Evidence {
      let mut view = BTreeMap::new();
      for (k, v) in [("shard_count", 2u64), ("worker_count", 2), ("wab_max_bytes", 1_000_000_000),
                     ("dead_letter_max_bytes", 100_000_000), ("wab_segment_max_bytes", 268_435_456),
                     ("wab_segment_max_age_secs", 0), ("wab_segment_max_lifetime_secs", 0),
                     ("sink_max_retries", 3), ("sink_timeout_secs", 30),
                     ("health_poll_interval_secs", 30), ("shutdown_timeout_secs", 30),
                     ("drain_shutdown_budget_secs", 10), ("max_connections", 1024)] {
          view.insert(k.to_string(), ConfigValue::U64(v));
      }
      view.insert("peer_uid_check".to_string(), ConfigValue::Bool(true));
      view.insert("tls_enabled".to_string(), ConfigValue::Bool(false));
      view.insert("sink_type".to_string(), ConfigValue::Str("http".into()));
      Evidence {
          now_unix_ms: now,
          source: Source::Daemon,
          identity: Some(IdentityEvidence {
              version: "4.1.0".into(), wire_version: 1, pid: 4242, started_at_unix_ms: now - 600_000,
              wab_dir: "/var/lib/weir/wab".into(), socket_path: "/run/weir/weir.sock".into(),
              wab_dir_dev: 66_306, wab_dir_ino: 1_048_577,
          }),
          started_at_unix_ms: Some(now - 600_000),
          phase: Some(Phase::Serving),
          phase_since_unix_ms: Some(now - 590_000),
          ever_served: Some(true),
          shutting_down: Some(false),
          shutting_down_since_unix_ms: None,
          forced: Some(false),
          drain_deadline_unix_ms: None,
          drain_budget_secs: Some(10),
          listeners: Some(vec![ListenerEvidence { kind: ListenerKind::Unix, state: ListenerState::Bound }]),
          shards: Some((0..2).map(|id| ShardEvidence {
              id, state: ShardState::Up, reason: None, since_unix_ms: now - 590_000, respawns: 0,
              worker_running: true, handoff_blocked_since_unix_ms: None, active_segment_age_secs: Some(12.0),
          }).collect()),
          workers: Some((0..2).map(|id| WorkerEvidence { id, running: true, exited_at_unix_ms: None }).collect()),
          queue_depth: Some(0),
          wab_cap: Some(CapEvidence {
              max_bytes: 1_000_000_000, low_water_bytes: 900_000_000, bytes_now: 10_000, rejecting: false,
              first_scan_ok_unix_ms: Some(now - 595_000), last_scan_ok_unix_ms: Some(now - 2_000),
              growing_unbounded: Some(false),
          }),
          channel: Some(ChannelEvidence { depth: 0, capacity: 256, high_since_unix_ms: None }),
          drain: Some(DrainEvidence {
              state: DrainStateCode::Draining, state_since_unix_ms: now - 590_000, pause_requested: false,
              paused: false, held: false, paused_since_unix_ms: None, paused_by_uid: None, generation: 1,
              observed_requeue: 0, in_flight: None, commit_in_flight_since_unix_ms: None, busy: None,
              last_progress_unix_ms: Some(now - 1_000), heartbeat_age_secs: Some(1.0), pending_segments: 0,
              backlog_segments: 0, replay_total: 0, replay_remaining: 0, blocked_since_unix_ms: None,
              last_requeue: None, exit: DrainExitCode::Running, last_panic: None,
          }),
          sink: Some(SinkEvidence {
              health: Some(SinkHealthCode::Healthy), health_since_unix_ms: Some(now - 590_000),
              last_success_unix_ms: Some(now - 1_000), last_failure_unix_ms: None, last_failure_kind: None,
              consecutive_failures: 0, last_error: None, probe_reason: None,
          }),
          backlog: Some(BacklogEvidence {
              undelivered_segments: 0, undelivered_records: 0, oldest_undelivered_age_secs: None,
              dead_letter_bytes: 0, dead_letter_max_bytes: Some(100_000_000), quarantine_bytes: 0,
              quarantine_files: 0, scanned_at_ms: now - 2_000,
          }),
          recent: Some(RecentCounters {
              ack_timeouts_5m: Some(0), fsync_failures_15m: Some(0), flusher_panics_15m: Some(0),
              stranded_since_last_success: Some(0), drain_panics_total: Some(0),
              recovery_segments_failed: Some(0), recovery_quarantine_copy_failed: Some(0),
              coalesce_ewma_us: Some(200),
          }),
          reload: Some(ReloadEvidence {
              trigger: ReloadTriggerCode::Sighup, at_unix_ms: now - 300_000, outcome: ReloadOutcomeCode::Noop,
              tls_last_failed: false, items: vec![],
              not_reloaded: vec![NotReloadedEvidence { item: ReloadItem::ConfigFile,
                                                       reason_code: NotReloadedReason::RestartRequired }],
          }),
          admin: Some(AdminEvidence { enabled: true, state: AdminState::Bound, reason: None,
                                      path: Some("/var/lib/weir/wab.admin.sock".into()) }),
          config: Some(ConfigEvidence {
              sink_type: "http".into(), commit_timeout_secs: Some(60),
              public: PublicConfig::from_view(&view), view,
          }),
      }
  }
  ```
  `crates/weir-status/src/rules.rs`. Write only the test module now:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::testing::serving_evidence;

      pub(crate) const NOW: u64 = 1_800_000_000_000;
      use ConditionCode as C;

      pub(crate) fn outcome(code: ConditionCode, e: &Evidence) -> &'static str {
          match rule(code, e, &Thresholds::DEFAULT) {
              Outcome::Unevaluated => "unevaluated",
              Outcome::Inactive => "inactive",
              Outcome::Active(_) => "active",
          }
      }
      pub(crate) fn with(f: impl FnOnce(&mut Evidence)) -> Evidence {
          let mut e = serving_evidence(NOW);
          f(&mut e);
          e
      }
      pub(crate) fn fired(code: ConditionCode, e: &Evidence) -> Fired {
          match rule(code, e, &Thresholds::DEFAULT) {
              Outcome::Active(f) => f,
              _ => panic!("{code:?} did not fire"),
          }
      }

      const INGEST: [ConditionCode; 11] = [
          C::Starting, C::ShuttingDown, C::ListenerDown, C::NoLiveShard, C::ShardOffline,
          C::WorkerExited, C::WabCapRejecting, C::DrainBackpressure, C::AckTimeoutsRecent,
          C::WabScanStale, C::WabGrowingUnbounded,
      ];

      #[test]
      fn a_healthy_serving_daemon_raises_no_ingest_condition() {
          let e = serving_evidence(NOW);
          for c in INGEST {
              assert_eq!(outcome(c, &e), "inactive", "{c:?}");
          }
      }

      #[test]
      fn missing_evidence_makes_every_ingest_rule_unevaluated() {
          let e = Evidence::empty(NOW, Source::Daemon);
          for c in INGEST {
              assert_eq!(outcome(c, &e), "unevaluated", "{c:?}");
          }
      }

      #[test]
      fn starting_fires_in_every_phase_before_serving_and_names_it() {
          for p in [Phase::Initializing, Phase::Recovering, Phase::Handoff, Phase::Binding] {
              let e = with(|e| {
                  e.phase = Some(p);
                  e.phase_since_unix_ms = Some(NOW - 7_000);
              });
              let f = fired(C::Starting, &e);
              assert_eq!(f.evidence["phase"], EvidenceValue::Code(p.as_str().into()));
              assert!(f.summary.contains(&format!("startup phase {} for 7s", p.as_str())), "{}", f.summary);
          }
          assert_eq!(outcome(C::Starting, &with(|e| e.phase = Some(Phase::ShuttingDown))), "inactive");
      }

      #[test]
      fn shutting_down_fires_and_names_the_budget_only_when_known() {
          let e = with(|e| e.shutting_down = Some(true));
          assert_eq!(fired(C::ShuttingDown, &e).summary, "graceful shutdown; new connections refused");
          let e = with(|e| {
              e.shutting_down = Some(true);
              e.drain_deadline_unix_ms = Some(NOW + 4_000);
              e.drain_budget_secs = Some(10);
          });
          assert!(fired(C::ShuttingDown, &e).summary.ends_with("drain has 4s of its 10s budget"));
      }

      #[test]
      fn listener_down_needs_a_failure_or_a_close_outside_shutdown() {
          let set = |state, sd| with(|e| {
              e.listeners = Some(vec![ListenerEvidence { kind: ListenerKind::Tcp, state }]);
              e.shutting_down = sd;
          });
          assert_eq!(outcome(C::ListenerDown, &set(ListenerState::Failed, Some(true))), "active");
          assert_eq!(outcome(C::ListenerDown, &set(ListenerState::Closed, Some(false))), "active");
          assert_eq!(outcome(C::ListenerDown, &set(ListenerState::Closed, Some(true))), "inactive");
          assert_eq!(outcome(C::ListenerDown, &set(ListenerState::Closed, None)), "unevaluated");
          assert_eq!(outcome(C::ListenerDown, &set(ListenerState::Pending, Some(false))), "inactive");
          assert!(fired(C::ListenerDown, &set(ListenerState::Failed, Some(false))).summary.starts_with("tcp listener"));
      }

      #[test]
      fn no_live_shard_counts_ingest_capable_shards_only_while_serving() {
          let down = |phase| with(|e| {
              e.phase = Some(phase);
              for s in e.shards.as_mut().unwrap() {
                  s.state = ShardState::Offline;
              }
          });
          assert_eq!(outcome(C::NoLiveShard, &down(Phase::Serving)), "active");
          assert_eq!(outcome(C::NoLiveShard, &down(Phase::Recovering)), "inactive");
          let worker_gone = with(|e| for s in e.shards.as_mut().unwrap() { s.worker_running = false });
          assert_eq!(outcome(C::NoLiveShard, &worker_gone), "active");
          let respawning = with(|e| e.shards.as_mut().unwrap()[0].state = ShardState::Respawning);
          assert_eq!(outcome(C::NoLiveShard, &respawning), "inactive");
      }

      #[test]
      fn shard_offline_is_a_partial_outage_outside_shutdown() {
          let one = with(|e| {
              let s = &mut e.shards.as_mut().unwrap()[1];
              s.state = ShardState::Offline;
              s.reason = Some(OfflineReason::CounterScanFailed);
          });
          let f = fired(C::ShardOffline, &one);
          assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![1]));
          assert!(f.summary.starts_with("shard(s) 1 offline (counter_scan_failed)"), "{}", f.summary);
          let all = with(|e| for s in e.shards.as_mut().unwrap() { s.state = ShardState::Offline });
          assert_eq!(outcome(C::ShardOffline, &all), "inactive", "all offline is no_live_shard's job");
          let mut stopping = one.clone();
          stopping.shutting_down = Some(true);
          assert_eq!(outcome(C::ShardOffline, &stopping), "inactive");
      }

      #[test]
      fn worker_exited_names_the_shards_it_owned_and_ignores_unstarted_workers() {
          let e = with(|e| {
              let w = &mut e.workers.as_mut().unwrap()[1];
              w.running = false;
              w.exited_at_unix_ms = Some(NOW - 1);
          });
          let f = fired(C::WorkerExited, &e);
          assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![1]));
          assert_eq!(f.summary, "worker 1 exited; shards 1 cannot accept records");
          let unstarted = with(|e| e.workers.as_mut().unwrap()[1].running = false);
          assert_eq!(outcome(C::WorkerExited, &unstarted), "inactive");
      }

      #[test]
      fn cap_rejecting_carries_bytes_cap_and_low_water() {
          let e = with(|e| {
              let c = e.wab_cap.as_mut().unwrap();
              c.rejecting = true;
              c.bytes_now = 950_000_000;
          });
          let f = fired(C::WabCapRejecting, &e);
          assert_eq!(f.evidence["low_water"], EvidenceValue::U64(900_000_000));
          assert!(f.summary.starts_with("WAB holds 950000000 of the 1000000000 cap"));
      }

      #[test]
      fn backpressure_is_three_valued() {
          let blocked = with(|e| e.shards.as_mut().unwrap()[0].handoff_blocked_since_unix_ms = Some(NOW - 2_500));
          let f = fired(C::DrainBackpressure, &blocked);
          assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![0]));
          assert_eq!(f.evidence["blocked_secs"], EvidenceValue::U64(2));
          let brief = with(|e| e.shards.as_mut().unwrap()[0].handoff_blocked_since_unix_ms = Some(NOW - 500));
          assert_eq!(outcome(C::DrainBackpressure, &brief), "inactive");
          let high = with(|e| e.channel = Some(ChannelEvidence { depth: 240, capacity: 256,
                                                                   high_since_unix_ms: Some(NOW - 11_000) }));
          assert_eq!(outcome(C::DrainBackpressure, &high), "active");
          // PR 1: no channel depth yet and no shard blocked -> cannot say "no backpressure".
          let unknown_channel = with(|e| e.channel = None);
          assert_eq!(outcome(C::DrainBackpressure, &unknown_channel), "unevaluated");
          let mut blocked_unknown_channel = blocked.clone();
          blocked_unknown_channel.channel = None;
          assert_eq!(outcome(C::DrainBackpressure, &blocked_unknown_channel), "active");
      }

      #[test]
      fn ack_timeouts_infer_their_cause_in_spec_order() {
          let timeouts = |f: &dyn Fn(&mut Evidence)| with(|e| {
              e.recent.as_mut().unwrap().ack_timeouts_5m = Some(3);
              f(e);
          });
          let cause = |e: &Evidence| fired(C::AckTimeoutsRecent, e).evidence["cause"].clone();
          let code = |s: &str| EvidenceValue::Code(s.into());
          assert_eq!(cause(&timeouts(&|e| e.shards.as_mut().unwrap()[0].handoff_blocked_since_unix_ms = Some(NOW - 5_000))),
                     code("delivery_stalling_ingest"));
          assert_eq!(cause(&timeouts(&|e| e.recent.as_mut().unwrap().fsync_failures_15m = Some(1))),
                     code("storage_slow_or_failing"));
          assert_eq!(cause(&timeouts(&|e| e.recent.as_mut().unwrap().coalesce_ewma_us = Some(2_000_000))),
                     code("storage_slow_or_failing"));
          assert_eq!(cause(&timeouts(&|_| {})), code("flusher_not_completing"));
      }

      #[test]
      fn scan_staleness_and_unbounded_growth() {
          let never = with(|e| e.wab_cap.as_mut().unwrap().last_scan_ok_unix_ms = None);
          assert!(fired(C::WabScanStale, &never).summary.starts_with("no successful WAB scan yet"));
          let old = with(|e| e.wab_cap.as_mut().unwrap().last_scan_ok_unix_ms = Some(NOW - 31_000));
          assert_eq!(fired(C::WabScanStale, &old).evidence["scan_age_secs"], EvidenceValue::U64(31));
          let offline = with(|e| e.source = Source::Metrics);
          assert_eq!(outcome(C::WabScanStale, &offline), "unevaluated", "only the daemon knows scan times");
          assert_eq!(outcome(C::WabGrowingUnbounded, &with(|e| e.wab_cap.as_mut().unwrap().growing_unbounded = Some(true))), "active");
          assert_eq!(outcome(C::WabGrowingUnbounded, &with(|e| e.wab_cap.as_mut().unwrap().growing_unbounded = None)), "unevaluated");
      }

      #[test]
      fn evaluate_attaches_catalogue_metadata_and_latch_times() {
          struct Fixed;
          impl SinceLookup for Fixed {
              fn since(&self, _: ConditionCode) -> Option<u64> { Some(77) }
          }
          let e = with(|e| e.wab_cap.as_mut().unwrap().rejecting = true);
          let ev = evaluate(&e, &Thresholds::DEFAULT, &Fixed);
          let c = ev.get(C::WabCapRejecting).expect("active");
          assert_eq!(c.since_unix_ms, Some(77));
          assert_eq!(c.severity, Severity::Critical);
          assert_eq!(c.runbook, "docs/monitoring.md#cond-wab_cap_rejecting");
          assert_eq!(c.gates, vec![Gate::Readiness]);
          assert!(!ev.unevaluated.contains(&C::WabCapRejecting));
      }
  }
  ```
  lib.rs: add `mod rules; #[doc(hidden)] pub mod testing; pub use rules::evaluate;`.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status rules::`. Expected: compile errors `cannot find function `rule`` / `evaluate`, `cannot find type `Outcome``.
- [ ] **Step 3: Implement.** `rules.rs`, above the tests:
  ```rust
  //! One rule per catalogue code (spec §9.3), plus `evaluate`. A rule returns
  //! `Unevaluated` when the evidence it needs is missing, never `Inactive`.
  use crate::codes::{CATALOGUE, ConditionCode};
  use crate::condition::{Condition, DETAIL_PANIC_MAX_BYTES, Evaluation, EvidenceValue, SinceLookup, Thresholds};
  use crate::evidence::*;
  use crate::summary;
  use std::borrow::Cow;
  use std::collections::BTreeMap;

  /// Evaluates every catalogue code against `e` (spec §5.1). Active
  /// conditions come back in catalogue order with their latch time.
  pub fn evaluate(e: &Evidence, t: &Thresholds, latches: &dyn SinceLookup) -> Evaluation {
      let mut out = Evaluation::default();
      for entry in CATALOGUE {
          match rule(entry.code, e, t) {
              Outcome::Unevaluated => out.unevaluated.push(entry.code),
              Outcome::Inactive => {}
              Outcome::Active(f) => out.conditions.push(Condition {
                  code: entry.code,
                  severity: entry.severity,
                  affects: entry.affects,
                  since_unix_ms: latches.since(entry.code),
                  summary: f.summary,
                  detail: f.detail,
                  evidence: f.evidence,
                  runbook: Cow::Borrowed(entry.runbook),
                  gates: entry.gates.to_vec(),
              }),
          }
      }
      out
  }

  pub(crate) struct Fired {
      pub(crate) summary: String,
      pub(crate) detail: Option<String>,
      pub(crate) evidence: BTreeMap<String, EvidenceValue>,
  }

  impl Fired {
      fn new(summary: String) -> Self {
          Fired { summary, detail: None, evidence: BTreeMap::new() }
      }
      fn with(mut self, key: &'static str, v: EvidenceValue) -> Self {
          self.evidence.insert(key.to_string(), v);
          self
      }
      #[allow(dead_code)] // first used by Task 1.5's drain_stopped; Task 1.5 drops this allow
      fn detail(mut self, d: Option<String>) -> Self {
          self.detail = d;
          self
      }
  }

  pub(crate) enum Outcome {
      Unevaluated,
      Inactive,
      Active(Fired),
  }

  impl From<Fired> for Outcome {
      fn from(f: Fired) -> Self {
          Outcome::Active(f)
      }
  }

  /// Why producers saw ACK_TIMEOUT nacks (spec §9.3 code 9), as a code.
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum AckTimeoutCause {
      DeliveryStallingIngest,
      StorageSlowOrFailing,
      FlusherNotCompleting,
  }

  impl AckTimeoutCause {
      pub(crate) fn as_str(self) -> &'static str {
          match self {
              Self::DeliveryStallingIngest => "delivery_stalling_ingest",
              Self::StorageSlowOrFailing => "storage_slow_or_failing",
              Self::FlusherNotCompleting => "flusher_not_completing",
          }
      }
  }

  fn u(v: u64) -> EvidenceValue {
      EvidenceValue::U64(v)
  }
  fn code(s: &'static str) -> EvidenceValue {
      EvidenceValue::Code(s.to_string())
  }
  fn age_ms(now: u64, then: u64) -> u64 {
      now.saturating_sub(then)
  }
  fn tri_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
      match (a, b) {
          (Some(true), _) | (_, Some(true)) => Some(true),
          (Some(false), Some(false)) => Some(false),
          _ => None,
      }
  }

  pub(crate) fn rule(c: ConditionCode, e: &Evidence, t: &Thresholds) -> Outcome {
      use ConditionCode as C;
      match c {
          C::Starting => starting(e),
          C::ShuttingDown => shutting_down(e),
          C::ListenerDown => listener_down(e),
          C::NoLiveShard => no_live_shard(e),
          C::ShardOffline => shard_offline(e),
          C::WorkerExited => worker_exited(e),
          C::WabCapRejecting => wab_cap_rejecting(e),
          C::DrainBackpressure => drain_backpressure(e, t),
          C::AckTimeoutsRecent => ack_timeouts_recent(e, t),
          C::WabScanStale => wab_scan_stale(e, t),
          C::WabGrowingUnbounded => wab_growing_unbounded(e),
          // Tasks 1.5 and 1.6 replace this arm with the remaining rules.
          _ => Outcome::Unevaluated,
      }
  }

  fn starting(e: &Evidence) -> Outcome {
      let Some(phase) = e.phase else { return Outcome::Unevaluated };
      if !phase.before_serving() {
          return Outcome::Inactive;
      }
      let secs = e.phase_since_unix_ms.map_or(0, |s| age_ms(e.now_unix_ms, s) / 1000);
      Fired::new(summary::starting(phase, secs))
          .with("phase", code(phase.as_str()))
          .with("phase_secs", u(secs))
          .into()
  }

  fn shutting_down(e: &Evidence) -> Outcome {
      match e.shutting_down {
          None => Outcome::Unevaluated,
          Some(false) => Outcome::Inactive,
          Some(true) => {
              let left = e.drain_deadline_unix_ms.map(|d| d.saturating_sub(e.now_unix_ms) / 1000);
              let mut f = Fired::new(summary::shutting_down(left, e.drain_budget_secs))
                  .with("forced", EvidenceValue::Bool(e.forced.unwrap_or(false)));
              if let Some(l) = left {
                  f = f.with("budget_left_secs", u(l));
              }
              if let Some(b) = e.drain_budget_secs {
                  f = f.with("budget_secs", u(b));
              }
              f.into()
          }
      }
  }

  fn listener_down(e: &Evidence) -> Outcome {
      let Some(listeners) = &e.listeners else { return Outcome::Unevaluated };
      let mut unknown = false;
      for l in listeners {
          let down = match l.state {
              ListenerState::Failed => Some(true),
              ListenerState::Closed => e.shutting_down.map(|sd| !sd),
              ListenerState::NotConfigured | ListenerState::Pending | ListenerState::Bound => Some(false),
          };
          match down {
              Some(true) => {
                  return Fired::new(summary::listener_down(l.kind))
                      .with("listener", code(l.kind.as_str()))
                      .with("state", code(l.state.as_str()))
                      .into();
              }
              None => unknown = true,
              Some(false) => {}
          }
      }
      if unknown { Outcome::Unevaluated } else { Outcome::Inactive }
  }

  fn no_live_shard(e: &Evidence) -> Outcome {
      let (Some(phase), Some(shards)) = (e.phase, &e.shards) else { return Outcome::Unevaluated };
      if phase != Phase::Serving || shards.iter().any(ShardEvidence::ingest_capable) {
          return Outcome::Inactive;
      }
      Fired::new(summary::no_live_shard()).with("shard_count", u(shards.len() as u64)).into()
  }

  fn shard_offline(e: &Evidence) -> Outcome {
      let (Some(shards), Some(sd)) = (&e.shards, e.shutting_down) else { return Outcome::Unevaluated };
      if sd {
          return Outcome::Inactive;
      }
      let offline: Vec<&ShardEvidence> = shards.iter().filter(|s| s.state == ShardState::Offline).collect();
      if offline.is_empty() || offline.len() == shards.len() {
          return Outcome::Inactive;
      }
      let ids: Vec<u16> = offline.iter().map(|s| s.id).collect();
      let reason = offline[0].reason.unwrap_or(OfflineReason::FlusherExited);
      Fired::new(summary::shard_offline(&ids, reason))
          .with("reason", code(reason.as_str()))
          .with("shards", EvidenceValue::Shards(ids))
          .into()
  }

  fn worker_exited(e: &Evidence) -> Outcome {
      let (Some(workers), Some(sd)) = (&e.workers, e.shutting_down) else { return Outcome::Unevaluated };
      if sd {
          return Outcome::Inactive;
      }
      // `exited_at` distinguishes a worker that ran and exited from one not
      // started yet (startup): only the former is a condition.
      let Some(w) = workers.iter().find(|w| !w.running && w.exited_at_unix_ms.is_some()) else {
          return Outcome::Inactive;
      };
      let n = workers.len() as u16;
      let shards: Vec<u16> =
          e.shards.iter().flatten().map(|s| s.id).filter(|id| n > 0 && id % n == w.id).collect();
      Fired::new(summary::worker_exited(w.id, &shards))
          .with("worker", u(u64::from(w.id)))
          .with("shards", EvidenceValue::Shards(shards))
          .into()
  }

  fn wab_cap_rejecting(e: &Evidence) -> Outcome {
      let Some(c) = &e.wab_cap else { return Outcome::Unevaluated };
      if !c.rejecting {
          return Outcome::Inactive;
      }
      Fired::new(summary::wab_cap_rejecting(c.bytes_now, c.max_bytes, c.low_water_bytes))
          .with("bytes", u(c.bytes_now))
          .with("cap", u(c.max_bytes))
          .with("low_water", u(c.low_water_bytes))
          .into()
  }

  fn drain_backpressure(e: &Evidence, t: &Thresholds) -> Outcome {
      let now = e.now_unix_ms;
      let blocked = e.shards.as_ref().map(|shards| {
          let ids: Vec<u16> = shards
              .iter()
              .filter(|s| s.handoff_blocked_since_unix_ms.is_some_and(|b| age_ms(now, b) >= t.handoff_blocked_ms))
              .map(|s| s.id)
              .collect();
          let longest_ms =
              shards.iter().filter_map(|s| s.handoff_blocked_since_unix_ms).map(|b| age_ms(now, b)).max().unwrap_or(0);
          (ids, longest_ms / 1000)
      });
      let channel_high =
          e.channel.map(|c| c.high_since_unix_ms.is_some_and(|h| age_ms(now, h) >= t.channel_high_ms));
      match tri_or(blocked.as_ref().map(|(ids, _)| !ids.is_empty()), channel_high) {
          None => Outcome::Unevaluated,
          Some(false) => Outcome::Inactive,
          Some(true) => {
              let (ids, secs) = blocked.unwrap_or_default();
              let depth = e.channel.map(|c| c.depth);
              let mut f = Fired::new(summary::drain_backpressure(depth, &ids, secs))
                  .with("blocked_secs", u(secs))
                  .with("shards", EvidenceValue::Shards(ids));
              if let Some(d) = depth {
                  f = f.with("channel_depth", u(d));
              }
              f.into()
          }
      }
  }

  fn ack_timeouts_recent(e: &Evidence, t: &Thresholds) -> Outcome {
      let Some(n) = e.recent.as_ref().and_then(|r| r.ack_timeouts_5m) else { return Outcome::Unevaluated };
      if n == 0 {
          return Outcome::Inactive;
      }
      let storage = e.recent.as_ref().is_some_and(|r| {
          r.fsync_failures_15m.unwrap_or(0) > 0 || r.coalesce_ewma_us.unwrap_or(0) > t.coalesce_slow_us
      });
      let cause = if matches!(drain_backpressure(e, t), Outcome::Active(_)) {
          AckTimeoutCause::DeliveryStallingIngest
      } else if storage {
          AckTimeoutCause::StorageSlowOrFailing
      } else {
          AckTimeoutCause::FlusherNotCompleting
      };
      Fired::new(summary::ack_timeouts_recent(n, cause))
          .with("ack_timeouts", u(n))
          .with("cause", code(cause.as_str()))
          .into()
  }

  fn wab_scan_stale(e: &Evidence, t: &Thresholds) -> Outcome {
      // Scan times exist only inside the daemon; a scrape cannot say.
      if e.source != Source::Daemon {
          return Outcome::Unevaluated;
      }
      let Some(c) = &e.wab_cap else { return Outcome::Unevaluated };
      match c.last_scan_ok_unix_ms {
          None => Fired::new(summary::wab_scan_stale(true, 0)).with("never", EvidenceValue::Bool(true)).into(),
          Some(at) if age_ms(e.now_unix_ms, at) > t.wab_scan_stale_ms => {
              let secs = age_ms(e.now_unix_ms, at) / 1000;
              Fired::new(summary::wab_scan_stale(false, secs)).with("scan_age_secs", u(secs)).into()
          }
          Some(_) => Outcome::Inactive,
      }
  }

  fn wab_growing_unbounded(e: &Evidence) -> Outcome {
      match e.wab_cap.as_ref().and_then(|c| c.growing_unbounded) {
          None => Outcome::Unevaluated,
          Some(false) => Outcome::Inactive,
          Some(true) => Fired::new(summary::wab_growing_unbounded()).into(),
      }
  }
  ```
  (`DETAIL_PANIC_MAX_BYTES` is used from Task 1.5; until then add `#[allow(unused_imports)]` on that `use` line and remove it in Task 1.5.)

  `summary.rs`, above `#[cfg(test)]`:
  ```rust
  use crate::evidence::{ListenerKind, OfflineReason, Phase};
  use crate::rules::AckTimeoutCause;

  fn join_ids(ids: &[u16]) -> String {
      ids.iter().map(u16::to_string).collect::<Vec<_>>().join(",")
  }

  pub(crate) fn starting(phase: Phase, secs: u64) -> String {
      format!("startup phase {} for {secs}s; the producer socket binds after recovery seals open segments", phase.as_str())
  }

  pub(crate) fn shutting_down(left_secs: Option<u64>, budget_secs: Option<u64>) -> String {
      match (left_secs, budget_secs) {
          (Some(left), Some(budget)) => {
              format!("graceful shutdown; new connections refused; drain has {left}s of its {budget}s budget")
          }
          _ => "graceful shutdown; new connections refused".to_string(),
      }
  }

  pub(crate) fn listener_down(kind: ListenerKind) -> String {
      format!("{} listener is not accepting; restart re-binds it", kind.as_str())
  }

  pub(crate) fn no_live_shard() -> String {
      "every shard's flusher or owning worker is offline; new connections are refused; restart re-runs recovery"
          .to_string()
  }

  pub(crate) fn shard_offline(ids: &[u16], reason: OfflineReason) -> String {
      format!(
          "shard(s) {} offline ({}); new connections avoid them; connections already pinned to them nack until they reconnect; restart to recover",
          join_ids(ids),
          reason.as_str()
      )
  }

  pub(crate) fn worker_exited(worker: u16, shards: &[u16]) -> String {
      format!("worker {worker} exited; shards {} cannot accept records", join_ids(shards))
  }

  pub(crate) fn wab_cap_rejecting(bytes: u64, cap: u64, low_water: u64) -> String {
      format!("WAB holds {bytes} of the {cap} cap; pushes nack until below {low_water}; the root cause is delivery (see drain/sink conditions)")
  }

  pub(crate) fn drain_backpressure(depth: Option<u64>, ids: &[u16], blocked_secs: u64) -> String {
      let depth = depth.map_or_else(|| "unknown".to_string(), |d| d.to_string());
      format!(
          "drain channel {depth}/256; shard(s) {} blocked handing off for {blocked_secs}s; acks wait behind the hand-off, so producers see ACK_TIMEOUT nacks of durable records (delivery, not storage)",
          join_ids(ids)
      )
  }

  pub(crate) fn ack_timeouts_recent(count: u64, cause: AckTimeoutCause) -> String {
      let why = match cause {
          AckTimeoutCause::DeliveryStallingIngest => "delivery is stalling ingest",
          AckTimeoutCause::StorageSlowOrFailing => "storage is slow or failing",
          AckTimeoutCause::FlusherNotCompleting => "a flusher is not completing groups; check shards",
      };
      format!("{count} ack timeout(s) in the last 5 min; {why}")
  }

  pub(crate) fn wab_scan_stale(never: bool, age_secs: u64) -> String {
      let tail = "wab_max_bytes is enforced against a stale size (fail-closed); lag figures are stale";
      if never { format!("no successful WAB scan yet; {tail}") } else { format!("no successful WAB scan for {age_secs}s; {tail}") }
  }

  pub(crate) fn wab_growing_unbounded() -> String {
      "the WAB is growing while delivery is unhealthy and no wab_max_bytes is set; producers are acked into an unbounded buffer".to_string()
  }
  ```
  Keep every `pub(crate) fn` signature on one line. `rustfmt` keeps a signature on one line while it fits in 100 columns, and all of these do.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all tests pass, including the 13 new `rules::tests`.
- [ ] **Step 5: Commit.** `git add crates/weir-status/src && git commit -m "feat(status): the evaluator and the ingest and lifecycle conditions"`

---

### Task 1.5: Delivery rules (§9.3 codes 12-24)

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-status/src/rules.rs` (`rule` arms and 13 rule fns; drop the `allow(unused_imports)` and the `allow(dead_code)` on `Fired::detail`), `crates/weir-status/src/summary.rs` (13 templates)
- Test: `crates/weir-status/src/rules.rs` (`mod tests`, new tests appended)

**Interfaces:**
- Consumes: Task 1.4 (`rule`, `Fired`, `Outcome`, the test helpers `outcome`, `with`, `fired`, `NOW`).
- Produces: no new public items; `evaluate` now decides codes 12-24.

- [ ] **Step 1: Write the failing tests** (append inside `rules::tests`). The `use` line is needed: rules.rs's `use crate::evidence::*` does not re-export evidence.rs's private `use crate::segment::SegmentId;`, and it goes here rather than in Task 1.4's header so the Task 1.4 build has no unused import.
  ```rust
  use crate::segment::SegmentId;

  const DELIVERY: [ConditionCode; 13] = [
      C::DrainStopped, C::DrainWedged, C::DrainPaused, C::DrainBlockedDeadLetterFull,
      C::DeliveryStalled, C::DeliveryLagging, C::SegmentsStranded, C::SinkDown, C::SinkDegraded,
      C::SinkFailing, C::DeadLetterNearCap, C::NoopSink, C::ActiveSegmentAging,
  ];

  #[test]
  fn a_healthy_serving_daemon_raises_no_delivery_condition() {
      let e = serving_evidence(NOW);
      for c in DELIVERY {
          assert_eq!(outcome(c, &e), "inactive", "{c:?}");
      }
  }

  #[test]
  fn missing_evidence_makes_every_delivery_rule_unevaluated() {
      let e = Evidence::empty(NOW, Source::Daemon);
      for c in DELIVERY {
          assert_eq!(outcome(c, &e), "unevaluated", "{c:?}");
      }
  }

  #[test]
  fn drain_stopped_is_gave_up_outside_shutdown_with_a_capped_panic_detail() {
      let gave_up = with(|e| {
          let d = e.drain.as_mut().unwrap();
          d.exit = DrainExitCode::GaveUp;
          d.last_panic = Some("x".repeat(1_000));
          e.recent.as_mut().unwrap().drain_panics_total = Some(10);
      });
      let f = fired(C::DrainStopped, &gave_up);
      assert!(f.summary.starts_with("drain supervisor gave up after 10 panics"));
      assert_eq!(f.detail.as_deref().map(str::len), Some(DETAIL_PANIC_MAX_BYTES));
      let mut stopping = gave_up.clone();
      stopping.shutting_down = Some(true);
      assert_eq!(outcome(C::DrainStopped, &stopping), "inactive");
      assert_eq!(outcome(C::DrainStopped, &with(|e| e.drain.as_mut().unwrap().exit = DrainExitCode::Stopped)), "inactive");
  }

  #[test]
  fn drain_wedged_uses_the_commit_timer_or_the_progress_timer() {
      // commit_timeout 60 s + 15 s grace
      let commit = |age| with(|e| e.drain.as_mut().unwrap().commit_in_flight_since_unix_ms = Some(NOW - age));
      assert_eq!(outcome(C::DrainWedged, &commit(75_000)), "inactive");
      assert_eq!(outcome(C::DrainWedged, &commit(75_001)), "active");
      // health_poll_interval 30 s + 15 s grace
      let progress = |age| with(|e| e.drain.as_mut().unwrap().last_progress_unix_ms = Some(NOW - age));
      assert_eq!(outcome(C::DrainWedged, &progress(44_999)), "inactive");
      assert_eq!(outcome(C::DrainWedged, &progress(45_000)), "active");
      let none = with(|e| e.drain.as_mut().unwrap().last_progress_unix_ms = None);
      assert_eq!(outcome(C::DrainWedged, &none), "unevaluated");
      let gone = with(|e| {
          let d = e.drain.as_mut().unwrap();
          d.exit = DrainExitCode::GaveUp;
          d.last_progress_unix_ms = Some(NOW - 600_000);
      });
      assert_eq!(outcome(C::DrainWedged, &gone), "inactive", "a stopped drain is drain_stopped, not wedged");
      // The safety floor's busy signal (main.rs `classify_drain_stall`): local
      // I/O is a slow disk, never a wedge, however long; a sink call is a wedge
      // only past commit_timeout + 15 s.
      let busy = |sink_call, age| with(|e| {
          let d = e.drain.as_mut().unwrap();
          d.busy = Some(BusyEvidence { sink_call, since_unix_ms: NOW - age });
          d.last_progress_unix_ms = Some(NOW - age);
      });
      assert_eq!(outcome(C::DrainWedged, &busy(false, 600_000)), "inactive", "SlowLocalIo is never a wedge");
      assert_eq!(outcome(C::DrainWedged, &busy(true, 50_000)), "inactive", "SlowSinkCall inside the limit");
      assert_eq!(outcome(C::DrainWedged, &busy(true, 75_000)), "inactive");
      assert_eq!(outcome(C::DrainWedged, &busy(true, 75_001)), "active");
      assert_eq!(fired(C::DrainWedged, &busy(true, 80_000)).evidence["busy_secs"], EvidenceValue::U64(80));
  }

  #[test]
  fn drain_paused_reports_held_records_unbounded_growth_and_the_uid_as_admin_detail() {
      let e = with(|e| {
          let d = e.drain.as_mut().unwrap();
          d.pause_requested = true;
          d.paused = true;
          d.held = true;
          d.paused_since_unix_ms = Some(NOW - 30_000);
          d.paused_by_uid = Some(1000);
          d.in_flight = Some(InFlightEvidence { segment: SegmentId { shard: 0, counter: 9 }, attempt: 1,
                                                retries_left: 3, processed_records: 512, next_retry_at_unix_ms: None });
          e.config.as_mut().unwrap().public.wab_max_bytes = Some(0);
      });
      let f = fired(C::DrainPaused, &e);
      assert!(f.summary.contains("(held: mid-segment at record 512)"), "{}", f.summary);
      assert!(f.summary.contains("without bound: wab_max_bytes=0"), "{}", f.summary);
      assert!(!f.summary.contains("1000"), "the uid is admin-only: {}", f.summary);
      assert_eq!(f.detail.as_deref(), Some("paused by uid 1000"));
      assert_eq!(f.evidence["segment"], EvidenceValue::Segment(SegmentId { shard: 0, counter: 9 }));
      let requested = with(|e| e.drain.as_mut().unwrap().pause_requested = true);
      assert_eq!(outcome(C::DrainPaused, &requested), "active", "requested counts before it takes effect");
  }

  #[test]
  fn blocked_stalled_lagging_and_stranded() {
      let blocked = with(|e| e.drain.as_mut().unwrap().state = DrainStateCode::BlockedDeadLetterFull);
      assert!(fired(C::DrainBlockedDeadLetterFull, &blocked).summary.starts_with("dead_letter/ at 0/100000000"));
      let stalled = with(|e| {
          e.backlog.as_mut().unwrap().undelivered_segments = 4;
          e.sink.as_mut().unwrap().last_success_unix_ms = Some(NOW - 300_000);
      });
      assert_eq!(outcome(C::DeliveryStalled, &stalled), "active");
      let mut paused = stalled.clone();
      paused.drain.as_mut().unwrap().paused = true;
      assert_eq!(outcome(C::DeliveryStalled, &paused), "inactive", "a pause is not a stall");
      let never = with(|e| {
          e.backlog.as_mut().unwrap().undelivered_segments = 1;
          e.sink.as_mut().unwrap().last_success_unix_ms = None;
      });
      assert_eq!(outcome(C::DeliveryStalled, &never), "active", "measured from process start (10 min ago)");
      let lag = with(|e| e.backlog.as_mut().unwrap().oldest_undelivered_age_secs = Some(901.0));
      assert_eq!(fired(C::DeliveryLagging, &lag).evidence["oldest_age_secs"], EvidenceValue::F64(901.0));
      let stranded = with(|e| e.recent.as_mut().unwrap().stranded_since_last_success = Some(2));
      assert_eq!(outcome(C::SegmentsStranded, &stranded), "active");
      let unknown = with(|e| e.recent.as_mut().unwrap().stranded_since_last_success = None);
      assert_eq!(outcome(C::SegmentsStranded, &unknown), "unevaluated");
  }

  #[test]
  fn sink_health_failing_and_noop() {
      let down = with(|e| {
          let s = e.sink.as_mut().unwrap();
          s.health = Some(SinkHealthCode::Down);
          s.probe_reason = Some("connect refused".into());
      });
      let f = fired(C::SinkDown, &down);
      assert_eq!(f.detail.as_deref(), Some("connect refused"));
      assert!(!f.summary.contains("refused"), "probe reason is admin-only");
      assert_eq!(outcome(C::SinkDegraded, &with(|e| e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Degraded))), "active");
      let failing = |health| with(|e| {
          let s = e.sink.as_mut().unwrap();
          s.consecutive_failures = 3;
          s.health = Some(health);
          s.last_failure_kind = Some(SinkFailureKind::Timeout);
          s.last_error = Some("POST failed".into());
      });
      let f = fired(C::SinkFailing, &failing(SinkHealthCode::Healthy));
      assert!(f.summary.ends_with("last kind timeout"));
      assert_eq!(f.detail.as_deref(), Some("POST failed"));
      assert_eq!(outcome(C::SinkFailing, &failing(SinkHealthCode::Down)), "inactive", "sink_down covers it");
      assert_eq!(outcome(C::NoopSink, &with(|e| e.config.as_mut().unwrap().sink_type = "noop".into())), "active");
  }

  #[test]
  fn dead_letter_near_cap_and_active_segment_aging() {
      let near = |bytes| with(|e| e.backlog.as_mut().unwrap().dead_letter_bytes = bytes);
      assert_eq!(outcome(C::DeadLetterNearCap, &near(79_999_999)), "inactive");
      assert_eq!(outcome(C::DeadLetterNearCap, &near(80_000_000)), "active");
      let aging = with(|e| e.shards.as_mut().unwrap()[1].active_segment_age_secs = Some(3_601.0));
      let f = fired(C::ActiveSegmentAging, &aging);
      assert_eq!(f.evidence["shard"], EvidenceValue::U64(1));
      let mut timer_set = aging.clone();
      timer_set.config.as_mut().unwrap().public.wab_segment_max_age_secs = Some(60);
      assert_eq!(outcome(C::ActiveSegmentAging, &timer_set), "inactive");
      let mut no_lag = aging.clone();
      no_lag.backlog = None;
      assert_eq!(outcome(C::ActiveSegmentAging, &no_lag), "unevaluated", "ages come from the lag scan");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status rules::`. Expected: `a_healthy_serving_daemon_raises_no_delivery_condition` FAILS (`left: "unevaluated", right: "inactive"`; the fallback arm still answers codes 12-24), and so do the other new tests.
- [ ] **Step 3: Implement.** In `rule`, add the arms above the fallback:
  ```rust
  C::DrainStopped => drain_stopped(e),
  C::DrainWedged => drain_wedged(e, t),
  C::DrainPaused => drain_paused(e),
  C::DrainBlockedDeadLetterFull => drain_blocked_dead_letter_full(e),
  C::DeliveryStalled => delivery_stalled(e, t),
  C::DeliveryLagging => delivery_lagging(e, t),
  C::SegmentsStranded => segments_stranded(e),
  C::SinkDown => sink_down(e),
  C::SinkDegraded => sink_degraded(e),
  C::SinkFailing => sink_failing(e, t),
  C::DeadLetterNearCap => dead_letter_near_cap(e, t),
  C::NoopSink => noop_sink(e),
  C::ActiveSegmentAging => active_segment_aging(e, t),
  ```
  The rule functions:
  ```rust
  fn truncate(s: &str, max: usize) -> String {
      if s.len() <= max {
          return s.to_string();
      }
      let mut end = max;
      while !s.is_char_boundary(end) {
          end -= 1;
      }
      s[..end].to_string()
  }

  fn drain_stopped(e: &Evidence) -> Outcome {
      let (Some(d), Some(sd)) = (&e.drain, e.shutting_down) else { return Outcome::Unevaluated };
      if d.exit != DrainExitCode::GaveUp || sd {
          return Outcome::Inactive;
      }
      let panics = e.recent.as_ref().and_then(|r| r.drain_panics_total).unwrap_or(0);
      Fired::new(summary::drain_stopped(panics))
          .with("panics", u(panics))
          .detail(d.last_panic.as_deref().map(|p| truncate(p, DETAIL_PANIC_MAX_BYTES)))
          .into()
  }

  fn drain_wedged(e: &Evidence, t: &Thresholds) -> Outcome {
      let (Some(d), Some(c)) = (&e.drain, &e.config) else { return Outcome::Unevaluated };
      if d.exit != DrainExitCode::Running {
          return Outcome::Inactive;
      }
      let now = e.now_unix_ms;
      if let Some(since) = d.commit_in_flight_since_unix_ms {
          let Some(ct) = c.commit_timeout_secs else { return Outcome::Unevaluated };
          let age = age_ms(now, since);
          return if age > ct.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
              Fired::new(summary::drain_wedged(true, age / 1000)).with("commit_age_secs", u(age / 1000)).into()
          } else {
              Outcome::Inactive
          };
      }
      // The safety floor's verdict table (main.rs `classify_drain_stall`, spec
      // safety floor §4.8): busy in local I/O is `SlowLocalIo`, never a wedge;
      // busy in a sink call (a non-yielding probe) is `SlowSinkCall` until
      // commit_timeout + 15 s. One verdict for the log line and the condition (G2).
      if let Some(b) = d.busy {
          if !b.sink_call {
              return Outcome::Inactive;
          }
          let Some(ct) = c.commit_timeout_secs else { return Outcome::Unevaluated };
          let age = age_ms(now, b.since_unix_ms);
          return if age > ct.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
              Fired::new(summary::drain_wedged(true, age / 1000)).with("busy_secs", u(age / 1000)).into()
          } else {
              Outcome::Inactive
          };
      }
      let (Some(p), Some(hp)) = (d.last_progress_unix_ms, c.public.health_poll_interval_secs) else {
          return Outcome::Unevaluated;
      };
      let age = age_ms(now, p);
      if age >= hp.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
          Fired::new(summary::drain_wedged(false, age / 1000)).with("progress_age_secs", u(age / 1000)).into()
      } else {
          Outcome::Inactive
      }
  }

  fn drain_paused(e: &Evidence) -> Outcome {
      let Some(d) = &e.drain else { return Outcome::Unevaluated };
      if !(d.pause_requested || d.paused) {
          return Outcome::Inactive;
      }
      let unbounded = e.config.as_ref().is_some_and(|c| c.public.wab_max_bytes == Some(0));
      let held = d.in_flight.filter(|_| d.held);
      let since = d.paused_since_unix_ms.unwrap_or(e.now_unix_ms);
      let mut f = Fired::new(summary::drain_paused(since, held.map(|i| i.processed_records), unbounded))
          .with("held", EvidenceValue::Bool(d.held))
          .with("in_effect", EvidenceValue::Bool(d.paused))
          .detail(d.paused_by_uid.map(|uid| format!("paused by uid {uid}")));
      if let Some(i) = held {
          f = f.with("held_at_record", u(i.processed_records)).with("segment", EvidenceValue::Segment(i.segment));
      }
      f.into()
  }

  fn drain_blocked_dead_letter_full(e: &Evidence) -> Outcome {
      let Some(d) = &e.drain else { return Outcome::Unevaluated };
      if d.state != DrainStateCode::BlockedDeadLetterFull {
          return Outcome::Inactive;
      }
      let dl = e.backlog.as_ref().map(|b| b.dead_letter_bytes);
      let cap = e.config.as_ref().and_then(|c| c.public.dead_letter_max_bytes);
      let mut f = Fired::new(summary::drain_blocked(dl, cap));
      if let Some(b) = d.blocked_since_unix_ms {
          f = f.with("blocked_since_ms", u(b));
      }
      f.into()
  }

  fn delivery_stalled(e: &Evidence, t: &Thresholds) -> Outcome {
      let (Some(b), Some(s), Some(d)) = (&e.backlog, &e.sink, &e.drain) else { return Outcome::Unevaluated };
      if b.undelivered_segments == 0 || d.paused || d.pause_requested {
          return Outcome::Inactive;
      }
      let Some(reference) = s.last_success_unix_ms.or(e.started_at_unix_ms) else { return Outcome::Unevaluated };
      let idle = age_ms(e.now_unix_ms, reference);
      if idle < t.delivery_stalled_ms {
          return Outcome::Inactive;
      }
      Fired::new(summary::delivery_stalled(b.undelivered_segments, idle / 1000))
          .with("undelivered_segments", u(b.undelivered_segments))
          .with("idle_secs", u(idle / 1000))
          .into()
  }

  fn delivery_lagging(e: &Evidence, t: &Thresholds) -> Outcome {
      let Some(b) = &e.backlog else { return Outcome::Unevaluated };
      match b.oldest_undelivered_age_secs {
          Some(a) if a > t.delivery_lagging_secs => Fired::new(summary::delivery_lagging(a as u64))
              .with("oldest_age_secs", EvidenceValue::F64(a))
              .into(),
          _ => Outcome::Inactive,
      }
  }

  fn segments_stranded(e: &Evidence) -> Outcome {
      let Some(n) = e.recent.as_ref().and_then(|r| r.stranded_since_last_success) else {
          return Outcome::Unevaluated;
      };
      if n == 0 {
          return Outcome::Inactive;
      }
      Fired::new(summary::segments_stranded(n)).with("stranded", u(n)).into()
  }

  fn sink_down(e: &Evidence) -> Outcome {
      let Some(s) = &e.sink else { return Outcome::Unevaluated };
      match s.health {
          None => Outcome::Unevaluated,
          Some(SinkHealthCode::Down) => {
              Fired::new(summary::sink_down(s.health_since_unix_ms)).detail(s.probe_reason.clone()).into()
          }
          Some(_) => Outcome::Inactive,
      }
  }

  fn sink_degraded(e: &Evidence) -> Outcome {
      let Some(s) = &e.sink else { return Outcome::Unevaluated };
      match s.health {
          None => Outcome::Unevaluated,
          Some(SinkHealthCode::Degraded) => Fired::new(summary::sink_degraded()).detail(s.probe_reason.clone()).into(),
          Some(_) => Outcome::Inactive,
      }
  }

  fn sink_failing(e: &Evidence, t: &Thresholds) -> Outcome {
      let Some(s) = &e.sink else { return Outcome::Unevaluated };
      if s.consecutive_failures < t.sink_failing_consecutive || s.health == Some(SinkHealthCode::Down) {
          return Outcome::Inactive;
      }
      let mut f = Fired::new(summary::sink_failing(s.consecutive_failures, s.last_failure_kind))
          .with("consecutive_failures", u(s.consecutive_failures))
          .detail(s.last_error.clone());
      if let Some(k) = s.last_failure_kind {
          f = f.with("kind", code(k.as_str()));
      }
      f.into()
  }

  fn dead_letter_near_cap(e: &Evidence, t: &Thresholds) -> Outcome {
      let (Some(b), Some(c)) = (&e.backlog, &e.config) else { return Outcome::Unevaluated };
      let Some(cap) = c.public.dead_letter_max_bytes else { return Outcome::Unevaluated };
      if cap == 0 || b.dead_letter_bytes.saturating_mul(100) < cap.saturating_mul(t.dead_letter_near_cap_percent) {
          return Outcome::Inactive;
      }
      Fired::new(summary::dead_letter_near_cap(b.dead_letter_bytes, cap))
          .with("bytes", u(b.dead_letter_bytes))
          .with("cap", u(cap))
          .into()
  }

  fn noop_sink(e: &Evidence) -> Outcome {
      let Some(c) = &e.config else { return Outcome::Unevaluated };
      if c.sink_type == "noop" { Fired::new(summary::noop_sink()).into() } else { Outcome::Inactive }
  }

  fn active_segment_aging(e: &Evidence, t: &Thresholds) -> Outcome {
      // Ages come from the lag scan; without a backlog record it has not run.
      let (Some(shards), Some(c), Some(_)) = (&e.shards, &e.config, &e.backlog) else {
          return Outcome::Unevaluated;
      };
      let (Some(age), Some(life)) = (c.public.wab_segment_max_age_secs, c.public.wab_segment_max_lifetime_secs) else {
          return Outcome::Unevaluated;
      };
      if age != 0 || life != 0 {
          return Outcome::Inactive;
      }
      let oldest = shards
          .iter()
          .filter_map(|s| s.active_segment_age_secs.map(|a| (s.id, a)))
          .filter(|(_, a)| *a > t.active_segment_aging_secs)
          .max_by(|a, b| a.1.total_cmp(&b.1));
      let Some((shard, secs)) = oldest else { return Outcome::Inactive };
      Fired::new(summary::active_segment_aging(shard, secs as u64))
          .with("shard", u(u64::from(shard)))
          .with("age_secs", EvidenceValue::F64(secs))
          .into()
  }
  ```
  `summary.rs` additions (extend the `use` to `use crate::evidence::{ListenerKind, OfflineReason, Phase, SinkFailureKind};`):
  ```rust
  pub(crate) fn drain_stopped(panics: u64) -> String {
      format!("drain supervisor gave up after {panics} panics; ingest continues, nothing is delivered, the WAB grows; sink_health is stale; restart resumes")
  }

  pub(crate) fn drain_wedged(in_sink_call: bool, secs: u64) -> String {
      let at = if in_sink_call { "inside a sink call" } else { "with no sink call in flight" };
      format!("drain thread has made no progress for {secs}s ({at}); it is blocked inside a sink call that does not yield; a restart re-delivers the in-flight segment from record 0")
  }

  pub(crate) fn drain_paused(since_ms: u64, held_at: Option<u64>, unbounded: bool) -> String {
      let held = held_at.map_or_else(String::new, |n| format!(" (held: mid-segment at record {n})"));
      let grows = if unbounded { "WAB grows without bound: wab_max_bytes=0" } else { "WAB grows" };
      format!("delivery paused since {since_ms}{held}; ingest keeps buffering; {grows}; restart also resumes")
  }

  pub(crate) fn drain_blocked(dl_bytes: Option<u64>, cap: Option<u64>) -> String {
      let n = |v: Option<u64>| v.map_or_else(|| "?".to_string(), |v| v.to_string());
      format!("dead_letter/ at {}/{}; free space (weir-ctl dl requeue/drop) or raise the cap", n(dl_bytes), n(cap))
  }

  pub(crate) fn delivery_stalled(undelivered: u64, secs: u64) -> String {
      format!("{undelivered} undelivered segment(s) and no successful commit for {secs}s")
  }

  pub(crate) fn delivery_lagging(age_secs: u64) -> String {
      format!("the oldest undelivered segment is up to {age_secs}s old")
  }

  pub(crate) fn segments_stranded(n: u64) -> String {
      format!("{n} segment(s) exhausted retries; auto-resumed on a healthy probe or via `weir-ctl drain retry-stranded`; each resume re-delivers from record 0")
  }

  pub(crate) fn sink_down(since_ms: Option<u64>) -> String {
      match since_ms {
          Some(t) => format!("sink unreachable since {t}; weir keeps buffering durably (not a readiness failure)"),
          None => "sink unreachable; weir keeps buffering durably (not a readiness failure)".to_string(),
      }
  }

  pub(crate) fn sink_degraded() -> String {
      "the sink's health probe reports degraded".to_string()
  }

  pub(crate) fn sink_failing(failures: u64, kind: Option<SinkFailureKind>) -> String {
      format!("{failures} consecutive commits failed while the probe passes; last kind {}", kind.map_or("unknown", SinkFailureKind::as_str))
  }

  pub(crate) fn dead_letter_near_cap(bytes: u64, cap: u64) -> String {
      format!("dead_letter/ holds {bytes} of its {cap}-byte cap")
  }

  pub(crate) fn noop_sink() -> String {
      "records are acked and DISCARDED".to_string()
  }

  pub(crate) fn active_segment_aging(shard: u16, secs: u64) -> String {
      format!("shard {shard}'s active segment is {secs}s old and both seal timers are 0; its records wait for a size rotation or shutdown")
  }
  ```
  `rustfmt` cannot wrap the long string literals and leaves them; the signature lines stay single-line.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass, including the 8 new tests.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): the delivery conditions"`

---

### Task 1.6: Durability and control-surface rules (§9.3 codes 25-30); make `rule` exhaustive

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-status/src/rules.rs`, `crates/weir-status/src/summary.rs`
- Test: `crates/weir-status/src/rules.rs`, `crates/weir-status/src/summary.rs`

**Interfaces:**
- Consumes: Tasks 1.4-1.5.
- Produces: `evaluate` decides all 30 codes; `rule` has no wildcard arm.

- [ ] **Step 1: Write the failing tests** (append inside `rules::tests`):
  ```rust
  #[test]
  fn durability_and_control_surface_conditions() {
      let e = serving_evidence(NOW);
      for c in [C::FsyncFailuresRecent, C::FlusherRespawnedRecent, C::QuarantineNonempty,
                C::RecoveryIncomplete, C::TlsReloadFailed, C::AdminUnavailable] {
          assert_eq!(outcome(c, &e), "inactive", "{c:?}");
      }
      assert_eq!(outcome(C::FsyncFailuresRecent, &with(|e| e.recent.as_mut().unwrap().fsync_failures_15m = Some(1))), "active");
      let respawned = with(|e| e.recent.as_mut().unwrap().flusher_panics_15m = Some(2));
      assert_eq!(outcome(C::FlusherRespawnedRecent, &respawned), "active");
      let mut one_down = respawned.clone();
      one_down.shards.as_mut().unwrap()[0].state = ShardState::Offline;
      assert_eq!(outcome(C::FlusherRespawnedRecent, &one_down), "inactive", "only once every shard is back up");
      assert_eq!(outcome(C::QuarantineNonempty, &with(|e| e.backlog.as_mut().unwrap().quarantine_bytes = 1)), "active");
      assert_eq!(outcome(C::RecoveryIncomplete, &with(|e| e.recent.as_mut().unwrap().recovery_quarantine_copy_failed = Some(1))), "active");
      let tls = with(|e| {
          let r = e.reload.as_mut().unwrap();
          r.tls_last_failed = true;
          r.items = vec![ReloadItemEvidence { item: ReloadItem::TlsCertificates, outcome: ReloadItemOutcome::Failed,
                                              detail: Some("bad key".into()) }];
      });
      let f = fired(C::TlsReloadFailed, &tls);
      assert_eq!(f.detail.as_deref(), Some("bad key"));
      assert!(!f.summary.contains("bad key"));
      let admin = with(|e| {
          let a = e.admin.as_mut().unwrap();
          a.state = AdminState::Unavailable;
          a.reason = Some(AdminReason::ParentInsecure);
      });
      assert!(fired(C::AdminUnavailable, &admin).summary.starts_with("admin socket not bound (parent_insecure)"));
      let disabled = with(|e| { let a = e.admin.as_mut().unwrap(); a.enabled = false; a.state = AdminState::Unavailable; });
      assert_eq!(outcome(C::AdminUnavailable, &disabled), "inactive");
  }

  #[test]
  fn full_evidence_decides_every_code_and_empty_evidence_decides_none() {
      let ev = evaluate(&serving_evidence(NOW), &Thresholds::DEFAULT, &crate::NoLatches);
      assert!(ev.unevaluated.is_empty(), "undecided on full evidence: {:?}", ev.unevaluated);
      assert!(ev.conditions.is_empty(), "a healthy daemon raised: {:?}", ev.conditions);
      let ev = evaluate(&Evidence::empty(NOW, Source::Disk), &Thresholds::DEFAULT, &crate::NoLatches);
      assert_eq!(ev.unevaluated, crate::ALL.to_vec(), "missing evidence must never read as healthy");
      assert!(ev.conditions.is_empty());
  }

  #[test]
  fn evaluate_never_emits_an_offline_only_or_unknown_code() {
      for code in [C::DaemonUnreachable, C::RecoveryPending, C::Unknown] {
          assert_eq!(outcome(code, &Evidence::empty(NOW, Source::Daemon)), "inactive");
      }
  }
  ```
  In `summary.rs`, change the last line of `summary_templates_take_no_free_text` to `assert_eq!(n, 30, "one template per daemon code");`.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status`. Expected:
  - `full_evidence_decides_every_code_and_empty_evidence_decides_none` FAILS: undecided `[FsyncFailuresRecent, FlusherRespawnedRecent, QuarantineNonempty, RecoveryIncomplete, TlsReloadFailed, AdminUnavailable]`;
  - `evaluate_never_emits_...` FAILS: `"unevaluated"` for the offline-only and unknown codes;
  - `summary_templates_take_no_free_text` FAILS: `left: 24, right: 30`.
- [ ] **Step 3: Implement.** Replace the wildcard arm of `rule` with:
  ```rust
  C::FsyncFailuresRecent => fsync_failures_recent(e),
  C::FlusherRespawnedRecent => flusher_respawned_recent(e),
  C::QuarantineNonempty => quarantine_nonempty(e),
  C::RecoveryIncomplete => recovery_incomplete(e),
  C::TlsReloadFailed => tls_reload_failed(e),
  C::AdminUnavailable => admin_unavailable(e),
  // Never evaluated by the daemon: weir-ctl emits the offline-only codes
  // itself, and Unknown only arrives from a newer daemon's document.
  C::DaemonUnreachable | C::RecoveryPending | C::Unknown => Outcome::Inactive,
  ```
  Rules:
  ```rust
  fn fsync_failures_recent(e: &Evidence) -> Outcome {
      let Some(n) = e.recent.as_ref().and_then(|r| r.fsync_failures_15m) else { return Outcome::Unevaluated };
      if n == 0 {
          return Outcome::Inactive;
      }
      Fired::new(summary::fsync_failures_recent(n)).with("fsync_failures", u(n)).into()
  }

  fn flusher_respawned_recent(e: &Evidence) -> Outcome {
      let (Some(n), Some(shards)) = (e.recent.as_ref().and_then(|r| r.flusher_panics_15m), &e.shards) else {
          return Outcome::Unevaluated;
      };
      if n == 0 || !shards.iter().all(|s| s.state == ShardState::Up) {
          return Outcome::Inactive;
      }
      Fired::new(summary::flusher_respawned_recent(n)).with("panics", u(n)).into()
  }

  fn quarantine_nonempty(e: &Evidence) -> Outcome {
      let Some(b) = &e.backlog else { return Outcome::Unevaluated };
      if b.quarantine_bytes == 0 {
          return Outcome::Inactive;
      }
      Fired::new(summary::quarantine_nonempty(b.quarantine_bytes, b.quarantine_files))
          .with("bytes", u(b.quarantine_bytes))
          .with("files", u(b.quarantine_files))
          .into()
  }

  fn recovery_incomplete(e: &Evidence) -> Outcome {
      let Some(r) = &e.recent else { return Outcome::Unevaluated };
      let (Some(failed), Some(copy)) = (r.recovery_segments_failed, r.recovery_quarantine_copy_failed) else {
          return Outcome::Unevaluated;
      };
      if failed == 0 && copy == 0 {
          return Outcome::Inactive;
      }
      Fired::new(summary::recovery_incomplete(failed, copy))
          .with("segments_failed", u(failed))
          .with("quarantine_copy_failed", u(copy))
          .into()
  }

  fn tls_reload_failed(e: &Evidence) -> Outcome {
      let Some(r) = &e.reload else { return Outcome::Unevaluated };
      if !r.tls_last_failed {
          return Outcome::Inactive;
      }
      let detail = r.items.iter().find(|i| i.item == ReloadItem::TlsCertificates).and_then(|i| i.detail.clone());
      Fired::new(summary::tls_reload_failed()).detail(detail).into()
  }

  fn admin_unavailable(e: &Evidence) -> Outcome {
      let Some(a) = &e.admin else { return Outcome::Unevaluated };
      if !(a.enabled && a.state == AdminState::Unavailable) {
          return Outcome::Inactive;
      }
      let mut f = Fired::new(summary::admin_unavailable(a.reason));
      if let Some(r) = a.reason {
          f = f.with("reason", code(r.as_str()));
      }
      f.into()
  }
  ```
  `summary.rs` (extend the `use` with `AdminReason`):
  ```rust
  pub(crate) fn fsync_failures_recent(n: u64) -> String {
      format!("{n} WAB fsync failure(s) in the last 15 min; the records in those groups were nacked, not acked")
  }

  pub(crate) fn flusher_respawned_recent(n: u64) -> String {
      format!("{n} WAB flusher panic(s) in the last 15 min; every shard is back up")
  }

  pub(crate) fn quarantine_nonempty(bytes: u64, files: u64) -> String {
      format!("quarantine/ holds {files} file(s), {bytes} bytes; inspect with weir-ctl quarantine")
  }

  pub(crate) fn recovery_incomplete(failed: u64, copy_failed: u64) -> String {
      format!("startup recovery failed for {failed} segment(s) and could not keep {copy_failed} quarantine copy(ies) in this process")
  }

  pub(crate) fn tls_reload_failed() -> String {
      "the last TLS reload failed; previous certificates still served".to_string()
  }

  pub(crate) fn admin_unavailable(reason: Option<AdminReason>) -> String {
      format!("admin socket not bound ({}); weir-ctl drain/reload unavailable, kill -HUP still reloads", reason.map_or("unknown", AdminReason::as_str))
  }
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass. Then verify the template guard by mutation:
  - add `pub(crate) fn leak(s: &str) -> String { s.to_string() }` to summary.rs;
  - `cargo test -p weir-status summary_templates` must FAIL with `free-text parameter `str``;
  - remove the line.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): durability and control-surface conditions; every code is decided"`

---

### Task 1.7: Probe predicates, `LivezCheck` and the shared exit-code table

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/probe.rs`
- Modify: `crates/weir-status/src/lib.rs`
- Test: `crates/weir-status/src/probe.rs` (`mod tests`)

**Interfaces:**
- Consumes: `rules::{rule, Outcome}` (Task 1.4), the catalogue.
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)] #[serde(rename_all = "snake_case")]
  pub enum LivezCheck { ListenerDown, NoLiveShard, WorkerExited, DrainStopped, DrainWedged }
  impl LivezCheck { pub const ALL: [LivezCheck; 5]; pub fn code(self) -> ConditionCode;
                    pub fn as_str(self) -> &'static str; pub fn parse(s: &str) -> Option<Self>; }
  pub struct ProbeCheck { pub code: ConditionCode, pub ok: bool, pub summary: Option<String> }
  pub struct ProbeVerdict { pub ok: bool, pub failing: Vec<ConditionCode>, pub checks: Vec<ProbeCheck> }
  pub const READINESS_CODES: [ConditionCode; 5];
  pub fn readiness(e: &Evidence) -> ProbeVerdict;       // fail-closed on missing evidence
  pub fn liveness(e: &Evidence, t: &Thresholds, checks: &[LivezCheck]) -> ProbeVerdict; // only while serving; fail-open
  pub fn startup(e: &Evidence) -> ProbeVerdict;         // ok iff ever_served
  pub const EXIT_READY: i32 = 0; pub const EXIT_NOT_READY: i32 = 1;
  pub const EXIT_UNREACHABLE: i32 = 2; pub const EXIT_CRITICAL: i32 = 3;
  pub fn exit_code(readiness: &ProbeVerdict, eval: &Evaluation) -> i32;
  ```

- [ ] **Step 1: Write the failing tests** (`probe.rs`, test module only):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::testing::serving_evidence;
      use crate::*;
      use ConditionCode as C;

      const NOW: u64 = 1_800_000_000_000;
      fn with(f: impl FnOnce(&mut Evidence)) -> Evidence {
          let mut e = serving_evidence(NOW);
          f(&mut e);
          e
      }

      #[test]
      fn readiness_codes_and_livez_checks_are_the_catalogue_gates() {
          let r: Vec<ConditionCode> =
              CATALOGUE.iter().filter(|e| e.gates.contains(&Gate::Readiness)).map(|e| e.code).collect();
          assert_eq!(r, READINESS_CODES.to_vec());
          let l: Vec<ConditionCode> =
              CATALOGUE.iter().filter(|e| e.gates.contains(&Gate::Liveness)).map(|e| e.code).collect();
          assert_eq!(l, LivezCheck::ALL.iter().map(|c| c.code()).collect::<Vec<_>>());
          for c in LivezCheck::ALL {
              assert_eq!(LivezCheck::parse(c.as_str()), Some(c));
              assert_eq!(serde_json::to_string(&c).unwrap(), format!("\"{}\"", c.as_str()));
          }
          assert_eq!(LivezCheck::parse("sink_down"), None);
      }

      #[test]
      fn readiness_truth_table() {
          assert!(readiness(&serving_evidence(NOW)).ok);
          let cases: Vec<(Evidence, ConditionCode)> = vec![
              (with(|e| e.phase = Some(Phase::Binding)), C::Starting),
              (with(|e| { e.shutting_down = Some(true); e.phase = Some(Phase::ShuttingDown) }), C::ShuttingDown),
              (with(|e| e.listeners.as_mut().unwrap()[0].state = ListenerState::Failed), C::ListenerDown),
              (with(|e| for s in e.shards.as_mut().unwrap() { s.state = ShardState::Offline }), C::NoLiveShard),
              (with(|e| e.wab_cap.as_mut().unwrap().rejecting = true), C::WabCapRejecting),
          ];
          for (e, code) in cases {
              let v = readiness(&e);
              assert!(!v.ok, "{code:?}");
              assert_eq!(v.failing, vec![code]);
              assert!(v.checks.iter().any(|c| c.code == code && !c.ok && c.summary.is_some()));
          }
      }

      #[test]
      fn readiness_ignores_sink_drain_pause_and_history() {
          let e = with(|e| {
              e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Down);
              e.drain.as_mut().unwrap().exit = DrainExitCode::GaveUp;
              e.drain.as_mut().unwrap().paused = true;
              e.recent.as_mut().unwrap().fsync_failures_15m = Some(9);
              e.backlog.as_mut().unwrap().quarantine_bytes = 10;
          });
          assert!(readiness(&e).ok);
      }

      #[test]
      fn readiness_fails_closed_on_missing_evidence() {
          let v = readiness(&Evidence::empty(NOW, Source::Disk));
          assert!(!v.ok);
          assert_eq!(v.failing, READINESS_CODES.to_vec());
          assert!(v.checks.iter().all(|c| c.summary.is_none()));
      }

      #[test]
      fn liveness_is_opt_in_serving_only_and_fail_open() {
          let exited = with(|e| {
              let w = &mut e.workers.as_mut().unwrap()[0];
              w.running = false;
              w.exited_at_unix_ms = Some(NOW);
          });
          assert!(liveness(&exited, &Thresholds::DEFAULT, &[]).ok, "default livez_checks = []");
          let v = liveness(&exited, &Thresholds::DEFAULT, &[LivezCheck::WorkerExited]);
          assert!(!v.ok);
          assert_eq!(v.failing, vec![C::WorkerExited]);
          let mut starting = exited.clone();
          starting.phase = Some(Phase::Recovering);
          assert!(liveness(&starting, &Thresholds::DEFAULT, &LivezCheck::ALL).ok, "only while serving");
          let no_drain = with(|e| e.drain = None);
          assert!(liveness(&no_drain, &Thresholds::DEFAULT, &[LivezCheck::DrainWedged]).ok,
                  "unknown is not dead: a restart must not be ordered on missing evidence");
      }

      #[test]
      fn startup_is_sticky_once_served() {
          assert!(startup(&serving_evidence(NOW)).ok);
          let v = startup(&with(|e| { e.ever_served = Some(false); e.phase = Some(Phase::Recovering) }));
          assert!(!v.ok);
          assert_eq!(v.failing, vec![C::Starting]);
          assert!(v.checks[0].summary.as_deref().unwrap().starts_with("startup phase recovering"));
          let later = with(|e| { e.phase = Some(Phase::ShuttingDown); e.shutting_down = Some(true) });
          assert!(startup(&later).ok, "sticky after serving");
      }

      #[test]
      fn exit_codes_and_the_parity_with_readyz() {
          let fixtures: Vec<Evidence> = vec![
              serving_evidence(NOW),
              with(|e| e.phase = Some(Phase::Handoff)),
              with(|e| e.wab_cap.as_mut().unwrap().rejecting = true),
              with(|e| e.drain.as_mut().unwrap().exit = DrainExitCode::GaveUp),
              with(|e| e.shards.as_mut().unwrap()[0].state = ShardState::Offline),
              with(|e| e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Down)),
              Evidence::empty(NOW, Source::Disk),
          ];
          for e in &fixtures {
              let r = readiness(e);
              let eval = evaluate(e, &Thresholds::DEFAULT, &NoLatches);
              // parity: readiness == "no active R-gated condition and none unevaluated"
              let r_from_eval = !eval.conditions.iter().any(|c| c.gates.contains(&Gate::Readiness))
                  && !eval.unevaluated.iter().any(|c| READINESS_CODES.contains(c));
              assert_eq!(r.ok, r_from_eval, "{:?}", e.phase);
              let x = exit_code(&r, &eval);
              assert_eq!(x == EXIT_READY || x == EXIT_CRITICAL, r.ok);
          }
          let ok = serving_evidence(NOW);
          assert_eq!(exit_code(&readiness(&ok), &evaluate(&ok, &Thresholds::DEFAULT, &NoLatches)), EXIT_READY);
          let crit = &fixtures[3];
          assert_eq!(exit_code(&readiness(crit), &evaluate(crit, &Thresholds::DEFAULT, &NoLatches)), EXIT_CRITICAL);
          let not_ready = &fixtures[1];
          assert_eq!(exit_code(&readiness(not_ready), &evaluate(not_ready, &Thresholds::DEFAULT, &NoLatches)),
                     EXIT_NOT_READY);
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status probe::`. Expected: compile errors `cannot find function `readiness`` / `LivezCheck` (after adding `mod probe;` to lib.rs).
- [ ] **Step 3: Implement** (`probe.rs`, above the tests; lib.rs gets `mod probe; pub use probe::*;`):
  ```rust
  //! Probe predicates (spec §10, §12) and the exit-code table weir-ctl and
  //! weir-readiness.sh share (spec §14). Each predicate runs the SAME rules
  //! `evaluate` runs, so `/readyz` and the Conditions list cannot disagree.
  use crate::codes::{ConditionCode, Severity};
  use crate::condition::{Evaluation, Thresholds};
  use crate::evidence::{Evidence, Phase};
  use crate::rules::{Outcome, rule};
  use serde::{Deserialize, Serialize};

  /// A check selectable in `livez_checks` (spec §12), in catalogue order.
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum LivezCheck {
      ListenerDown,
      NoLiveShard,
      WorkerExited,
      DrainStopped,
      DrainWedged,
  }

  impl LivezCheck {
      pub const ALL: [LivezCheck; 5] = [
          LivezCheck::ListenerDown,
          LivezCheck::NoLiveShard,
          LivezCheck::WorkerExited,
          LivezCheck::DrainStopped,
          LivezCheck::DrainWedged,
      ];
      pub fn code(self) -> ConditionCode {
          match self {
              LivezCheck::ListenerDown => ConditionCode::ListenerDown,
              LivezCheck::NoLiveShard => ConditionCode::NoLiveShard,
              LivezCheck::WorkerExited => ConditionCode::WorkerExited,
              LivezCheck::DrainStopped => ConditionCode::DrainStopped,
              LivezCheck::DrainWedged => ConditionCode::DrainWedged,
          }
      }
      pub fn as_str(self) -> &'static str {
          self.code().as_str()
      }
      pub fn parse(s: &str) -> Option<Self> {
          Self::ALL.into_iter().find(|c| c.as_str() == s)
      }
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct ProbeCheck {
      pub code: ConditionCode,
      pub ok: bool,
      /// The public summary of a failing check (`None` when it passed, or
      /// when it failed for missing evidence).
      pub summary: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct ProbeVerdict {
      pub ok: bool,
      pub failing: Vec<ConditionCode>,
      pub checks: Vec<ProbeCheck>,
  }

  impl ProbeVerdict {
      fn passing() -> Self {
          ProbeVerdict { ok: true, failing: Vec::new(), checks: Vec::new() }
      }
      fn pass(&mut self, code: ConditionCode) {
          self.checks.push(ProbeCheck { code, ok: true, summary: None });
      }
      fn fail(&mut self, code: ConditionCode, summary: Option<String>) {
          self.ok = false;
          self.failing.push(code);
          self.checks.push(ProbeCheck { code, ok: false, summary });
      }
      fn record(&mut self, code: ConditionCode, outcome: Outcome, fail_closed: bool) {
          match outcome {
              Outcome::Inactive => self.pass(code),
              Outcome::Active(f) => self.fail(code, Some(f.summary)),
              Outcome::Unevaluated if fail_closed => self.fail(code, None),
              Outcome::Unevaluated => self.pass(code),
          }
      }
  }

  /// The R-gated codes (spec §9.3), in catalogue order.
  pub const READINESS_CODES: [ConditionCode; 5] = [
      ConditionCode::Starting,
      ConditionCode::ShuttingDown,
      ConditionCode::ListenerDown,
      ConditionCode::NoLiveShard,
      ConditionCode::WabCapRejecting,
  ];

  /// `/readyz` (spec §10, owner decision 4): serving, not shutting down, not
  /// cap-rejecting, at least one ingest-capable shard, no listener down.
  /// Missing evidence fails the check (fail-closed).
  pub fn readiness(e: &Evidence) -> ProbeVerdict {
      let t = Thresholds::DEFAULT;
      let mut v = ProbeVerdict::passing();
      for code in READINESS_CODES {
          v.record(code, rule(code, e, &t), true);
      }
      v
  }

  /// `/livez` (spec §12): only the selected checks, only while serving, and
  /// only on positive evidence (a restart must never be ordered on a gap).
  pub fn liveness(e: &Evidence, t: &Thresholds, checks: &[LivezCheck]) -> ProbeVerdict {
      let serving = e.phase == Some(Phase::Serving);
      let mut v = ProbeVerdict::passing();
      for c in checks {
          if serving {
              v.record(c.code(), rule(c.code(), e, t), false);
          } else {
              v.pass(c.code());
          }
      }
      v
  }

  /// `/startupz` (spec §10): 200 once the daemon has ever served; sticky.
  pub fn startup(e: &Evidence) -> ProbeVerdict {
      let mut v = ProbeVerdict::passing();
      if e.ever_served == Some(true) {
          v.pass(ConditionCode::Starting);
      } else {
          let summary = match rule(ConditionCode::Starting, e, &Thresholds::DEFAULT) {
              Outcome::Active(f) => Some(f.summary),
              Outcome::Inactive | Outcome::Unevaluated => None,
          };
          v.fail(ConditionCode::Starting, summary);
      }
      v
  }

  pub const EXIT_READY: i32 = 0;
  pub const EXIT_NOT_READY: i32 = 1;
  pub const EXIT_UNREACHABLE: i32 = 2;
  pub const EXIT_CRITICAL: i32 = 3;

  /// The shared table (spec §14): 1 exactly when `/readyz` would be 503;
  /// 3 when ready but a critical condition is active; else 0. (2, unreachable
  /// or identity mismatch, is decided by the caller before any evidence exists.)
  pub fn exit_code(readiness: &ProbeVerdict, eval: &Evaluation) -> i32 {
      if !readiness.ok {
          EXIT_NOT_READY
      } else if eval.conditions.iter().any(|c| c.severity == Severity::Critical) {
          EXIT_CRITICAL
      } else {
          EXIT_READY
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass (7 new).
- [ ] **Step 5: Commit.** `git add crates/weir-status/src && git commit -m "feat(status): readiness, liveness and startup predicates with the shared exit codes"`

---
### Task 1.8: `AdminStatusDoc` (`weir.status/v1`, admin view) and the golden document

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/doc.rs`, `crates/weir-status/tests/golden.rs`, `crates/weir-status/tests/golden/status_v1_admin.json` (blessed in Step 3)
- Modify: `crates/weir-status/src/lib.rs`, `crates/weir-status/src/testing.rs` (add `full_evidence`, `FixedLatches`)

**Interfaces:**
- Consumes: Tasks 1.2-1.7.
- Produces (field names are spec §9.1's; PR 5 builds `PublicStatusDoc` / `public()` from these):
  ```rust
  pub const SCHEMA: &str = "weir.status/v1";
  pub const CHANNEL_CAPACITY: u64 = 256;
  pub const ADMIN_PROTOCOL: u32 = 1;
  pub const ADMIN_VERBS: [&str; 5] = ["status", "reload", "drain.pause", "drain.resume", "drain.retry_stranded"];
  pub const MAX_DOC_SHARDS: usize = 256;  pub const MAX_DOC_CONDITIONS: usize = 40;
  pub enum View { Public, Admin }
  pub struct AdminStatusDoc { pub schema: String, pub view: View, pub version: String, pub wire_version: u8,
      pub generated_at_ms: u64, pub started_at_ms: u64, pub uptime_secs: f64, pub pid: u32,
      pub instance: InstanceView, pub verdict: Verdict, pub conditions: Vec<Condition>,
      pub unevaluated: Vec<ConditionCode>, pub startup: StartupView, pub lifecycle: LifecycleView,
      pub ingest: IngestView, pub shards: Vec<ShardView>, pub drain: Option<AdminDrainView>,
      pub sink: Option<AdminSinkView>, pub backlog: Option<BacklogView>, pub reload: AdminReloadView,
      pub admin: AdminSurfaceView, pub config: AdminConfigDoc }
  impl AdminStatusDoc { pub fn build(identity: &IdentityEvidence, e: &Evidence, eval: &Evaluation,
                                     t: &Thresholds, livez_checks: &[LivezCheck]) -> Self; }
  pub struct InstanceView { pub wab_dir: String, pub socket_path: String, pub wab_dir_dev: u64, pub wab_dir_ino: u64 }
  pub struct Verdict { pub ready: bool, pub live: bool, pub startup_complete: bool,
      pub livez_checks: Vec<LivezCheck>, pub readiness_failing: Vec<ConditionCode>, pub liveness_failing: Vec<ConditionCode> }
  pub struct ReplayView { pub total: u64, pub remaining: u64 }
  pub struct StartupView { pub phase: Option<Phase>, pub phase_since_ms: Option<u64>, pub replay: Option<ReplayView> }
  pub struct LifecycleView { pub shutting_down: Option<bool>, pub shutting_down_since_ms: Option<u64>,
      pub forced: Option<bool>, pub drain_deadline_ms: Option<u64> }
  pub struct ListenerView { pub kind: ListenerKind, pub state: ListenerState }
  pub struct WabCapView { pub max_bytes: u64, pub low_water_bytes: u64, pub bytes_now: u64, pub rejecting: bool,
      pub last_scan_ok_ms: Option<u64> }
  pub struct IngestView { pub listeners: Vec<ListenerView>, pub queue_depth: Option<u64>, pub wab_cap: Option<WabCapView> }
  pub struct ShardView { pub id: u16, pub state: ShardState, pub reason: Option<OfflineReason>, pub since_ms: u64,
      pub respawns: u32, pub worker_running: bool, pub handoff_blocked_since_ms: Option<u64>,
      pub active_segment_age_secs: Option<f64> }
  pub struct InFlightView { pub segment: SegmentId, pub attempt: u32, pub retries_left: u32,
      pub processed_records: u64, pub next_retry_at_ms: Option<u64>, pub commit_in_flight_since_ms: Option<u64> }
  pub struct AdminRequeueView { pub ticket: u64, pub requeued: u64, pub already_queued: u64, pub backoff_cut: bool,
      pub coalesced: bool, pub scan_failed: bool, pub at_ms: u64, pub scan_error: Option<String> }
  pub struct AdminDrainView { pub state: DrainStateCode, pub state_since_ms: Option<u64>, pub paused: bool,
      pub pause_requested: bool, pub held: bool, pub paused_since_ms: Option<u64>, pub paused_by_uid: Option<u32>,
      pub generation: u64, pub observed_requeue: u64, pub in_flight: Option<InFlightView>,
      pub pending_segments: u64, pub backlog_segments: u64, pub channel_depth: Option<u64>,
      pub channel_capacity: u64, pub heartbeat_age_secs: Option<f64>, pub blocked_since_ms: Option<u64>,
      pub last_requeue: Option<AdminRequeueView>, pub exit: DrainExitCode, pub last_panic: Option<String> }
  pub struct AdminSinkFailure { pub at_ms: u64, pub kind: Option<SinkFailureKind>, pub message: Option<String> }
  pub struct AdminSinkView { #[serde(rename = "type")] pub sink_type: String, pub health: Option<SinkHealthCode>,
      pub health_since_ms: Option<u64>, pub last_success_ms: Option<u64>,
      pub last_failure: Option<AdminSinkFailure>, pub consecutive_failures: u64, pub probe_reason: Option<String> }
  pub struct BacklogView { pub undelivered_segments: u64, pub undelivered_records: u64,
      pub oldest_undelivered_age_secs: Option<f64>, pub dead_letter_bytes: u64,
      pub dead_letter_max_bytes: Option<u64>, pub quarantine_bytes: u64, pub quarantine_files: u64, pub scanned_at_ms: u64 }
  pub struct AdminReloadItem { pub item: String, pub outcome: ReloadItemOutcome, pub detail: Option<String> }
  pub struct NotReloadedView { pub item: String, pub reason_code: String }
  pub struct AdminReloadLast { pub trigger: ReloadTriggerCode, pub at_ms: u64, pub outcome: ReloadOutcomeCode,
      pub items: Vec<AdminReloadItem>, pub not_reloaded: Vec<NotReloadedView> }
  pub struct AdminReloadView { pub last: Option<AdminReloadLast> }
  pub struct AdminSurfaceView { pub protocol: u32, pub verbs: Vec<String>, pub state: Option<AdminState>,
      pub reason_code: Option<String>, pub path: Option<String> }
  pub struct AdminConfigDoc { pub public: PublicConfig, pub view: BTreeMap<String, ConfigValue> }
  // all derive Clone, Debug, PartialEq, Serialize, Deserialize
  #[doc(hidden)] pub mod testing { pub fn full_evidence(now: u64) -> Evidence; pub struct FixedLatches(pub u64); }
  ```

- [ ] **Step 1: Write the failing tests.** `crates/weir-status/tests/golden.rs`:
  ```rust
  //! The admin rendering of `weir.status/v1` (spec §9.1). The golden file is the
  //! contract weir-ctl and PR 5's public projection read; re-bless only on an
  //! intended, additive change: `WEIR_BLESS=1 cargo test -p weir-status --test golden`.
  use serde_json::{Value, json};
  use std::path::Path;
  use weir_status::testing::{FixedLatches, full_evidence};
  use weir_status::*;

  const NOW: u64 = 1_800_000_000_000;

  fn doc() -> AdminStatusDoc {
      let e = full_evidence(NOW);
      let eval = evaluate(&e, &Thresholds::DEFAULT, &FixedLatches(NOW - 60_000));
      AdminStatusDoc::build(e.identity.as_ref().unwrap(), &e, &eval, &Thresholds::DEFAULT, &[LivezCheck::NoLiveShard])
  }

  fn golden_check(rel: &str, actual: Value) {
      let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
      if std::env::var_os("WEIR_BLESS").is_some() {
          std::fs::create_dir_all(path.parent().unwrap()).unwrap();
          std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
          return;
      }
      let text = std::fs::read_to_string(&path)
          .unwrap_or_else(|_| panic!("run once with WEIR_BLESS=1 to create {}", path.display()));
      let expected: Value = serde_json::from_str(&text).unwrap();
      assert_eq!(actual, expected, "{rel} drifted; if intended, re-bless with WEIR_BLESS=1 and review the diff");
  }

  #[test]
  fn admin_golden_matches() {
      golden_check("tests/golden/status_v1_admin.json", serde_json::to_value(doc()).unwrap());
  }

  #[test]
  fn admin_top_level_keys_are_the_spec_9_1_set() {
      let v = serde_json::to_value(doc()).unwrap();
      let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
      keys.sort_unstable();
      assert_eq!(keys, vec![
          "admin", "backlog", "conditions", "config", "drain", "generated_at_ms", "ingest", "instance",
          "lifecycle", "pid", "reload", "schema", "shards", "sink", "started_at_ms", "startup",
          "unevaluated", "uptime_secs", "verdict", "version", "view", "wire_version",
      ]);
      assert_eq!(v["schema"], "weir.status/v1");
      assert_eq!(v["view"], "admin");
      assert_eq!(v["drain"]["channel_capacity"], 256);
  }

  #[test]
  fn the_golden_populates_every_admin_only_field_pr5_plants_sentinels_into() {
      let v = serde_json::to_value(doc()).unwrap();
      for pointer in [
          "/instance/wab_dir", "/instance/socket_path", "/drain/paused_by_uid", "/drain/last_panic",
          "/drain/in_flight/segment", "/drain/last_requeue/scan_error", "/sink/last_failure/message",
          "/sink/probe_reason", "/reload/last/items/0/detail", "/admin/path", "/config/view/wab_dir",
          "/conditions/0/detail", "/shards/0/id",
      ] {
          let x = v.pointer(pointer).unwrap_or_else(|| panic!("{pointer} missing"));
          assert!(!x.is_null(), "{pointer} is null; the fixture must populate it");
      }
  }

  #[test]
  fn verdict_and_conditions_come_from_the_shared_evaluator() {
      let d = doc();
      assert!(d.verdict.ready);
      assert!(d.verdict.startup_complete);
      assert_eq!(d.verdict.livez_checks, vec![LivezCheck::NoLiveShard]);
      let codes: Vec<ConditionCode> = d.conditions.iter().map(|c| c.code).collect();
      assert_eq!(codes, vec![ConditionCode::DrainPaused, ConditionCode::SinkFailing, ConditionCode::TlsReloadFailed]);
      assert_eq!(d.conditions[0].since_unix_ms, Some(NOW - 60_000));
      assert!(d.unevaluated.is_empty());
  }

  #[test]
  fn a_newer_daemons_document_still_parses() {
      let mut v = serde_json::to_value(doc()).unwrap();
      v["conditions"][0]["code"] = json!("brand_new_code");
      v["brand_new_section"] = json!({ "x": 1 });
      v["drain"]["brand_new_field"] = json!(true);
      let d: AdminStatusDoc = serde_json::from_value(v).unwrap();
      assert_eq!(d.conditions[0].code, ConditionCode::Unknown);
      let again = serde_json::to_value(&d).unwrap();
      assert_eq!(again["conditions"][0]["code"], "brand_new_code");
  }

  #[test]
  fn the_document_is_bounded() {
      let mut e = full_evidence(NOW);
      let template = e.shards.as_ref().unwrap()[0].clone();
      e.shards = Some((0..300u16).map(|id| ShardEvidence { id, ..template.clone() }).collect());
      let eval = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
      let d = AdminStatusDoc::build(e.identity.as_ref().unwrap(), &e, &eval, &Thresholds::DEFAULT, &[]);
      assert_eq!(d.shards.len(), MAX_DOC_SHARDS);
      assert!(d.conditions.len() <= MAX_DOC_CONDITIONS);
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status --test golden`. Expected: compile errors `cannot find type `AdminStatusDoc`` / `full_evidence` / `FixedLatches`.
- [ ] **Step 3: Implement.** `testing.rs` additions:
  ```rust
  /// A latch table where every active condition was first seen at `.0`.
  pub struct FixedLatches(pub u64);

  impl SinceLookup for FixedLatches {
      fn since(&self, _code: ConditionCode) -> Option<u64> {
          Some(self.0)
      }
  }

  /// `serving_evidence` with every admin-only free-text field populated and
  /// three conditions active (drain_paused, sink_failing, tls_reload_failed),
  /// so the golden document exercises every Option PR 5's sentinel test plants
  /// into. Strings are recognisable, never real secrets.
  pub fn full_evidence(now: u64) -> Evidence {
      let mut e = serving_evidence(now);
      let d = e.drain.as_mut().unwrap();
      d.pause_requested = true;
      d.paused = true;
      d.held = true;
      d.paused_since_unix_ms = Some(now - 30_000);
      d.paused_by_uid = Some(1000);
      d.in_flight = Some(InFlightEvidence {
          segment: SegmentId { shard: 1, counter: 42 }, attempt: 2, retries_left: 1,
          processed_records: 512, next_retry_at_unix_ms: Some(now + 4_000),
      });
      d.commit_in_flight_since_unix_ms = Some(now - 500);
      d.last_requeue = Some(RequeueEvidence {
          ticket: 3, requeued: 2, already_queued: 1, backoff_cut: true, coalesced: false,
          scan_error: Some("fixture scan error text".into()), at_unix_ms: now - 10_000,
      });
      d.last_panic = Some("fixture panic text".into());
      d.replay_total = 300;
      d.replay_remaining = 12;
      d.pending_segments = 3;
      d.backlog_segments = 12;
      let s = e.sink.as_mut().unwrap();
      s.consecutive_failures = 4;
      s.last_failure_unix_ms = Some(now - 2_000);
      s.last_failure_kind = Some(SinkFailureKind::Transient);
      s.last_error = Some("fixture sink error text".into());
      s.probe_reason = Some("fixture probe reason text".into());
      let b = e.backlog.as_mut().unwrap();
      b.undelivered_segments = 15;
      b.undelivered_records = 4_096;
      b.oldest_undelivered_age_secs = Some(120.5);
      b.dead_letter_bytes = 1_024;
      let r = e.reload.as_mut().unwrap();
      r.outcome = ReloadOutcomeCode::Failed;
      r.tls_last_failed = true;
      r.items = vec![ReloadItemEvidence {
          item: ReloadItem::TlsCertificates, outcome: ReloadItemOutcome::Failed,
          detail: Some("fixture reload detail text".into()),
      }];
      let c = e.config.as_mut().unwrap();
      c.view.insert("wab_dir".into(), ConfigValue::Str("/var/lib/weir/wab".into()));
      c.view.insert("sink_url".into(), ConfigValue::Str("https://sink.example:443".into()));
      c.view.insert("sink_url_path_redacted".into(), ConfigValue::Bool(true));
      e.queue_depth = Some(7);
      e.channel = Some(ChannelEvidence { depth: 17, capacity: 256, high_since_unix_ms: None });
      e
  }
  ```
  `doc.rs`:
  ```rust
  //! `weir.status/v1`, admin rendering (spec §9.1). The public rendering is a
  //! separate type built from this one (PR 5). Conventions: times are unix-ms
  //! integers, ages f64 seconds, an unknown value is `null`. Additive-only.
  use crate::condition::{Condition, Evaluation, Thresholds};
  use crate::codes::ConditionCode;
  use crate::config::{ConfigValue, PublicConfig};
  use crate::evidence::*;
  use crate::probe::{LivezCheck, liveness, readiness, startup};
  use crate::segment::SegmentId;
  use serde::{Deserialize, Serialize};
  use std::collections::BTreeMap;

  pub const SCHEMA: &str = "weir.status/v1";
  pub const CHANNEL_CAPACITY: u64 = 256;
  pub const ADMIN_PROTOCOL: u32 = 1;
  pub const ADMIN_VERBS: [&str; 5] = ["status", "reload", "drain.pause", "drain.resume", "drain.retry_stranded"];
  /// Spec §10: the document is bounded at 256 shards and 40 conditions.
  pub const MAX_DOC_SHARDS: usize = 256;
  pub const MAX_DOC_CONDITIONS: usize = 40;

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum View {
      Public,
      Admin,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct AdminStatusDoc {
      pub schema: String,
      pub view: View,
      pub version: String,
      pub wire_version: u8,
      pub generated_at_ms: u64,
      pub started_at_ms: u64,
      pub uptime_secs: f64,
      pub pid: u32,
      pub instance: InstanceView,
      pub verdict: Verdict,
      pub conditions: Vec<Condition>,
      pub unevaluated: Vec<ConditionCode>,
      pub startup: StartupView,
      pub lifecycle: LifecycleView,
      pub ingest: IngestView,
      pub shards: Vec<ShardView>,
      pub drain: Option<AdminDrainView>,
      pub sink: Option<AdminSinkView>,
      pub backlog: Option<BacklogView>,
      pub reload: AdminReloadView,
      pub admin: AdminSurfaceView,
      pub config: AdminConfigDoc,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct InstanceView {
      pub wab_dir: String,
      pub socket_path: String,
      pub wab_dir_dev: u64,
      pub wab_dir_ino: u64,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct Verdict {
      pub ready: bool,
      pub live: bool,
      pub startup_complete: bool,
      pub livez_checks: Vec<LivezCheck>,
      pub readiness_failing: Vec<ConditionCode>,
      pub liveness_failing: Vec<ConditionCode>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct ReplayView {
      pub total: u64,
      pub remaining: u64,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct StartupView {
      pub phase: Option<Phase>,
      pub phase_since_ms: Option<u64>,
      pub replay: Option<ReplayView>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct LifecycleView {
      pub shutting_down: Option<bool>,
      pub shutting_down_since_ms: Option<u64>,
      pub forced: Option<bool>,
      pub drain_deadline_ms: Option<u64>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct ListenerView {
      pub kind: ListenerKind,
      pub state: ListenerState,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct WabCapView {
      pub max_bytes: u64,
      pub low_water_bytes: u64,
      pub bytes_now: u64,
      pub rejecting: bool,
      pub last_scan_ok_ms: Option<u64>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct IngestView {
      pub listeners: Vec<ListenerView>,
      pub queue_depth: Option<u64>,
      pub wab_cap: Option<WabCapView>,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct ShardView {
      pub id: u16,
      pub state: ShardState,
      pub reason: Option<OfflineReason>,
      pub since_ms: u64,
      pub respawns: u32,
      pub worker_running: bool,
      pub handoff_blocked_since_ms: Option<u64>,
      pub active_segment_age_secs: Option<f64>,
  }

  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct InFlightView {
      pub segment: SegmentId,
      pub attempt: u32,
      pub retries_left: u32,
      pub processed_records: u64,
      pub next_retry_at_ms: Option<u64>,
      pub commit_in_flight_since_ms: Option<u64>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminRequeueView {
      pub ticket: u64,
      pub requeued: u64,
      pub already_queued: u64,
      pub backoff_cut: bool,
      pub coalesced: bool,
      pub scan_failed: bool,
      pub at_ms: u64,
      pub scan_error: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct AdminDrainView {
      pub state: DrainStateCode,
      pub state_since_ms: Option<u64>,
      pub paused: bool,
      pub pause_requested: bool,
      pub held: bool,
      pub paused_since_ms: Option<u64>,
      pub paused_by_uid: Option<u32>,
      pub generation: u64,
      pub observed_requeue: u64,
      pub in_flight: Option<InFlightView>,
      pub pending_segments: u64,
      pub backlog_segments: u64,
      pub channel_depth: Option<u64>,
      pub channel_capacity: u64,
      pub heartbeat_age_secs: Option<f64>,
      pub blocked_since_ms: Option<u64>,
      pub last_requeue: Option<AdminRequeueView>,
      pub exit: DrainExitCode,
      pub last_panic: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminSinkFailure {
      pub at_ms: u64,
      pub kind: Option<SinkFailureKind>,
      pub message: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminSinkView {
      #[serde(rename = "type")]
      pub sink_type: String,
      pub health: Option<SinkHealthCode>,
      pub health_since_ms: Option<u64>,
      pub last_success_ms: Option<u64>,
      pub last_failure: Option<AdminSinkFailure>,
      pub consecutive_failures: u64,
      pub probe_reason: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  pub struct BacklogView {
      pub undelivered_segments: u64,
      pub undelivered_records: u64,
      pub oldest_undelivered_age_secs: Option<f64>,
      pub dead_letter_bytes: u64,
      pub dead_letter_max_bytes: Option<u64>,
      pub quarantine_bytes: u64,
      pub quarantine_files: u64,
      pub scanned_at_ms: u64,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminReloadItem {
      pub item: String,
      pub outcome: ReloadItemOutcome,
      pub detail: Option<String>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct NotReloadedView {
      pub item: String,
      pub reason_code: String,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminReloadLast {
      pub trigger: ReloadTriggerCode,
      pub at_ms: u64,
      pub outcome: ReloadOutcomeCode,
      pub items: Vec<AdminReloadItem>,
      pub not_reloaded: Vec<NotReloadedView>,
  }

  #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminReloadView {
      pub last: Option<AdminReloadLast>,
  }

  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct AdminSurfaceView {
      pub protocol: u32,
      pub verbs: Vec<String>,
      pub state: Option<AdminState>,
      pub reason_code: Option<String>,
      pub path: Option<String>,
  }

  #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
  pub struct AdminConfigDoc {
      pub public: PublicConfig,
      /// The full ConfigView (spec §5.14). Admin only by construction.
      pub view: BTreeMap<String, ConfigValue>,
  }

  impl AdminStatusDoc {
      /// Renders one admin document from one set of facts and its evaluation.
      /// `identity` is required: only the daemon itself builds this document.
      pub fn build(
          identity: &IdentityEvidence,
          e: &Evidence,
          eval: &Evaluation,
          t: &Thresholds,
          livez_checks: &[LivezCheck],
      ) -> Self {
          let ready = readiness(e);
          let live = liveness(e, t, livez_checks);
          let started = startup(e);
          let now = e.now_unix_ms;
          AdminStatusDoc {
              schema: SCHEMA.to_string(),
              view: View::Admin,
              version: identity.version.clone(),
              wire_version: identity.wire_version,
              generated_at_ms: now,
              started_at_ms: identity.started_at_unix_ms,
              uptime_secs: now.saturating_sub(identity.started_at_unix_ms) as f64 / 1000.0,
              pid: identity.pid,
              instance: InstanceView {
                  wab_dir: identity.wab_dir.clone(),
                  socket_path: identity.socket_path.clone(),
                  wab_dir_dev: identity.wab_dir_dev,
                  wab_dir_ino: identity.wab_dir_ino,
              },
              verdict: Verdict {
                  ready: ready.ok,
                  live: live.ok,
                  startup_complete: started.ok,
                  livez_checks: livez_checks.to_vec(),
                  readiness_failing: ready.failing,
                  liveness_failing: live.failing,
              },
              conditions: eval.conditions.iter().take(MAX_DOC_CONDITIONS).cloned().collect(),
              unevaluated: eval.unevaluated.clone(),
              startup: StartupView {
                  phase: e.phase,
                  phase_since_ms: e.phase_since_unix_ms,
                  replay: e.drain.as_ref().map(|d| ReplayView { total: d.replay_total, remaining: d.replay_remaining }),
              },
              lifecycle: LifecycleView {
                  shutting_down: e.shutting_down,
                  shutting_down_since_ms: e.shutting_down_since_unix_ms,
                  forced: e.forced,
                  drain_deadline_ms: e.drain_deadline_unix_ms,
              },
              ingest: IngestView {
                  listeners: e.listeners.iter().flatten().map(|l| ListenerView { kind: l.kind, state: l.state }).collect(),
                  queue_depth: e.queue_depth,
                  wab_cap: e.wab_cap.map(|c| WabCapView {
                      max_bytes: c.max_bytes,
                      low_water_bytes: c.low_water_bytes,
                      bytes_now: c.bytes_now,
                      rejecting: c.rejecting,
                      last_scan_ok_ms: c.last_scan_ok_unix_ms,
                  }),
              },
              shards: e.shards.iter().flatten().take(MAX_DOC_SHARDS).map(shard_view).collect(),
              drain: e.drain.as_ref().map(|d| drain_view(d, e.channel.as_ref())),
              sink: e.sink.as_ref().map(|s| sink_view(s, e.config.as_ref())),
              backlog: e.backlog.as_ref().map(|b| BacklogView {
                  undelivered_segments: b.undelivered_segments,
                  undelivered_records: b.undelivered_records,
                  oldest_undelivered_age_secs: b.oldest_undelivered_age_secs,
                  dead_letter_bytes: b.dead_letter_bytes,
                  dead_letter_max_bytes: b.dead_letter_max_bytes,
                  quarantine_bytes: b.quarantine_bytes,
                  quarantine_files: b.quarantine_files,
                  scanned_at_ms: b.scanned_at_ms,
              }),
              reload: AdminReloadView { last: e.reload.as_ref().map(reload_last) },
              admin: AdminSurfaceView {
                  protocol: ADMIN_PROTOCOL,
                  verbs: ADMIN_VERBS.iter().map(|v| v.to_string()).collect(),
                  state: e.admin.as_ref().map(|a| a.state),
                  reason_code: e.admin.as_ref().and_then(|a| a.reason).map(|r| r.as_str().to_string()),
                  path: e.admin.as_ref().and_then(|a| a.path.clone()),
              },
              config: e
                  .config
                  .as_ref()
                  .map(|c| AdminConfigDoc { public: c.public.clone(), view: c.view.clone() })
                  .unwrap_or_default(),
          }
      }
  }

  fn shard_view(s: &ShardEvidence) -> ShardView {
      ShardView {
          id: s.id,
          state: s.state,
          reason: s.reason,
          since_ms: s.since_unix_ms,
          respawns: s.respawns,
          worker_running: s.worker_running,
          handoff_blocked_since_ms: s.handoff_blocked_since_unix_ms,
          active_segment_age_secs: s.active_segment_age_secs,
      }
  }

  fn drain_view(d: &DrainEvidence, channel: Option<&ChannelEvidence>) -> AdminDrainView {
      AdminDrainView {
          state: d.state,
          state_since_ms: Some(d.state_since_unix_ms),
          paused: d.paused,
          pause_requested: d.pause_requested,
          held: d.held,
          paused_since_ms: d.paused_since_unix_ms,
          paused_by_uid: d.paused_by_uid,
          generation: d.generation,
          observed_requeue: d.observed_requeue,
          in_flight: d.in_flight.map(|i| InFlightView {
              segment: i.segment,
              attempt: i.attempt,
              retries_left: i.retries_left,
              processed_records: i.processed_records,
              next_retry_at_ms: i.next_retry_at_unix_ms,
              commit_in_flight_since_ms: d.commit_in_flight_since_unix_ms,
          }),
          pending_segments: d.pending_segments,
          backlog_segments: d.backlog_segments,
          channel_depth: channel.map(|c| c.depth),
          channel_capacity: CHANNEL_CAPACITY,
          heartbeat_age_secs: d.heartbeat_age_secs,
          blocked_since_ms: d.blocked_since_unix_ms,
          last_requeue: d.last_requeue.as_ref().map(|r| AdminRequeueView {
              ticket: r.ticket,
              requeued: r.requeued,
              already_queued: r.already_queued,
              backoff_cut: r.backoff_cut,
              coalesced: r.coalesced,
              scan_failed: r.scan_error.is_some(),
              at_ms: r.at_unix_ms,
              scan_error: r.scan_error.clone(),
          }),
          exit: d.exit,
          last_panic: d.last_panic.clone(),
      }
  }

  fn sink_view(s: &SinkEvidence, config: Option<&ConfigEvidence>) -> AdminSinkView {
      AdminSinkView {
          sink_type: config.map_or_else(|| "unknown".to_string(), |c| c.sink_type.clone()),
          health: s.health,
          health_since_ms: s.health_since_unix_ms,
          last_success_ms: s.last_success_unix_ms,
          last_failure: s.last_failure_unix_ms.map(|at| AdminSinkFailure {
              at_ms: at,
              kind: s.last_failure_kind,
              message: s.last_error.clone(),
          }),
          consecutive_failures: s.consecutive_failures,
          probe_reason: s.probe_reason.clone(),
      }
  }

  fn reload_last(r: &ReloadEvidence) -> AdminReloadLast {
      AdminReloadLast {
          trigger: r.trigger,
          at_ms: r.at_unix_ms,
          outcome: r.outcome,
          items: r
              .items
              .iter()
              .map(|i| AdminReloadItem { item: i.item.as_str().to_string(), outcome: i.outcome, detail: i.detail.clone() })
              .collect(),
          not_reloaded: r
              .not_reloaded
              .iter()
              .map(|n| NotReloadedView { item: n.item.as_str().to_string(), reason_code: n.reason_code.as_str().to_string() })
              .collect(),
      }
  }
  ```
  lib.rs: `pub mod doc; pub use doc::*;`.

  Bless and review:
  - `WEIR_BLESS=1 cargo test -p weir-status --test golden admin_golden_matches`
  - Open `crates/weir-status/tests/golden/status_v1_admin.json` and check by eye:
    - `"view": "admin"`;
    - `conditions[*].since_ms` (never `since_unix_ms`);
    - `drain.channel_capacity` is 256;
    - `sink.type` is `"http"`;
    - `conditions[0].code` is `"drain_paused"`, with `detail` `"paused by uid 1000"`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass, including 6 `golden` tests.
- [ ] **Step 5: Commit.** `git add crates/weir-status && git commit -m "feat(status): the weir.status/v1 admin document and its golden file"`

---

### Task 1.9: `exposition::evidence_from_metrics` and `active_conditions_from_metrics`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-status/src/exposition.rs`, `crates/weir-status/tests/fixtures/metrics_4_0_1_min.txt` (hand-written, minimal), `crates/weir-status/tests/fixtures/metrics_4_0_1.txt` (captured from a real v4.0.1 daemon; spec §16 "captured 4.0.1 and 4.1 expositions"; the 4.1 capture is Task 1.27's)
- Modify: `crates/weir-status/src/lib.rs` (`pub mod exposition;`)
- Test: `crates/weir-status/src/exposition.rs` (`mod tests`)

**Interfaces:**
- Consumes: Tasks 1.2-1.7.
- Produces:
  ```rust
  pub mod exposition {
      pub fn evidence_from_metrics(body: &str, now_unix_ms: u64) -> Evidence;            // source = Metrics
      pub fn active_conditions_from_metrics(body: &str) -> Option<Vec<(ConditionCode, Severity)>>; // None = family absent (pre-4.1)
  }
  ```
  Contract: every fact absent from the body stays `None`, so a 4.0.1 scrape yields a 4.0.1-shaped (mostly unevaluated) evaluation, never a healthy one. Windowed counters are never inferred from one scrape.

- [ ] **Step 1: Write the failing tests.** First capture a real 4.0.1 exposition. Build it from the `v4.0.1` tag, **not** this branch's base: 3adc5f5 still says `version = "4.0.1"` but already exports the safety floor's families (`weir_wab_dir_lock_held`, `weir_drain_busy_seconds`, `weir_drain_segments_parked`, `weir_config_reloads`, …), which no released 4.0.1 daemon has. From the repository root:
  ```bash
  V401="$(mktemp -d)/weir-v401"
  git worktree add --detach "$V401" v4.0.1
  cargo build --manifest-path "$V401/Cargo.toml" --target-dir "$V401/target" -p weir-server --locked
  D="$(mktemp -d)"; mkdir -p "$D/wab"
  WEIR_SINK_TYPE=noop WEIR_WAB_DIR="$D/wab" WEIR_SOCKET_PATH="$D/weir.sock" \
    WEIR_METRICS_BIND=127.0.0.1 WEIR_METRICS_PORT=19185 "$V401/target/debug/weir-server" &
  PID=$!
  # Wait for the first health poll, so the sink and drain families carry a state.
  for _ in $(seq 60); do
    curl -s http://127.0.0.1:19185/metrics | grep -qF 'weir_sink_health{state="healthy"} 1' && break
    sleep 1
  done
  mkdir -p crates/weir-status/tests/fixtures
  curl -s http://127.0.0.1:19185/metrics > crates/weir-status/tests/fixtures/metrics_4_0_1.txt
  kill -TERM "$PID"; wait "$PID"
  git worktree remove --force "$V401"; rm -rf "$D"
  grep -cE '^weir_(startup_phase|wab_dir_lock_held|drain_busy_seconds)' crates/weir-status/tests/fixtures/metrics_4_0_1.txt
  grep -F 'weir_sink_health{state="healthy"} 1' crates/weir-status/tests/fixtures/metrics_4_0_1.txt
  ```
  Expected: the first `grep -c` prints `0` (no Foundations or safety-floor family), and the second prints the healthy series. Commit the capture as it is; never hand-edit it. Task 1.27 adds the test that parses it, together with the 4.1 capture.

  Then the hand-written minimal fixture, `crates/weir-status/tests/fixtures/metrics_4_0_1_min.txt`. It keeps the unit tests below independent of the capture's timing: the shape of a 4.0.1 scrape with no Foundations families, the noop sink, a draining drain, one recovery failure.
  ```text
  # HELP weir_wab_bytes_on_disk Current bytes used by live WAB shard segments on disk.
  # TYPE weir_wab_bytes_on_disk gauge
  weir_wab_bytes_on_disk 4096
  # HELP weir_sink_health Sink health.
  # TYPE weir_sink_health gauge
  weir_sink_health{state="healthy"} 1
  weir_sink_health{state="degraded"} 0
  weir_sink_health{state="down"} 0
  # HELP weir_sink_info Configured sink.
  # TYPE weir_sink_info gauge
  weir_sink_info{sink_type="noop"} 1
  # HELP weir_drain_state Drain state.
  # TYPE weir_drain_state gauge
  weir_drain_state{state="draining"} 1
  weir_drain_state{state="retrying_transient"} 0
  weir_drain_state{state="blocked_dead_letter_full"} 0
  weir_drain_state{state="stopped"} 0
  # HELP weir_recovery_segments_failed Recovery failures.
  # TYPE weir_recovery_segments_failed counter
  weir_recovery_segments_failed_total 1
  # HELP weir_recovery_quarantine_copy_failed Quarantine copy failures.
  # TYPE weir_recovery_quarantine_copy_failed counter
  weir_recovery_quarantine_copy_failed_total 0
  # HELP weir_dead_letter_bytes_on_disk Dead-letter bytes.
  # TYPE weir_dead_letter_bytes_on_disk gauge
  weir_dead_letter_bytes_on_disk 0
  # HELP weir_quarantine_bytes_on_disk Quarantine bytes.
  # TYPE weir_quarantine_bytes_on_disk gauge
  weir_quarantine_bytes_on_disk 0
  # EOF
  ```
  `exposition.rs`, tests only:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::*;

      const NOW: u64 = 1_800_000_000_000;
      const V401: &str = include_str!("../tests/fixtures/metrics_4_0_1_min.txt");

      /// What a 4.1 daemon serving with two shards exposes (the families this
      /// PR adds, plus the 4.0.1 ones the parser reads).
      const V41: &str = "\
  weir_start_time_seconds 1799999400
  weir_startup_phase{phase=\"initializing\"} 0
  weir_startup_phase{phase=\"serving\"} 1
  weir_wab_shard_up{shard=\"0\"} 1
  weir_wab_shard_up{shard=\"1\"} 0
  weir_worker_up{worker=\"0\"} 1
  weir_worker_up{worker=\"1\"} 1
  weir_wab_handoff_blocked_seconds{shard=\"0\"} 3.5
  weir_wab_handoff_blocked_seconds{shard=\"1\"} 0
  weir_wab_active_segment_age_seconds{shard=\"0\"} 12
  weir_wab_cap_rejecting 1
  weir_wab_bytes_on_disk 950
  weir_config_wab_max_bytes 1000
  weir_config_dead_letter_max_bytes 100
  weir_config_segment_max_age_seconds 0
  weir_config_segment_max_lifetime_seconds 0
  weir_wab_undelivered_segments 3
  weir_wab_undelivered_records 40
  weir_wab_oldest_undelivered_age_seconds 61.5
  weir_drain_state{state=\"draining\"} 0
  weir_drain_state{state=\"stopped\"} 1
  weir_drain_panics_total 10
  weir_drain_busy_seconds{kind=\"sink_commit\"} 0
  weir_drain_busy_seconds{kind=\"segment_read\"} 7.5
  weir_sink_health{state=\"down\"} 1
  weir_sink_health{state=\"healthy\"} 0
  weir_sink_info{sink_type=\"http\"} 1
  weir_condition_active{code=\"wab_cap_rejecting\",severity=\"critical\"} 1
  weir_condition_active{code=\"noop_sink\",severity=\"warning\"} 0
  weir_condition_active{code=\"tls_cert_expiring\",severity=\"warning\"} 1
  # EOF
  ";

      #[test]
      fn a_4_0_1_scrape_yields_only_what_4_0_1_exposed() {
          let e = evidence_from_metrics(V401, NOW);
          assert_eq!(e.source, Source::Metrics);
          assert_eq!(e.phase, None);
          assert!(e.shards.is_none() && e.workers.is_none() && e.wab_cap.is_none() && e.backlog.is_none());
          assert_eq!(e.sink.as_ref().unwrap().health, Some(SinkHealthCode::Healthy));
          assert_eq!(e.drain.as_ref().unwrap().state, DrainStateCode::Draining);
          assert_eq!(e.config.as_ref().unwrap().sink_type, "noop");
          assert_eq!(e.recent.as_ref().unwrap().recovery_segments_failed, Some(1));
          assert_eq!(e.recent.as_ref().unwrap().ack_timeouts_5m, None, "no window from one scrape");
          let ev = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
          assert!(ev.is_active(ConditionCode::NoopSink));
          assert!(ev.is_active(ConditionCode::RecoveryIncomplete));
          assert!(ev.unevaluated.contains(&ConditionCode::Starting));
          assert!(!readiness(&e).ok, "an old daemon's readiness is unknown, not ready");
          assert_eq!(active_conditions_from_metrics(V401), None);
      }

      #[test]
      fn a_4_1_scrape_reconstructs_phase_shards_cap_backlog_and_drain() {
          let e = evidence_from_metrics(V41, NOW);
          assert_eq!(e.phase, Some(Phase::Serving));
          assert_eq!(e.ever_served, Some(true));
          assert_eq!(e.shutting_down, Some(false));
          assert_eq!(e.started_at_unix_ms, Some(1_799_999_400_000));
          let shards = e.shards.as_ref().unwrap();
          assert_eq!(shards[0].state, ShardState::Up);
          assert_eq!(shards[0].handoff_blocked_since_unix_ms, Some(NOW - 3_500));
          assert_eq!(shards[0].active_segment_age_secs, Some(12.0));
          assert_eq!(shards[1].state, ShardState::Offline);
          let cap = e.wab_cap.unwrap();
          assert!(cap.rejecting);
          assert_eq!((cap.bytes_now, cap.max_bytes, cap.low_water_bytes), (950, 1000, 900));
          assert_eq!(e.backlog.as_ref().unwrap().undelivered_records, 40);
          assert_eq!(e.drain.as_ref().unwrap().exit, DrainExitCode::GaveUp, "stopped while serving = gave up");
          assert_eq!(e.recent.as_ref().unwrap().drain_panics_total, Some(10));
          assert_eq!(e.drain.as_ref().unwrap().busy,
                     Some(BusyEvidence { sink_call: false, since_unix_ms: NOW - 7_500 }), "the series > 0 wins");
          let ev = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
          for c in [ConditionCode::WabCapRejecting, ConditionCode::ShardOffline, ConditionCode::DrainStopped,
                    ConditionCode::SinkDown] {
              assert!(ev.is_active(c), "{c:?}");
          }
          assert!(ev.unevaluated.contains(&ConditionCode::WabScanStale));
          // Listener states are not exported and V41 has no `weir_ready 1`, so
          // listener_down is unknown and readiness fails closed on it too.
          assert_eq!(e.listeners, None);
          assert_eq!(readiness(&e).failing, vec![ConditionCode::ListenerDown, ConditionCode::WabCapRejecting]);
      }

      #[test]
      fn the_daemons_own_ready_verdict_vouches_for_its_listeners() {
          let e = evidence_from_metrics("weir_ready 1\nweir_startup_phase{phase=\"serving\"} 1\n", NOW);
          assert_eq!(e.listeners, Some(Vec::new()));
          assert!(!evaluate(&e, &Thresholds::DEFAULT, &NoLatches).unevaluated.contains(&ConditionCode::ListenerDown));
      }

      #[test]
      fn active_conditions_keep_unknown_codes_and_skip_inactive_ones() {
          let active = active_conditions_from_metrics(V41).unwrap();
          assert_eq!(active, vec![
              (ConditionCode::WabCapRejecting, Severity::Critical),
              (ConditionCode::Unknown, Severity::Warning),
          ]);
      }

      #[test]
      fn the_parser_ignores_comments_blank_lines_and_garbage() {
          let e = evidence_from_metrics("# EOF\n\nnot a sample line\nweir_wab_cap_rejecting notanumber\n", NOW);
          assert_eq!(e, Evidence::empty(NOW, Source::Metrics).with_recent_absolutes_only());
      }
  }
  ```
  The last test needs a tiny test-only helper on `Evidence`. Add it in this task, `#[cfg(test)]`, in exposition.rs:
  ```rust
  #[cfg(test)]
  impl Evidence {
      fn with_recent_absolutes_only(mut self) -> Self {
          self.recent = Some(RecentCounters::default());
          self
      }
  }
  ```
  Add `pub mod exposition;` to lib.rs, so the new tests compile and fail.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-status exposition::`. Expected: compile errors `cannot find function `evidence_from_metrics`` / `active_conditions_from_metrics`, and `cannot find type `Evidence`` for the test-only `impl Evidence` (its `use` lines arrive in Step 3).
- [ ] **Step 3: Implement** (`exposition.rs`, above the tests):
  ```rust
  //! Prometheus/OpenMetrics text → `Evidence` (spec §5.1 `exposition`), so
  //! weir-ctl can run the same evaluator against a daemon it can only scrape,
  //! including a pre-Foundations one. Label values in weir's own families
  //! never contain `"` or `,`, so a simple split parser is exact for them; any
  //! line it cannot parse is skipped, never guessed.
  use crate::codes::{ConditionCode, Severity};
  use crate::config::{ConfigValue, PublicConfig};
  use crate::evidence::*;
  use std::collections::BTreeMap;

  struct Sample<'a> {
      name: &'a str,
      labels: Vec<(&'a str, &'a str)>,
      value: f64,
  }

  fn parse_line(line: &str) -> Option<Sample<'_>> {
      let line = line.trim();
      if line.is_empty() || line.starts_with('#') {
          return None;
      }
      let (head, value) = line.rsplit_once(' ')?;
      let value: f64 = value.parse().ok()?;
      let (name, labels) = match head.find('{') {
          Some(i) => (&head[..i], parse_labels(head[i + 1..].strip_suffix('}')?)?),
          None => (head, Vec::new()),
      };
      Some(Sample { name, labels, value })
  }

  fn parse_labels(body: &str) -> Option<Vec<(&str, &str)>> {
      let mut out = Vec::new();
      let mut rest = body;
      while !rest.is_empty() {
          let (key, after) = rest.split_once("=\"")?;
          let end = after.find('"')?;
          out.push((key, &after[..end]));
          rest = &after[end + 1..];
          rest = rest.strip_prefix(',').unwrap_or(rest);
      }
      Some(out)
  }

  struct Scrape<'a>(Vec<Sample<'a>>);

  impl<'a> Scrape<'a> {
      fn parse(body: &'a str) -> Self {
          Scrape(body.lines().filter_map(parse_line).collect())
      }
      fn scalar(&self, name: &str) -> Option<f64> {
          self.0.iter().find(|s| s.name == name && s.labels.is_empty()).map(|s| s.value)
      }
      fn labelled(&self, name: &str, label: &str) -> Vec<(&'a str, f64)> {
          self.0
              .iter()
              .filter(|s| s.name == name)
              .filter_map(|s| s.labels.iter().find(|(k, _)| *k == label).map(|(_, v)| (*v, s.value)))
              .collect()
      }
      fn one_hot(&self, name: &str, label: &str) -> Option<&'a str> {
          self.labelled(name, label).into_iter().find(|(_, v)| *v >= 1.0).map(|(k, _)| k)
      }
  }

  fn ms(secs: f64) -> u64 {
      (secs * 1000.0) as u64
  }

  fn find<T: Copy>(all: &[T], name: fn(T) -> &'static str, s: &str) -> Option<T> {
      all.iter().copied().find(|v| name(*v) == s)
  }

  /// A named fn, not a closure: an elided lifetime in a closure parameter is a
  /// fresh late-bound region, so a closure could not return a `&'a str` tied
  /// to the sample.
  fn label<'a>(x: &Sample<'a>, k: &str) -> Option<&'a str> {
      x.labels.iter().find(|(n, _)| *n == k).map(|(_, v)| *v)
  }

  /// Builds the evidence a scrape supports. Absent families stay `None`.
  pub fn evidence_from_metrics(body: &str, now_unix_ms: u64) -> Evidence {
      let s = Scrape::parse(body);
      let now = now_unix_ms;
      let mut e = Evidence::empty(now, Source::Metrics);
      e.started_at_unix_ms = s.scalar("weir_start_time_seconds").map(ms);
      e.phase = s.one_hot("weir_startup_phase", "phase").and_then(|p| find(Phase::ALL, Phase::as_str, p));
      if let Some(p) = e.phase {
          e.shutting_down = Some(p == Phase::ShuttingDown);
          if p == Phase::Serving {
              e.ever_served = Some(true);
          }
      }
      let serving = e.phase == Some(Phase::Serving);
      // Listener states are not exported. The daemon's own `weir_ready` verdict
      // covers listener_down: ready means no listener is down; anything else
      // stays unknown (and readiness fails closed on it).
      if s.scalar("weir_ready").is_some_and(|v| v >= 1.0) {
          e.listeners = Some(Vec::new());
      }

      let workers: Vec<(u16, bool)> = s
          .labelled("weir_worker_up", "worker")
          .into_iter()
          .filter_map(|(w, v)| Some((w.parse().ok()?, v >= 1.0)))
          .collect();
      if !workers.is_empty() {
          e.workers = Some(workers.iter().map(|&(id, running)| WorkerEvidence {
              id,
              running,
              exited_at_unix_ms: (!running && serving).then_some(now),
          }).collect());
      }
      let shard_up = s.labelled("weir_wab_shard_up", "shard");
      if !shard_up.is_empty() {
          let blocked = s.labelled("weir_wab_handoff_blocked_seconds", "shard");
          let ages = s.labelled("weir_wab_active_segment_age_seconds", "shard");
          let lookup = |set: &[(&str, f64)], id: &str| set.iter().find(|(k, _)| *k == id).map(|(_, v)| *v);
          let wc = workers.len() as u16;
          e.shards = Some(shard_up.iter().filter_map(|&(id_s, up)| {
              let id: u16 = id_s.parse().ok()?;
              let state = if up >= 1.0 {
                  ShardState::Up
              } else if e.phase.is_some_and(Phase::before_serving) {
                  ShardState::Starting
              } else {
                  ShardState::Offline
              };
              Some(ShardEvidence {
                  id,
                  state,
                  reason: None,
                  since_unix_ms: now,
                  respawns: 0,
                  worker_running: wc == 0 || workers.iter().any(|&(w, r)| w == id % wc && r),
                  handoff_blocked_since_unix_ms: lookup(&blocked, id_s).filter(|v| *v > 0.0).map(|v| now.saturating_sub(ms(v))),
                  active_segment_age_secs: lookup(&ages, id_s),
              })
          }).collect());
      }

      if let (Some(rej), Some(bytes), Some(max)) = (
          s.scalar("weir_wab_cap_rejecting"),
          s.scalar("weir_wab_bytes_on_disk"),
          s.scalar("weir_config_wab_max_bytes"),
      ) {
          let max = max as u64;
          e.wab_cap = Some(CapEvidence {
              max_bytes: max,
              low_water_bytes: max / 10 * 9,
              bytes_now: bytes as u64,
              rejecting: rej >= 1.0,
              first_scan_ok_unix_ms: None,
              last_scan_ok_unix_ms: None,
              growing_unbounded: None,
          });
      }

      if let Some(state) =
          s.one_hot("weir_drain_state", "state").and_then(|v| find(DrainStateCode::ALL, DrainStateCode::as_str, v))
      {
          let paused = s.scalar("weir_drain_paused").is_some_and(|v| v >= 1.0);
          let exit = match state {
              DrainStateCode::Stopped if e.phase == Some(Phase::ShuttingDown) => DrainExitCode::Stopped,
              DrainStateCode::Stopped => DrainExitCode::GaveUp,
              _ => DrainExitCode::Running,
          };
          let queue = s.labelled("weir_drain_pending_segments", "queue");
          let q = |name: &str| queue.iter().find(|(k, _)| *k == name).map_or(0, |(_, v)| *v as u64);
          let heartbeat = s.scalar("weir_drain_heartbeat_age_seconds");
          // `weir_drain_busy_seconds{kind}`: at most one kind is > 0 (the
          // safety floor's `publish_drain_busy`); all 0 means between operations.
          let busy = s
              .labelled("weir_drain_busy_seconds", "kind")
              .into_iter()
              .find(|(_, v)| *v > 0.0)
              .map(|(kind, v)| BusyEvidence {
                  sink_call: kind == "sink_commit" || kind == "sink_probe",
                  since_unix_ms: now.saturating_sub(ms(v)),
              });
          e.drain = Some(DrainEvidence {
              state,
              state_since_unix_ms: now,
              pause_requested: paused,
              paused,
              held: false,
              paused_since_unix_ms: None,
              paused_by_uid: None,
              generation: s.scalar("weir_drain_generation").map_or(0, |v| v as u64),
              observed_requeue: 0,
              in_flight: None,
              commit_in_flight_since_unix_ms: s
                  .scalar("weir_drain_commit_in_flight_seconds")
                  .filter(|v| *v > 0.0)
                  .map(|v| now.saturating_sub(ms(v))),
              busy,
              last_progress_unix_ms: heartbeat.map(|v| now.saturating_sub(ms(v))),
              heartbeat_age_secs: heartbeat,
              pending_segments: q("live"),
              backlog_segments: q("backlog"),
              replay_total: 0,
              replay_remaining: s.scalar("weir_drain_replay_remaining_segments").map_or(0, |v| v as u64),
              blocked_since_unix_ms: s
                  .scalar("weir_dead_letter_blocked_duration_seconds")
                  .filter(|v| *v > 0.0)
                  .map(|v| now.saturating_sub(ms(v))),
              last_requeue: None,
              exit,
              last_panic: None,
          });
      }

      if let Some(h) =
          s.one_hot("weir_sink_health", "state").and_then(|v| find(SinkHealthCode::ALL, SinkHealthCode::as_str, v))
      {
          let failures = s.labelled("weir_sink_last_failure_timestamp_seconds", "kind");
          let last = failures.iter().filter(|(_, v)| *v > 0.0).max_by(|a, b| a.1.total_cmp(&b.1));
          e.sink = Some(SinkEvidence {
              health: Some(h),
              health_since_unix_ms: None,
              last_success_unix_ms: s.scalar("weir_sink_last_success_timestamp_seconds").filter(|v| *v > 0.0).map(ms),
              last_failure_unix_ms: last.map(|(_, v)| ms(*v)),
              last_failure_kind: last.and_then(|(k, _)| find(SinkFailureKind::ALL, SinkFailureKind::as_str, k)),
              consecutive_failures: s.scalar("weir_sink_consecutive_failures").map_or(0, |v| v as u64),
              last_error: None,
              probe_reason: None,
          });
      }

      if let Some(segments) = s.scalar("weir_wab_undelivered_segments") {
          e.backlog = Some(BacklogEvidence {
              undelivered_segments: segments as u64,
              undelivered_records: s.scalar("weir_wab_undelivered_records").map_or(0, |v| v as u64),
              oldest_undelivered_age_secs: s.scalar("weir_wab_oldest_undelivered_age_seconds").filter(|v| *v > 0.0),
              dead_letter_bytes: s.scalar("weir_dead_letter_bytes_on_disk").map_or(0, |v| v as u64),
              dead_letter_max_bytes: s.scalar("weir_config_dead_letter_max_bytes").map(|v| v as u64),
              quarantine_bytes: s.scalar("weir_quarantine_bytes_on_disk").map_or(0, |v| v as u64),
              quarantine_files: 0,
              scanned_at_ms: now,
          });
      }

      // Only absolutes: a single scrape has no window, so the windowed fields
      // stay None (unevaluated) rather than reading as "no recent failures".
      e.recent = Some(RecentCounters {
          drain_panics_total: s.scalar("weir_drain_panics_total").map(|v| v as u64),
          recovery_segments_failed: s.scalar("weir_recovery_segments_failed_total").map(|v| v as u64),
          recovery_quarantine_copy_failed: s.scalar("weir_recovery_quarantine_copy_failed_total").map(|v| v as u64),
          ..RecentCounters::default()
      });

      if let Some(sink_type) = s.one_hot("weir_sink_info", "sink_type") {
          let mut view = BTreeMap::new();
          for (key, metric) in [
              ("wab_max_bytes", "weir_config_wab_max_bytes"),
              ("dead_letter_max_bytes", "weir_config_dead_letter_max_bytes"),
              ("wab_segment_max_bytes", "weir_config_wab_segment_max_bytes"),
              ("wab_segment_max_age_secs", "weir_config_segment_max_age_seconds"),
              ("wab_segment_max_lifetime_secs", "weir_config_segment_max_lifetime_seconds"),
              ("sink_max_retries", "weir_config_sink_max_retries"),
              ("shutdown_timeout_secs", "weir_config_shutdown_timeout_seconds"),
              ("drain_shutdown_budget_secs", "weir_config_drain_shutdown_budget_seconds"),
          ] {
              if let Some(v) = s.scalar(metric) {
                  view.insert(key.to_string(), ConfigValue::U64(v as u64));
              }
          }
          if let Some(n) = e.shards.as_ref().map(Vec::len) {
              view.insert("shard_count".to_string(), ConfigValue::U64(n as u64));
          }
          if !workers.is_empty() {
              view.insert("worker_count".to_string(), ConfigValue::U64(workers.len() as u64));
          }
          e.config = Some(ConfigEvidence {
              sink_type: sink_type.to_string(),
              commit_timeout_secs: None,
              public: PublicConfig::from_view(&view),
              view,
          });
      }
      e
  }

  /// The daemon's own verdicts from `weir_condition_active` (spec §14 step 3).
  /// `None` when the family is absent (a pre-Foundations daemon). Unknown codes
  /// are kept as `ConditionCode::Unknown`.
  pub fn active_conditions_from_metrics(body: &str) -> Option<Vec<(ConditionCode, Severity)>> {
      let s = Scrape::parse(body);
      let series: Vec<&Sample<'_>> = s.0.iter().filter(|x| x.name == "weir_condition_active").collect();
      if series.is_empty() {
          return None;
      }
      Some(
          series
              .iter()
              .filter(|x| x.value >= 1.0)
              .filter_map(|x| {
                  let code = ConditionCode::parse_lossy(label(x, "code")?);
                  let severity = match label(x, "severity")? {
                      "info" => Severity::Info,
                      "warning" => Severity::Warning,
                      "critical" => Severity::Critical,
                      _ => return None,
                  };
                  Some((code, severity))
              })
              .collect(),
      )
  }
  ```
  lib.rs: add `pub use exposition::{active_conditions_from_metrics, evidence_from_metrics};`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-status`. Expected: all pass (5 new). In `a_4_1_scrape_reconstructs...`, the V41 fixture has worker 1 running, so shard 1 offline means a partial outage and `shard_offline` fires. `no_live_shard` stays inactive because shard 0 is up.
- [ ] **Step 5: Commit.** `git add crates/weir-status && git commit -m "feat(status): evaluate a daemon from its metrics exposition"`

---
### Task 1.10: `wab::cap::evaluate`: one hysteresis for the push path and the poll task

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/wab/cap.rs`
- Modify: `crates/weir-server/src/wab/mod.rs:1-6` (`pub(crate) mod cap;`)
- Modify: `crates/weir-server/src/socket/connection.rs:858-887`. `over_wab_cap` now delegates; its doc comment moves to `cap.rs`.
- Test: `crates/weir-server/src/wab/cap.rs` (`mod tests`). The end-to-end cap tests in connection.rs (:1822-2000) stay where they are, unchanged. They pin the wire behaviour (InternalError, connection kept), which `cap.rs` cannot see. The *pure* hysteresis assertions are the ones that move here, as a table.

**Interfaces:**
- Consumes: nothing new.
- Produces:
  ```rust
  pub(crate) fn low_water(cap: u64) -> u64;                                   // cap / 10 * 9
  pub(crate) fn evaluate(cap: u64, bytes: u64, rejecting: &AtomicBool) -> bool; // updates the flag, returns it
  ```

- [ ] **Step 1: Write the failing test.** `crates/weir-server/src/wab/cap.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use std::sync::atomic::{AtomicBool, Ordering};

      /// (cap, flag before, bytes) -> (returned, flag after). The push path and
      /// the poll task both call this, so one table pins both.
      #[test]
      fn hysteresis_table() {
          let cases: &[(u64, bool, u64, bool)] = &[
              (0, false, u64::MAX, false),        // cap 0 disables the check
              (1_000, false, 999, false),
              (1_000, false, 1_000, true),        // reaching the cap starts rejecting
              (1_000, true, 950, true),           // between low water and cap: still rejecting
              (1_000, true, 900, true),           // low water itself still rejects
              (1_000, true, 899, false),          // below low water: accepting again
              (1_000, false, 950, false),         // below the cap from accepting: stays accepting
              (u64::MAX, false, u64::MAX, true),  // no overflow in the low-water arithmetic
          ];
          for &(cap, before, bytes, want) in cases {
              let flag = AtomicBool::new(before);
              assert_eq!(evaluate(cap, bytes, &flag), want, "cap={cap} before={before} bytes={bytes}");
              assert_eq!(flag.load(Ordering::Relaxed), want, "flag after, cap={cap} bytes={bytes}");
          }
      }

      #[test]
      fn low_water_divides_before_multiplying() {
          assert_eq!(low_water(1_000), 900);
          assert_eq!(low_water(u64::MAX), u64::MAX / 10 * 9);
          assert_eq!(low_water(9), 0);
      }
  }
  ```
  Add `pub(crate) mod cap;` to wab/mod.rs.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server wab::cap -- --test-threads=1`. Expected: `cannot find function `evaluate`` / `low_water`.
- [ ] **Step 3: Implement.** `cap.rs`, above the tests:
  ```rust
  //! The `wab_max_bytes` hysteresis (spec §5.5). The push path
  //! (`socket::connection::over_wab_cap`) and the 5 s poll task call the same
  //! function, so the flag clears without traffic, and the two writers compute
  //! the same idempotent answer from the same inputs.
  //!
  //! Once rejecting, keep rejecting until bytes fall below `cap * 9 / 10`, so
  //! ingest does not flap at the boundary. `cap == 0` disables the check. The
  //! flag is shared daemon-wide: one observer of recovery releases every
  //! connection, and one observer of the cap holds them all. A failed WAB scan
  //! must NOT call this (the last value stands: fail-closed).
  use std::sync::atomic::{AtomicBool, Ordering};

  /// The level below which a rejecting cap releases. Divides before
  /// multiplying: `cap * 9` overflows above `u64::MAX / 9`, and the rounding
  /// loss is at most 9 bytes.
  pub(crate) fn low_water(cap: u64) -> u64 {
      cap / 10 * 9
  }

  /// Applies the hysteresis to `bytes`, stores the result in `rejecting` and
  /// returns it.
  pub(crate) fn evaluate(cap: u64, bytes: u64, rejecting: &AtomicBool) -> bool {
      if cap == 0 {
          return false;
      }
      let was = rejecting.load(Ordering::Relaxed);
      let now = if was { bytes >= low_water(cap) } else { bytes >= cap };
      if now != was {
          rejecting.store(now, Ordering::Relaxed);
      }
      now
  }
  ```
  `socket/connection.rs`: replace the body of `over_wab_cap` (and shrink its doc comment to one line pointing at `crate::wab::cap`):
  ```rust
  /// Whether the WAB cap is currently rejecting pushes; see [`crate::wab::cap`].
  fn over_wab_cap(config: &ConnectionConfig) -> bool {
      crate::wab::cap::evaluate(
          config.wab_max_bytes,
          config.wab_bytes_now.load(Ordering::Relaxed),
          &config.wab_cap_rejecting,
      )
  }
  ```
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server wab::cap -- --test-threads=1`. Expected: 2 passed.
  - `cargo test -p weir-server --bin weir-server socket::connection::tests:: -- --test-threads=1`. Expected: every connection test passes unchanged, including the six existing cap tests (`push_over_the_wab_cap_nacks_internal_error_and_keeps_the_connection`, `queue_saturation_does_not_move_the_wab_cap_counter`, `push_under_the_wab_cap_is_accepted`, `wab_cap_disabled_never_rejects`, `wab_cap_hysteresis_holds_until_the_low_water_mark`, `wab_cap_rejects_every_durability_tier`). libtest ORs its filters, so a second `wab_cap` filter would widen the run, not narrow it.
- [ ] **Step 5: Commit.** `git add crates/weir-server/src/wab/cap.rs && git commit -am "refactor(wab): one cap hysteresis shared by the push path and the poll task"`

---

### Task 1.11: Server dependencies, the `status` module, `LifecycleCell` and `ListenerCells`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/Cargo.toml`:
  - `[features] tls`: drop `"dep:arc-swap"`;
  - `arc-swap`: no longer `optional`;
  - add `weir-status`;
  - move `serde_json` from `[dev-dependencies]` to `[dependencies]`.
- Modify: `crates/weir-server/src/main.rs:8` (add `mod status;` after `mod sink;`)
- Create: `crates/weir-server/src/status/mod.rs`, `crates/weir-server/src/status/board.rs`
- Test: `crates/weir-server/src/status/board.rs` (`mod tests`)

**Interfaces:**
- Consumes: `weir_status::{Phase, ListenerKind, ListenerState, ListenerEvidence}`.
- Produces:
  ```rust
  pub(crate) fn status::unix_ms_now() -> u64;
  pub(crate) struct LifecycleCell { /* atomics */ }
  impl LifecycleCell {
      pub(crate) fn new(now_ms: u64) -> Self;
      pub(crate) fn enter(&self, phase: Phase, now_ms: u64) -> bool;  // false once ShuttingDown (nothing leaves it)
      pub(crate) fn phase(&self) -> Phase;  pub(crate) fn phase_since_ms(&self) -> u64;
      pub(crate) fn begin_shutdown(&self, now_ms: u64) -> bool;     // true for the first caller only
      pub(crate) fn mark_forced(&self);                             // PR 4
      pub(crate) fn is_shutting_down(&self) -> bool;
      pub(crate) fn shutting_down_flag(&self) -> Arc<AtomicBool>;   // shared with the ShardRegistry; the flushers use their own flag, which the Lifecycle sets at T0 (PR 3/4)
      pub(crate) fn shutting_down_since_ms(&self) -> Option<u64>;
      pub(crate) fn forced(&self) -> bool;  pub(crate) fn ever_served(&self) -> bool;
      pub(crate) fn set_drain_deadline(&self, deadline_unix_ms: u64, budget_secs: u64); // PR 4
      pub(crate) fn drain_deadline_ms(&self) -> Option<u64>;  pub(crate) fn drain_budget_secs(&self) -> Option<u64>;
  }
  pub(crate) struct ListenerCell;   impl ListenerCell { pub(crate) fn new(ListenerState) -> Self; pub(crate) fn set(&self, ListenerState); pub(crate) fn get(&self) -> ListenerState; }
  pub(crate) struct ListenerCells { pub(crate) unix: ListenerCell, pub(crate) tcp: ListenerCell }
  impl ListenerCells { pub(crate) fn new(unix_configured: bool, tcp_configured: bool) -> Self;
                       pub(crate) fn evidence(&self) -> Vec<ListenerEvidence>; }  // skips NotConfigured
  ```

- [ ] **Step 1: Write the failing test.** `crates/weir-server/src/status/board.rs`, test module only:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn entering_serving_marks_ever_served_and_stamps_the_phase() {
          let life = LifecycleCell::new(1_000);
          assert_eq!(life.phase(), Phase::Initializing);
          assert!(!life.ever_served());
          assert!(life.enter(Phase::Recovering, 2_000));
          assert_eq!((life.phase(), life.phase_since_ms()), (Phase::Recovering, 2_000));
          assert!(life.enter(Phase::Serving, 3_000));
          assert!(life.ever_served());
      }

      #[test]
      fn nothing_leaves_shutting_down() {
          let life = LifecycleCell::new(0);
          life.enter(Phase::Binding, 1);
          assert!(life.begin_shutdown(5));
          assert!(!life.enter(Phase::Serving, 6), "a late startup step must not resurrect serving");
          assert_eq!(life.phase(), Phase::ShuttingDown);
          assert!(!life.ever_served());
          assert_eq!(life.shutting_down_since_ms(), Some(5));
      }

      #[test]
      fn begin_shutdown_is_first_caller_wins_and_shares_its_flag() {
          let life = LifecycleCell::new(0);
          let flag = life.shutting_down_flag();
          assert!(!flag.load(Ordering::Acquire));
          assert!(life.begin_shutdown(10));
          assert!(!life.begin_shutdown(20));
          assert!(flag.load(Ordering::Acquire), "the registry sees the same flag");
          assert_eq!(life.shutting_down_since_ms(), Some(10));
          assert_eq!(life.drain_deadline_ms(), None);
          life.set_drain_deadline(30_000, 10);
          assert_eq!((life.drain_deadline_ms(), life.drain_budget_secs()), (Some(30_000), Some(10)));
      }

      #[test]
      fn listener_evidence_skips_unconfigured_listeners() {
          let l = ListenerCells::new(true, false);
          assert_eq!(l.evidence(), vec![ListenerEvidence { kind: ListenerKind::Unix, state: ListenerState::Pending }]);
          l.unix.set(ListenerState::Bound);
          l.tcp.set(ListenerState::Bound);
          assert_eq!(l.evidence().len(), 2);
          assert_eq!(l.unix.get(), ListenerState::Bound);
      }
  }
  ```
  `crates/weir-server/src/status/mod.rs`:
  ```rust
  //! Live status (spec §5.2-5.3, §5.11, §5.14): the wait-free board the
  //! daemon's threads publish into, the shard registry, the poll tasks and
  //! the renderers. Writers store atomics or swap `ArcSwap` cells; readers
  //! only load. No blocking lock type appears anywhere under this directory
  //! (spec §9.2 rule 5); `no_blocking_locks_under_status` enforces it.
  pub(crate) mod board;

  /// Milliseconds since the Unix epoch. 0 on a clock before 1970.
  pub(crate) fn unix_ms_now() -> u64 {
      std::time::SystemTime::now()
          .duration_since(std::time::UNIX_EPOCH)
          .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
          .unwrap_or(0)
  }
  ```
  `main.rs`: add `mod status;` after `mod sink;`.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::board -- --test-threads=1`. Expected: `cannot find type `LifecycleCell`` / `ListenerCells` (and `unresolved import weir_status` until Cargo.toml is edited).
- [ ] **Step 3: Implement.**
  `Cargo.toml` (`crates/weir-server`):
  ```toml
  tls = ["dep:tokio-rustls", "dep:rustls-pki-types", "dep:rustls"]
  ```
  ```toml
  # Shared status model and evaluator (spec §5.1): the daemon and weir-ctl
  # must compute the same Conditions from the same facts.
  weir-status = { workspace = true }
  # Wait-free cells on the status board (ArcSwap/ArcSwapOption, spec §9.2) and
  # the hot-reloadable TLS ServerConfig. Unconditional since Foundations: the
  # board exists in every build. Already in the lock (it was optional behind `tls`).
  arc-swap = "1"
  # Rendering the status document (PR 5's /status, PR 6's admin socket).
  # Already in the lock as a dev-dependency.
  serde_json = "1"
  ```
  Delete the old `arc-swap = { version = "1", optional = true }` line and the `serde_json = "1"` line in `[dev-dependencies]`. Keep its comment, moved to the new entry: "...the seed-regression replay test also uses it".

  `board.rs`, above the tests:
  ```rust
  //! The status board's cells (spec §5.2). Every cell has one writer; readers
  //! only load.
  use std::sync::Arc;
  use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
  use weir_status::{ListenerEvidence, ListenerKind, ListenerState, Phase};

  const NONE_MS: u64 = 0;
  const NO_BUDGET: u64 = u64::MAX;

  fn opt_ms(v: u64) -> Option<u64> {
      (v != NONE_MS).then_some(v)
  }

  /// Startup phase and shutdown state. Phase stores use Release, so a reader
  /// that sees `Serving` also sees the listener stores made before it (spec §6
  /// step 12).
  pub(crate) struct LifecycleCell {
      phase: AtomicU8,
      phase_since_ms: AtomicU64,
      shutting_down: Arc<AtomicBool>,
      shutting_down_since_ms: AtomicU64,
      forced: AtomicBool,
      ever_served: AtomicBool,
      drain_deadline_ms: AtomicU64,
      drain_budget_secs: AtomicU64,
  }

  impl LifecycleCell {
      pub(crate) fn new(now_ms: u64) -> Self {
          LifecycleCell {
              phase: AtomicU8::new(Phase::Initializing.as_u8()),
              phase_since_ms: AtomicU64::new(now_ms),
              shutting_down: Arc::new(AtomicBool::new(false)),
              shutting_down_since_ms: AtomicU64::new(NONE_MS),
              forced: AtomicBool::new(false),
              ever_served: AtomicBool::new(false),
              drain_deadline_ms: AtomicU64::new(NONE_MS),
              drain_budget_secs: AtomicU64::new(NO_BUDGET),
          }
      }

      /// Moves to `phase`. Refused (false) once `ShuttingDown`: a startup step
      /// that finishes after SIGTERM must not report the daemon as serving.
      pub(crate) fn enter(&self, phase: Phase, now_ms: u64) -> bool {
          let target = phase.as_u8();
          let shutting = Phase::ShuttingDown.as_u8();
          let moved = self
              .phase
              .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                  (cur != shutting || target == shutting).then_some(target)
              })
              .is_ok();
          if moved {
              self.phase_since_ms.store(now_ms, Ordering::Relaxed);
              if phase == Phase::Serving {
                  self.ever_served.store(true, Ordering::Release);
              }
          }
          moved
      }

      pub(crate) fn phase(&self) -> Phase {
          Phase::from_u8(self.phase.load(Ordering::Acquire)).unwrap_or(Phase::Initializing)
      }

      pub(crate) fn phase_since_ms(&self) -> u64 {
          self.phase_since_ms.load(Ordering::Relaxed)
      }

      /// T0 (spec §7): sets the shared flag and the phase. True only for the
      /// first caller.
      pub(crate) fn begin_shutdown(&self, now_ms: u64) -> bool {
          if self.shutting_down.swap(true, Ordering::AcqRel) {
              return false;
          }
          self.shutting_down_since_ms.store(now_ms, Ordering::Relaxed);
          self.enter(Phase::ShuttingDown, now_ms);
          true
      }

      #[cfg_attr(not(test), allow(dead_code))] // PR 4 (second signal) writes this
      pub(crate) fn mark_forced(&self) {
          self.forced.store(true, Ordering::Relaxed);
      }

      pub(crate) fn is_shutting_down(&self) -> bool {
          self.shutting_down.load(Ordering::Acquire)
      }

      pub(crate) fn shutting_down_flag(&self) -> Arc<AtomicBool> {
          Arc::clone(&self.shutting_down)
      }

      pub(crate) fn shutting_down_since_ms(&self) -> Option<u64> {
          opt_ms(self.shutting_down_since_ms.load(Ordering::Relaxed))
      }

      pub(crate) fn forced(&self) -> bool {
          self.forced.load(Ordering::Relaxed)
      }

      pub(crate) fn ever_served(&self) -> bool {
          self.ever_served.load(Ordering::Acquire)
      }

      #[cfg_attr(not(test), allow(dead_code))] // PR 4 (T2 request_stop) writes this
      pub(crate) fn set_drain_deadline(&self, deadline_unix_ms: u64, budget_secs: u64) {
          self.drain_budget_secs.store(budget_secs, Ordering::Relaxed);
          self.drain_deadline_ms.store(deadline_unix_ms, Ordering::Relaxed);
      }

      pub(crate) fn drain_deadline_ms(&self) -> Option<u64> {
          opt_ms(self.drain_deadline_ms.load(Ordering::Relaxed))
      }

      pub(crate) fn drain_budget_secs(&self) -> Option<u64> {
          let v = self.drain_budget_secs.load(Ordering::Relaxed);
          (v != NO_BUDGET).then_some(v)
      }
  }

  /// One listener's state.
  pub(crate) struct ListenerCell(AtomicU8);

  impl ListenerCell {
      pub(crate) fn new(state: ListenerState) -> Self {
          ListenerCell(AtomicU8::new(state.as_u8()))
      }
      pub(crate) fn set(&self, state: ListenerState) {
          self.0.store(state.as_u8(), Ordering::Release);
      }
      pub(crate) fn get(&self) -> ListenerState {
          ListenerState::from_u8(self.0.load(Ordering::Acquire)).unwrap_or(ListenerState::Failed)
      }
  }

  pub(crate) struct ListenerCells {
      pub(crate) unix: ListenerCell,
      pub(crate) tcp: ListenerCell,
  }

  impl ListenerCells {
      pub(crate) fn new(unix_configured: bool, tcp_configured: bool) -> Self {
          let initial = |configured| if configured { ListenerState::Pending } else { ListenerState::NotConfigured };
          ListenerCells { unix: ListenerCell::new(initial(unix_configured)), tcp: ListenerCell::new(initial(tcp_configured)) }
      }

      pub(crate) fn evidence(&self) -> Vec<ListenerEvidence> {
          [(ListenerKind::Unix, &self.unix), (ListenerKind::Tcp, &self.tcp)]
              .into_iter()
              .map(|(kind, cell)| ListenerEvidence { kind, state: cell.get() })
              .filter(|l| l.state != ListenerState::NotConfigured)
              .collect()
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server status::board -- --test-threads=1`. Expected: 4 passed.
  - `cargo build -p weir-server --no-default-features` and `cargo build -p weir-server --features tls`. Both must compile: `socket/tls.rs` still finds `arc_swap`.
- [ ] **Step 5: Commit.** `git add crates/weir-server Cargo.lock && git commit -m "feat(status): lifecycle and listener cells; arc-swap and weir-status become dependencies"`

---

### Task 1.12: `ShardRegistry`, `ShardCell`, `WorkerCell`, `WorkerGuard`, `ShardSlot`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/status/shards.rs`
- Modify: `crates/weir-server/src/status/mod.rs` (add `pub(crate) mod shards;` and `pub(crate) use shards::{ShardRegistry, ShardSlot, SupervisorExit, WorkerGuard};`)
- Test: `crates/weir-server/src/status/shards.rs` (`mod tests`)

**Interfaces:**
- Consumes: `weir_status::{ShardState, OfflineReason, ShardEvidence, WorkerEvidence}`, `status::unix_ms_now`.
- Produces:
  ```rust
  pub(crate) enum SupervisorExit { InputClosed, CounterScanFailed, RespawnExhausted }  // Copy, Eq
  pub(crate) struct ShardRegistry { /* Box<[ShardCell]>, Box<[WorkerCell]>, Arc<AtomicBool> */ }
  impl ShardRegistry {
      pub(crate) fn new(shard_count: usize, worker_count: usize, shutting_down: Arc<AtomicBool>) -> Self;
      pub(crate) fn detached() -> Arc<Self>;                          // 1 shard, 1 worker, own flag (DST/tests)
      pub(crate) fn shard_count(&self) -> usize;  pub(crate) fn worker_count(&self) -> usize;
      pub(crate) fn state(&self, shard: usize) -> ShardState;
      pub(crate) fn reason(&self, shard: usize) -> Option<OfflineReason>;
      pub(crate) fn respawns(&self, shard: usize) -> u32;
      pub(crate) fn mark_up(&self, shard: usize);                     // the flusher, after its counter scan
      pub(crate) fn mark_respawning(&self, shard: usize);             // the supervisor, per caught panic
      pub(crate) fn mark_exit(&self, shard: usize, exit: SupervisorExit); // the supervisor, terminal
      pub(crate) fn set_handoff_blocked_since_ms(&self, shard: usize, since_unix_ms: u64); // 0 clears (PR 3)
      pub(crate) fn handoff_blocked_since_ms(&self, shard: usize) -> Option<u64>;
      pub(crate) fn worker_running(&self, worker: usize) -> bool;
      pub(crate) fn ingest_capable(&self, shard: usize) -> bool;
      pub(crate) fn ingest_capable_count(&self) -> usize;
      pub(crate) fn pick_live(&self, n: u64) -> Option<u32>;          // (n mod k)-th ingest-capable shard (PR 3)
      pub(crate) fn all_settled(&self) -> bool;                       // no shard Starting (PR 3)
      pub(crate) fn shard_evidence(&self) -> Vec<ShardEvidence>;      // active_segment_age_secs: None
      pub(crate) fn worker_evidence(&self) -> Vec<WorkerEvidence>;
  }
  pub(crate) struct WorkerGuard;   impl WorkerGuard { pub(crate) fn enter(registry: Arc<ShardRegistry>, worker: usize) -> Self; }  // Drop: running=false, exited_at=now
  #[derive(Clone)] pub(crate) struct ShardSlot;
  impl ShardSlot { pub(crate) fn new(registry: Arc<ShardRegistry>, shard: usize) -> Self; pub(crate) fn detached() -> Self;
                   pub(crate) fn mark_up(&self); pub(crate) fn mark_respawning(&self);
                   pub(crate) fn mark_exit(&self, exit: SupervisorExit); pub(crate) fn registry(&self) -> &Arc<ShardRegistry>;
                   pub(crate) fn shard(&self) -> usize; }
  ```

- [ ] **Step 1: Write the failing tests** (`shards.rs`, test module only):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      fn registry(shards: usize, workers: usize) -> Arc<ShardRegistry> {
          Arc::new(ShardRegistry::new(shards, workers, Arc::new(AtomicBool::new(false))))
      }
      fn all_up(r: &Arc<ShardRegistry>) -> Vec<WorkerGuard> {
          (0..r.shard_count()).for_each(|s| r.mark_up(s));
          (0..r.worker_count()).map(|w| WorkerGuard::enter(Arc::clone(r), w)).collect()
      }

      #[test]
      fn everything_starts_starting_and_unsettled_with_no_live_shard() {
          let r = registry(2, 1);
          assert_eq!(r.state(0), ShardState::Starting);
          assert!(!r.all_settled());
          assert_eq!(r.ingest_capable_count(), 0);
          assert_eq!(r.pick_live(0), None);
      }

      #[test]
      fn pick_live_round_robins_over_ingest_capable_shards_only() {
          let r = registry(4, 2);
          let _workers = all_up(&r);
          assert!(r.all_settled());
          let picks: Vec<u32> = (0..8).map(|n| r.pick_live(n).unwrap()).collect();
          assert_eq!(picks, vec![0, 1, 2, 3, 0, 1, 2, 3]);
          r.mark_exit(1, SupervisorExit::RespawnExhausted);
          let picks: Vec<u32> = (0..6).map(|n| r.pick_live(n).unwrap()).collect();
          assert_eq!(picks, vec![0, 2, 3, 0, 2, 3]);
          r.mark_respawning(2);
          assert!(r.ingest_capable(2), "Respawning counts: the batch queue survives a respawn");
          assert_eq!(r.respawns(2), 1);
      }

      #[test]
      fn a_dead_worker_takes_its_shards_out_of_routing() {
          let r = registry(4, 2);
          let mut workers = all_up(&r);
          drop(workers.remove(1)); // worker 1 owns shards 1 and 3 (shard % worker_count)
          assert!(!r.worker_running(1));
          let picks: Vec<u32> = (0..4).map(|n| r.pick_live(n).unwrap()).collect();
          assert_eq!(picks, vec![0, 2, 0, 2]);
          let w = r.worker_evidence();
          assert!(w[1].exited_at_unix_ms.is_some());
          assert!(w[0].running && w[0].exited_at_unix_ms.is_none());
      }

      #[test]
      fn supervisor_exits_map_to_terminal_states() {
          let flag = Arc::new(AtomicBool::new(false));
          let r = ShardRegistry::new(3, 1, Arc::clone(&flag));
          r.mark_exit(0, SupervisorExit::CounterScanFailed);
          r.mark_exit(1, SupervisorExit::InputClosed);
          assert_eq!((r.state(0), r.reason(0)), (ShardState::Offline, Some(OfflineReason::CounterScanFailed)));
          assert_eq!((r.state(1), r.reason(1)), (ShardState::Offline, Some(OfflineReason::FlusherExited)));
          flag.store(true, Ordering::Release);
          r.mark_exit(2, SupervisorExit::InputClosed);
          assert_eq!((r.state(2), r.reason(2)), (ShardState::Stopped, None), "input closed during shutdown is a clean stop");
          assert!(r.all_settled());
      }

      #[test]
      fn the_handoff_marker_keeps_its_first_timestamp_and_clears_on_zero() {
          let r = registry(1, 1);
          assert_eq!(r.handoff_blocked_since_ms(0), None);
          r.set_handoff_blocked_since_ms(0, 100);
          r.set_handoff_blocked_since_ms(0, 200);
          assert_eq!(r.handoff_blocked_since_ms(0), Some(100), "the first timeout stamps it");
          r.set_handoff_blocked_since_ms(0, 0);
          assert_eq!(r.handoff_blocked_since_ms(0), None);
          assert_eq!(r.shard_evidence()[0].handoff_blocked_since_unix_ms, None);
      }

      #[test]
      fn a_detached_slot_is_independent() {
          let a = ShardSlot::detached();
          let b = ShardSlot::detached();
          a.mark_up();
          assert_eq!(a.registry().state(0), ShardState::Up);
          assert_eq!(b.registry().state(0), ShardState::Starting);
      }

      #[test]
      fn a_panicking_worker_still_marks_itself_exited() {
          let r = registry(1, 1);
          let r2 = Arc::clone(&r);
          let joined = std::thread::spawn(move || {
              let _g = WorkerGuard::enter(r2, 0);
              panic!("worker body panicked");
          })
          .join();
          assert!(joined.is_err());
          assert!(!r.worker_running(0));
          assert!(r.worker_evidence()[0].exited_at_unix_ms.is_some());
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::shards -- --test-threads=1`. Expected: `cannot find type `ShardRegistry`` etc.
- [ ] **Step 3: Implement** (`shards.rs`, above the tests):
  ```rust
  //! Per-shard and per-worker state (spec §5.3). The flusher supervisor is the
  //! single writer of a shard's terminal state; the flusher writes only `Up`;
  //! a worker's drop guard writes its own running flag. Readers (routing,
  //! evidence) only load: O(shard_count <= 256) loads per `pick_live`.
  use super::unix_ms_now;
  use std::sync::Arc;
  use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
  use weir_status::{OfflineReason, ShardEvidence, ShardState, WorkerEvidence};

  const NO_REASON: u8 = u8::MAX;

  /// How a flusher supervisor ended (also SP-G's `SupervisorExit` seam).
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum SupervisorExit {
      InputClosed,
      CounterScanFailed,
      RespawnExhausted,
  }

  struct ShardCell {
      state: AtomicU8,
      reason: AtomicU8,
      since_ms: AtomicU64,
      respawns: AtomicU32,
      handoff_blocked_since_ms: AtomicU64,
  }

  impl ShardCell {
      fn new(now_ms: u64) -> Self {
          ShardCell {
              state: AtomicU8::new(ShardState::Starting.as_u8()),
              reason: AtomicU8::new(NO_REASON),
              since_ms: AtomicU64::new(now_ms),
              respawns: AtomicU32::new(0),
              handoff_blocked_since_ms: AtomicU64::new(0),
          }
      }
      fn set(&self, state: ShardState, reason: Option<OfflineReason>) {
          self.reason.store(reason.map_or(NO_REASON, OfflineReason::as_u8), Ordering::Relaxed);
          self.since_ms.store(unix_ms_now(), Ordering::Relaxed);
          self.state.store(state.as_u8(), Ordering::Release);
      }
      fn state(&self) -> ShardState {
          ShardState::from_u8(self.state.load(Ordering::Acquire)).unwrap_or(ShardState::Offline)
      }
  }

  struct WorkerCell {
      running: AtomicBool,
      exited_at_ms: AtomicU64,
  }

  pub(crate) struct ShardRegistry {
      shards: Box<[ShardCell]>,
      workers: Box<[WorkerCell]>,
      shutting_down: Arc<AtomicBool>,
  }

  impl ShardRegistry {
      pub(crate) fn new(shard_count: usize, worker_count: usize, shutting_down: Arc<AtomicBool>) -> Self {
          let now = unix_ms_now();
          ShardRegistry {
              shards: (0..shard_count).map(|_| ShardCell::new(now)).collect(),
              workers: (0..worker_count)
                  .map(|_| WorkerCell { running: AtomicBool::new(false), exited_at_ms: AtomicU64::new(0) })
                  .collect(),
              shutting_down,
          }
      }

      /// A one-shard, one-worker registry nothing else observes (spec §5.3:
      /// "DST passes a detached registry").
      #[cfg_attr(not(any(test, feature = "dst")), allow(dead_code))] // DST and tests only
      pub(crate) fn detached() -> Arc<Self> {
          Arc::new(Self::new(1, 1, Arc::new(AtomicBool::new(false))))
      }

      #[cfg_attr(not(test), allow(dead_code))] // PR 3/4 read it
      pub(crate) fn shard_count(&self) -> usize {
          self.shards.len()
      }
      #[cfg_attr(not(test), allow(dead_code))] // PR 3/4 read it
      pub(crate) fn worker_count(&self) -> usize {
          self.workers.len()
      }
      pub(crate) fn state(&self, shard: usize) -> ShardState {
          self.shards[shard].state()
      }
      pub(crate) fn reason(&self, shard: usize) -> Option<OfflineReason> {
          OfflineReason::from_u8(self.shards[shard].reason.load(Ordering::Relaxed))
      }
      pub(crate) fn respawns(&self, shard: usize) -> u32 {
          self.shards[shard].respawns.load(Ordering::Relaxed)
      }

      pub(crate) fn mark_up(&self, shard: usize) {
          self.shards[shard].set(ShardState::Up, None);
      }

      pub(crate) fn mark_respawning(&self, shard: usize) {
          self.shards[shard].respawns.fetch_add(1, Ordering::Relaxed);
          self.shards[shard].set(ShardState::Respawning, None);
      }

      /// Spec §5.3: InputClosed is `Stopped` while shutting down, else
      /// `Offline(FlusherExited)`; the other two are `Offline(<reason>)`.
      pub(crate) fn mark_exit(&self, shard: usize, exit: SupervisorExit) {
          let (state, reason) = match exit {
              SupervisorExit::InputClosed if self.shutting_down.load(Ordering::Acquire) => (ShardState::Stopped, None),
              SupervisorExit::InputClosed => (ShardState::Offline, Some(OfflineReason::FlusherExited)),
              SupervisorExit::CounterScanFailed => (ShardState::Offline, Some(OfflineReason::CounterScanFailed)),
              SupervisorExit::RespawnExhausted => (ShardState::Offline, Some(OfflineReason::RespawnExhausted)),
          };
          self.shards[shard].set(state, reason);
      }

      /// The hand-off-blocked marker (spec §5.3). The first timeout of an
      /// episode stamps it; later stamps keep the first; 0 clears it.
      #[cfg_attr(not(test), allow(dead_code))] // PR 3's hand_off writes this
      pub(crate) fn set_handoff_blocked_since_ms(&self, shard: usize, since_unix_ms: u64) {
          let cell = &self.shards[shard].handoff_blocked_since_ms;
          if since_unix_ms == 0 {
              cell.store(0, Ordering::Relaxed);
          } else {
              let _ = cell.compare_exchange(0, since_unix_ms, Ordering::Relaxed, Ordering::Relaxed);
          }
      }

      pub(crate) fn handoff_blocked_since_ms(&self, shard: usize) -> Option<u64> {
          let v = self.shards[shard].handoff_blocked_since_ms.load(Ordering::Relaxed);
          (v != 0).then_some(v)
      }

      pub(crate) fn worker_running(&self, worker: usize) -> bool {
          self.workers.get(worker).is_some_and(|w| w.running.load(Ordering::Acquire))
      }

      /// Spec §5.3: `Up|Respawning` AND the worker owning partition
      /// `shard % worker_count` is running.
      pub(crate) fn ingest_capable(&self, shard: usize) -> bool {
          let state = self.shards[shard].state();
          matches!(state, ShardState::Up | ShardState::Respawning)
              && self.worker_running(shard % self.workers.len().max(1))
      }

      #[cfg_attr(not(test), allow(dead_code))] // PR 3 routes with this
      pub(crate) fn ingest_capable_count(&self) -> usize {
          (0..self.shards.len()).filter(|&s| self.ingest_capable(s)).count()
      }

      /// The (n mod k)-th ingest-capable shard. A shard that changes state
      /// between the count and the walk is tolerated: the walk falls back to
      /// the first capable shard, or `None`.
      #[cfg_attr(not(test), allow(dead_code))] // PR 3 routes with this
      pub(crate) fn pick_live(&self, n: u64) -> Option<u32> {
          let k = self.ingest_capable_count();
          if k == 0 {
              return None;
          }
          let target = (n % k as u64) as usize;
          let mut capable = (0..self.shards.len()).filter(|&s| self.ingest_capable(s));
          capable
              .clone()
              .nth(target)
              .or_else(|| capable.next())
              .map(|s| s as u32)
      }

      #[cfg_attr(not(test), allow(dead_code))] // PR 3's settle wait reads this
      pub(crate) fn all_settled(&self) -> bool {
          self.shards.iter().all(|s| s.state() != ShardState::Starting)
      }

      pub(crate) fn shard_evidence(&self) -> Vec<ShardEvidence> {
          (0..self.shards.len())
              .map(|s| ShardEvidence {
                  id: s as u16,
                  state: self.state(s),
                  reason: self.reason(s),
                  since_unix_ms: self.shards[s].since_ms.load(Ordering::Relaxed),
                  respawns: self.respawns(s),
                  worker_running: self.worker_running(s % self.workers.len().max(1)),
                  handoff_blocked_since_unix_ms: self.handoff_blocked_since_ms(s),
                  active_segment_age_secs: None,
              })
              .collect()
      }

      pub(crate) fn worker_evidence(&self) -> Vec<WorkerEvidence> {
          self.workers
              .iter()
              .enumerate()
              .map(|(i, w)| {
                  let exited = w.exited_at_ms.load(Ordering::Acquire);
                  WorkerEvidence {
                      id: i as u16,
                      running: w.running.load(Ordering::Acquire),
                      exited_at_unix_ms: (exited != 0).then_some(exited),
                  }
              })
              .collect()
      }
  }

  /// Held by a worker thread for its whole life; `Drop` runs on a normal
  /// exit and during a panic unwind alike.
  pub(crate) struct WorkerGuard {
      registry: Arc<ShardRegistry>,
      worker: usize,
  }

  impl WorkerGuard {
      pub(crate) fn enter(registry: Arc<ShardRegistry>, worker: usize) -> Self {
          registry.workers[worker].running.store(true, Ordering::Release);
          WorkerGuard { registry, worker }
      }
  }

  impl Drop for WorkerGuard {
      fn drop(&mut self) {
          let cell = &self.registry.workers[self.worker];
          cell.exited_at_ms.store(unix_ms_now().max(1), Ordering::Release);
          cell.running.store(false, Ordering::Release);
      }
  }

  /// One shard's handle on the registry, for its supervisor and flusher.
  #[derive(Clone)]
  pub(crate) struct ShardSlot {
      registry: Arc<ShardRegistry>,
      shard: usize,
  }

  impl ShardSlot {
      pub(crate) fn new(registry: Arc<ShardRegistry>, shard: usize) -> Self {
          ShardSlot { registry, shard }
      }
      #[cfg_attr(not(any(test, feature = "dst")), allow(dead_code))] // DST and tests only
      pub(crate) fn detached() -> Self {
          Self::new(ShardRegistry::detached(), 0)
      }
      pub(crate) fn mark_up(&self) {
          self.registry.mark_up(self.shard);
      }
      pub(crate) fn mark_respawning(&self) {
          self.registry.mark_respawning(self.shard);
      }
      pub(crate) fn mark_exit(&self, exit: SupervisorExit) {
          self.registry.mark_exit(self.shard, exit);
      }
      #[cfg_attr(not(test), allow(dead_code))] // tests; PR 3's hand-off marker
      pub(crate) fn registry(&self) -> &Arc<ShardRegistry> {
          &self.registry
      }
      #[cfg_attr(not(test), allow(dead_code))] // PR 3's hand-off marker reads it
      pub(crate) fn shard(&self) -> usize {
          self.shard
      }
  }
  ```
  The `#[cfg_attr(not(test), allow(dead_code))]` markers follow the existing idiom (reload.rs:53). They name the later PR that writes or reads each item. Two further items have no production caller until later in this PR: `shard_evidence` and `worker_evidence` until Task 1.17. Their `dead_code` warnings between those tasks are expected; `cargo clippy -- -D warnings` is the PR-level gate (Task 1.28).
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server status::shards -- --test-threads=1`. Expected: 7 passed.
- [ ] **Step 5: Commit.** `git add crates/weir-server/src/status && git commit -m "feat(status): shard registry, worker cells and live-shard selection"`

---

### Task 1.13: `FlusherExit`: the supervisor writes a shard's terminal state

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/wab/mod.rs`:
  - `WabHandle` doc (:106-113);
  - `run_with_panic_supervision` (:172-226);
  - `spawn` (:263-358): one new parameter, the slot per shard;
  - `flusher_thread` (:791-1028): one new last parameter, `-> FlusherExit`, `mark_up` after the counter scan, `return FlusherExit::CounterScanFailed` at :869, `FlusherExit::InputClosed` at the end;
  - the supervisor tests (:1989-2115, the `// ── Panic supervision` block).
- Modify: `crates/weir-server/src/wab/dst.rs`, the four `flusher_thread` sites (:924, :1160, :1474, :1637) and the two `run_with_panic_supervision` sites (:1148, :1277).
- Modify: `crates/weir-server/src/main.rs:1087-1093` (`let wab_handle = wab::spawn(`): pass the registry. In this task `main` still creates a throwaway `ShardRegistry::new(config.shard_count, config.worker_count, Arc::new(AtomicBool::new(false)))` inline; Task 1.23 replaces it with `Arc::clone(&board.shards)`.
- Test: `crates/weir-server/src/wab/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: `status::{ShardRegistry, ShardSlot, SupervisorExit}` (Task 1.12).
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum FlusherExit { InputClosed, CounterScanFailed }
  fn run_with_panic_supervision<F, B, C>(shard_id: usize, metrics: Arc<Metrics>, clock: &C, slot: &ShardSlot,
                                         body_factory: F) -> SupervisorExit
      where F: FnMut() -> B, B: FnOnce() -> FlusherExit, C: BlockingClock;
  fn flusher_thread<C: BlockingClock>(/* 16 existing params */, slot: &ShardSlot) -> FlusherExit;
  pub(crate) fn spawn(wab_dir: PathBuf, config: WabConfig, drain_tx: Sender<PathBuf>, metrics: Arc<Metrics>,
                      coalesce_hint: Arc<AtomicU64>, registry: Arc<ShardRegistry>) -> io::Result<WabHandle>;
  ```
  PR 3 moves `registry` unchanged onto `spawn_flushers` and appends its `handoff` parameter after `slot`.

- [ ] **Step 1: Write the failing tests** (append inside `wab/mod.rs`'s `mod tests`):
  ```rust
  use crate::status::{ShardRegistry, ShardSlot, SupervisorExit};
  use weir_status::{OfflineReason, ShardState};

  fn slot_with_flag(shutting_down: bool) -> ShardSlot {
      let flag = Arc::new(std::sync::atomic::AtomicBool::new(shutting_down));
      ShardSlot::new(Arc::new(ShardRegistry::new(1, 1, flag)), 0)
  }

  #[test]
  fn the_supervisor_maps_every_exit_to_a_terminal_registry_state() {
      let m = Arc::new(crate::metrics::Metrics::new().0);
      let clock = crate::wab::dst::SimClock::new();
      let cases: Vec<(bool, FlusherExit, SupervisorExit, ShardState, Option<OfflineReason>)> = vec![
          (false, FlusherExit::InputClosed, SupervisorExit::InputClosed, ShardState::Offline, Some(OfflineReason::FlusherExited)),
          (true, FlusherExit::InputClosed, SupervisorExit::InputClosed, ShardState::Stopped, None),
          (false, FlusherExit::CounterScanFailed, SupervisorExit::CounterScanFailed, ShardState::Offline, Some(OfflineReason::CounterScanFailed)),
      ];
      for (sd, body_exit, want_exit, want_state, want_reason) in cases {
          let slot = slot_with_flag(sd);
          let got = run_with_panic_supervision(0, Arc::clone(&m), &clock, &slot, || move || body_exit);
          assert_eq!(got, want_exit);
          assert_eq!(slot.registry().state(0), want_state, "{body_exit:?} shutting_down={sd}");
          assert_eq!(slot.registry().reason(0), want_reason);
      }
  }

  #[test]
  fn panic_exhaustion_ends_respawn_exhausted_after_marking_each_respawn() {
      let m = Arc::new(crate::metrics::Metrics::new().0);
      let clock = crate::wab::dst::SimClock::new();
      let slot = slot_with_flag(false);
      let got = run_with_panic_supervision(0, Arc::clone(&m), &clock, &slot, || {
          || -> FlusherExit { panic!("persistent panic") }
      });
      assert_eq!(got, SupervisorExit::RespawnExhausted);
      assert_eq!(slot.registry().state(0), ShardState::Offline);
      assert_eq!(slot.registry().reason(0), Some(OfflineReason::RespawnExhausted));
      assert_eq!(slot.registry().respawns(0), MAX_FLUSHER_RESPAWNS);
  }

  /// The counter-scan give-up used to be a bare `return`, indistinguishable
  /// from a clean channel close (spec §5.3). A missing shard dir makes every
  /// `segment_counters` read_dir fail.
  #[test]
  fn a_counter_scan_give_up_returns_counter_scan_failed_and_never_marks_up() {
      let slot = ShardSlot::detached();
      let (_work_tx, work_rx) = crossbeam_channel::bounded::<Batch>(1);
      let (drain_tx, _drain_rx) = crossbeam_channel::bounded::<PathBuf>(1);
      let missing = tmp_dir("counter_scan_missing").join("no_such_shard");
      let exit = flusher_thread(
          0, missing, work_rx, drain_tx, 1, Duration::from_millis(5), 64 * 1024 * 1024, None, None, None,
          Arc::new(crate::metrics::Metrics::new().0), Arc::new(AtomicU64::new(200)),
          &crate::wab::dst::SimClock::new(), Arc::new(FsSegmentStore), Compression::None, 1, &slot,
      );
      assert_eq!(exit, FlusherExit::CounterScanFailed);
      assert_eq!(slot.registry().state(0), ShardState::Starting, "only the supervisor writes terminal states");
  }

  #[test]
  fn a_flusher_marks_its_shard_up_after_the_counter_scan_and_returns_input_closed() {
      let slot = ShardSlot::detached();
      let (work_tx, work_rx) = crossbeam_channel::bounded::<Batch>(1);
      drop(work_tx); // Disconnected on the first recv
      let (drain_tx, _drain_rx) = crossbeam_channel::bounded::<PathBuf>(1);
      let exit = flusher_thread(
          0, tmp_dir("flusher_marks_up"), work_rx, drain_tx, 1, Duration::from_millis(5), 64 * 1024 * 1024,
          None, None, None, Arc::new(crate::metrics::Metrics::new().0), Arc::new(AtomicU64::new(200)),
          &crate::wab::dst::SimClock::new(), Arc::new(FsSegmentStore), Compression::None, 1, &slot,
      );
      assert_eq!(exit, FlusherExit::InputClosed);
      assert_eq!(slot.registry().state(0), ShardState::Up);
  }
  ```
  `tmp_dir` is the module's existing helper (wab/mod.rs:1308). `Batch`, `Compression`, `FsSegmentStore`, `Duration` and `AtomicU64` are already in scope through `use super::*` in that test module. If one is not, add it to the test module's `use` list.

  Then update the existing tests:
  - `panic_then_recover_factory` returns `impl FnMut() -> Box<dyn FnOnce() -> FlusherExit + Send>`. Its inner closure ends with `FlusherExit::InputClosed` after the `if`.
  - Every `run_with_panic_supervision(a, b, c, f)` call in this module becomes `run_with_panic_supervision(a, b, c, &ShardSlot::detached(), f)`.
  - `supervisor_lets_clean_exit_pass_through` uses `|| || FlusherExit::InputClosed`.
  - `supervisor_catches_formatted_panic_and_increments_metric`'s inner closure ends with `FlusherExit::InputClosed`.
  - `flusher_panic_loop_caps_out_after_max_respawns` uses `|| || -> FlusherExit { panic!("persistent panic") }`.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server wab:: -- --test-threads=1`. Expected: compile errors `cannot find type `FlusherExit``, `this function takes 4 arguments but 5 arguments were supplied`.
- [ ] **Step 3: Implement.** In `wab/mod.rs`:
  ```rust
  use crate::status::{ShardRegistry, ShardSlot, SupervisorExit};

  /// How a flusher body ended (spec §5.3). A panic is not a value: the
  /// supervisor sees it as `Err` from `catch_unwind`.
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum FlusherExit {
      /// The batch channel disconnected (every worker gone).
      InputClosed,
      /// The startup counter scan failed repeatedly; the shard refuses to seal.
      CounterScanFailed,
  }
  ```
  `run_with_panic_supervision`:
  ```rust
  fn run_with_panic_supervision<F, B, C>(
      shard_id: usize,
      metrics: Arc<Metrics>,
      clock: &C,
      slot: &ShardSlot,
      mut body_factory: F,
  ) -> SupervisorExit
  where
      F: FnMut() -> B,
      B: FnOnce() -> FlusherExit,
      C: BlockingClock,
  {
      let mut attempt: u32 = 0;
      loop {
          let body = body_factory();
          let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
          match result {
              // The supervisor is the single writer of terminal states (spec §5.3).
              Ok(exit) => {
                  let exit = match exit {
                      FlusherExit::InputClosed => SupervisorExit::InputClosed,
                      FlusherExit::CounterScanFailed => SupervisorExit::CounterScanFailed,
                  };
                  slot.mark_exit(exit);
                  return exit;
              }
              Err(panic_payload) => {
                  let msg = panic_message_str(&panic_payload);
                  metrics.wab_flusher_panics.inc();
                  attempt += 1;
                  if attempt > MAX_FLUSHER_RESPAWNS {
                      tracing::error!(/* unchanged fields and text */);
                      slot.mark_exit(SupervisorExit::RespawnExhausted);
                      return SupervisorExit::RespawnExhausted;
                  }
                  slot.mark_respawning();
                  tracing::warn!(/* unchanged */);
                  clock.sleep(Duration::from_millis(10 * u64::from(attempt)));
              }
          }
      }
  }
  ```
  (Keep the two existing `tracing` calls byte-for-byte; only the two `slot.` lines and the return values are new.)

  `flusher_thread`:
  - add `slot: &ShardSlot` as the last parameter and `-> FlusherExit` as the return type;
  - in the counter-scan loop (:854), change `Ok(()) => break,` to `Ok(()) => { slot.mark_up(); break; }`;
  - in the give-up arm, replace `return;` (:869) with `return FlusherExit::CounterScanFailed;` and extend the error text with "(weir_wab_shard_up=0, condition shard_offline/no_live_shard)";
  - after the final `match writer.seal_current() { … }` (:1005-1027), add `FlusherExit::InputClosed` as the tail expression.

  `spawn`: add the parameter `registry: Arc<ShardRegistry>`. Inside the per-shard loop, add `let slot = ShardSlot::new(Arc::clone(&registry), shard_id);`, then change the thread body to:
  ```rust
  .spawn(move || {
      // The supervisor's verdict is recorded on the registry; the return
      // value is for callers (DST, tests) that have no registry to read.
      let _ = run_with_panic_supervision(shard_id, metrics_for_panic, &RealClock, &slot, || {
          let sdir = sdir.clone();
          let rx = rx.clone();
          let drain_clone = drain_clone.clone();
          let metrics_for_flusher = Arc::clone(&metrics_for_flusher);
          let coalesce_hint = Arc::clone(&coalesce_hint_for_flusher);
          let slot = slot.clone();
          move || {
              let store: Arc<dyn SegmentStore> = Arc::new(FsSegmentStore);
              flusher_thread(
                  shard_id as u16, sdir, rx, drain_clone, batch_size, batch_deadline, segment_max_bytes,
                  segment_max_age, segment_max_lifetime, core_id, metrics_for_flusher, coalesce_hint,
                  &RealClock, store, compression, compression_level, &slot,
              )
          }
      });
  })
  ```
  `dst.rs`:
  - :924 and :1637 add `&crate::status::ShardSlot::detached(),` as the last argument (each call keeps its trailing `;`; the spawn closure discards the value);
  - :1474 likewise;
  - :1148: `super::run_with_panic_supervision(0, metrics_for_panic, &clock, &crate::status::ShardSlot::detached(), || { … move || { super::flusher_thread(…, 1, &slot) } })`. Add `let slot = crate::status::ShardSlot::detached();` before the `std::thread::spawn` and `let slot = slot.clone();` inside the factory, and **remove the semicolon** after the inner `flusher_thread(...)` call so the body returns `FlusherExit`;
  - :1277: `super::run_with_panic_supervision(0, Arc::clone(&metrics), &clock, &crate::status::ShardSlot::detached(), || { || -> FlusherExit { panic!("DST: persistent flusher panic") } });` with `use super::FlusherExit;` at the top of the function or module.

  `main.rs:1087`, the call becomes:
  ```rust
  let wab_handle = wab::spawn(
      config.wab_dir.clone(),
      wab_config,
      drain_tx,
      Arc::clone(&metrics),
      Arc::clone(&coalesce_hint),
      Arc::new(status::ShardRegistry::new(
          config.shard_count,
          config.worker_count,
          Arc::new(std::sync::atomic::AtomicBool::new(false)),
      )), // Task 1.23 replaces this with the board's registry
  )?;
  ```
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server wab:: -- --test-threads=1`. Expected: all wab tests pass, including the 4 new ones and the 5 updated supervisor tests.
  - `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`. Expected: the DST sweep passes.
- [ ] **Step 5: Commit.** `git commit -am "feat(wab): flusher exits carry a reason the supervisor records on the shard registry"`

---

### Task 1.14: Worker drop guards

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/worker.rs:266-356`:
  - `spawn_workers` becomes a `#[cfg(test)]` wrapper;
  - new `spawn_workers_with`;
  - one guard line inside the thread.
- Modify: `crates/weir-server/src/main.rs:1087-1109` (`let wab_handle = wab::spawn(` through `worker::spawn_workers(`): declare `worker_registry` and call `spawn_workers_with(.., Some(registry))`. Task 1.23 re-points it at the board.
- Test: `crates/weir-server/src/worker.rs` (`mod tests`)

**Interfaces:**
- Consumes: `status::{ShardRegistry, WorkerGuard}`.
- Produces:
  ```rust
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn spawn_workers_with(queue_rx: &QueueReceiver<WorkUnit>, shard_txs: Vec<Sender<Batch>>,
      shard_count: usize, worker_count: usize, batch_size: usize, batch_deadline: Duration,
      coalesce_hint: Arc<AtomicU64>, registry: Option<Arc<ShardRegistry>>) -> Vec<thread::JoinHandle<()>>;
  #[cfg(test)] pub fn spawn_workers(/* unchanged 7 params */) -> Vec<thread::JoinHandle<()>>;  // = spawn_workers_with(.., None)
  ```

- [ ] **Step 1: Write the failing test** (append to `worker.rs` `mod tests`):
  ```rust
  #[test]
  fn a_worker_marks_itself_running_and_exited_on_the_registry() {
      use crate::status::ShardRegistry;
      let registry = Arc::new(ShardRegistry::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))));
      let (queue_tx, queue_rx) = crate::queue::new::<WorkUnit>(1);
      let (shard_txs, _shard_rxs) = make_shard_channels(1);
      let handles = spawn_workers_with(
          &queue_rx, shard_txs, 1, 1, 8, Duration::from_millis(5), default_hint(), Some(Arc::clone(&registry)),
      );
      let deadline = std::time::Instant::now() + Duration::from_secs(2);
      while !registry.worker_running(0) {
          assert!(std::time::Instant::now() < deadline, "worker never marked itself running");
          std::thread::sleep(Duration::from_millis(5));
      }
      drop(queue_tx);
      for h in handles {
          h.join().unwrap();
      }
      assert!(!registry.worker_running(0));
      assert!(registry.worker_evidence()[0].exited_at_unix_ms.is_some());
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server worker::tests::a_worker_marks -- --test-threads=1`. Expected: `cannot find function `spawn_workers_with``.
- [ ] **Step 3: Implement.** Rename the existing `pub fn spawn_workers` (worker.rs:266) to `pub(crate) fn spawn_workers_with`, add the parameter `registry: Option<Arc<crate::status::ShardRegistry>>`, and put `#[allow(clippy::too_many_arguments)]` on it (8 parameters; clippy's limit is 7). In the per-worker loop, add `let registry = registry.clone();` next to `let hint = Arc::clone(&coalesce_hint);`. Inside the spawned closure, as its first statement:
  ```rust
  // Spec §5.3: a worker that exits or panics takes the shards it owns
  // (shard % worker_count) out of routing and readiness. Dropped on a
  // normal return and during unwinding alike.
  let _alive = registry.map(|r| crate::status::WorkerGuard::enter(r, worker_idx));
  ```
  Then add the test-only wrapper with the original doc comment:
  ```rust
  #[cfg(test)]
  pub fn spawn_workers(
      queue_rx: &QueueReceiver<WorkUnit>,
      shard_txs: Vec<Sender<Batch>>,
      shard_count: usize,
      worker_count: usize,
      batch_size: usize,
      batch_deadline: Duration,
      coalesce_hint: Arc<AtomicU64>,
  ) -> Vec<thread::JoinHandle<()>> {
      spawn_workers_with(queue_rx, shard_txs, shard_count, worker_count, batch_size, batch_deadline, coalesce_hint, None)
  }
  ```
  In `main.rs`, immediately before `let wab_handle = wab::spawn(` (:1087; `wab::spawn` consumes the registry, so declaring it before `spawn_workers` at :1101 would not compile): `let worker_registry = Arc::new(status::ShardRegistry::new(config.shard_count, config.worker_count, Arc::new(std::sync::atomic::AtomicBool::new(false))));`. Pass it to both `wab::spawn` (`Arc::clone(&worker_registry)`, replacing the inline registry from Task 1.13) and `worker::spawn_workers_with(&queue_rx, wab_handle.shard_txs, config.shard_count, config.worker_count, config.batch_size, Duration::from_millis(config.batch_deadline_ms), coalesce_hint, Some(Arc::clone(&worker_registry)))`. Task 1.23 replaces `worker_registry` with `board.shards`.
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server worker:: -- --test-threads=1`. Expected: all worker tests pass (the 7 existing ones still call `spawn_workers`).
- [ ] **Step 5: Commit.** `git commit -am "feat(worker): a drop guard records each worker's running state on the shard registry"`

---
### Task 1.15: `ConfigView`, an exhaustive and classified view of `Config`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/status/config_view.rs`
- Modify: `crates/weir-server/src/status/mod.rs` (`pub(crate) mod config_view;`)
- Modify: `crates/weir-server/src/config/mod.rs:1302` (add `Config::commit_timeout`)
- Modify: `crates/weir-server/src/main.rs:1129` (use `config.commit_timeout()`)
- Test: `crates/weir-server/src/status/config_view.rs` (`mod tests`)

**Interfaces:**
- Consumes: `crate::config::{Config, PartialConfig, SinkType, RedactedUrl}`, `crate::wab::lock::LockPolicy`, `weir_status::{ConfigValue, ConfigEvidence, PublicConfig}`.
- Produces:
  ```rust
  impl Config { pub(crate) fn commit_timeout(&self) -> Duration; }   // max(2 × sink_timeout_secs, 60 s)
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum Disclosure { Public, Admin, AdminRedacted }
  pub(crate) struct ConfigView { /* BTreeMap<&'static str, (Disclosure, ConfigValue)>, sink_type, commit_timeout_secs */ }
  impl From<&Config> for ConfigView;               // exhaustive destructure, no `..`
  impl fmt::Debug for ConfigView;                  // `{key: value, ...}` for the startup line
  impl ConfigView {
      pub(crate) fn view(&self) -> BTreeMap<String, ConfigValue>;
      pub(crate) fn public(&self) -> PublicConfig;
      pub(crate) fn evidence(&self) -> ConfigEvidence;
      pub(crate) fn disclosure(&self, key: &str) -> Option<Disclosure>;
      pub(crate) fn sink_type(&self) -> &'static str;
      #[cfg(test)] pub(crate) fn empty_for_test() -> Self;
  }
  pub(crate) fn url_origin(url: &str) -> String;   // "scheme://host[:port]" or "<unparseable>"
  ```
  PR 5 adds `livez_checks` to the destructure with the same `admin(..)` call this task uses for `wab_dir_lock`.

- [ ] **Step 1: Write the failing tests** (`config_view.rs`, test module only):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::config::PartialConfig;
      use std::time::Duration;

      fn config(label: &str, layer: PartialConfig) -> Config {
          let dir = crate::testutil::scratch_dir(&format!("cfgview_{label}"));
          crate::testutil::mkdir_p(&dir);
          Config::from_layers(PartialConfig { wab_dir: Some(dir), ..layer }, PartialConfig::empty(), PartialConfig::empty())
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
          doc.retain(|k| k != "drain_shutdown_budget_secs"); // the key arrives with PR 4
          doc.sort();
          assert_eq!(public, doc);
          let p = v.public();
          assert!(p.shard_count.is_some() && p.tls_enabled == Some(false) && p.peer_uid_check.is_some());
      }

      #[cfg(feature = "http-sink")]
      #[test]
      fn a_sink_url_is_reduced_to_scheme_host_and_port() {
          let v = ConfigView::from(&config("url", PartialConfig {
              sink_type: Some("http".into()),
              sink_url: Some("https://user:SENTINEL_PW@sink.example:8443/hooks/SENTINEL_PATH?token=SENTINEL_TOK".into()),
              ..PartialConfig::empty()
          }));
          let view = v.view();
          assert_eq!(view["sink_url"], ConfigValue::Str("https://sink.example:8443".into()));
          assert_eq!(view["sink_url_path_redacted"], ConfigValue::Bool(true));
          assert_eq!(v.disclosure("sink_url"), Some(Disclosure::AdminRedacted));
          let shown = format!("{v:?} {:?}", serde_json::to_string(&view).unwrap());
          assert!(!shown.contains("SENTINEL"), "{shown}");
      }

      #[test]
      fn the_s3_secret_never_enters_the_view_and_the_key_id_is_only_a_presence_flag() {
          let v = ConfigView::from(&config("s3secret", PartialConfig {
              sink_s3_secret_access_key: Some("SENTINEL_SECRET".into()),
              sink_s3_access_key_id: Some("SENTINEL_KEYID".into()),
              ..PartialConfig::empty()
          }));
          let shown = format!("{v:?} {}", serde_json::to_string(&v.view()).unwrap());
          assert!(!shown.contains("SENTINEL"), "{shown}");
          assert_eq!(v.view()["sink_s3_access_key_id_set"], ConfigValue::Bool(true));
          assert!(v.disclosure("sink_s3_secret_access_key").is_none());
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
          assert!(!line.contains("U64("), "values print raw, not as enum variants: {line}");
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::config_view -- --test-threads=1`. Expected: `cannot find type `ConfigView``, `no method named `commit_timeout``.
- [ ] **Step 3: Implement.** `config/mod.rs`, inside `impl Config` at :1302:
  ```rust
  /// The drain's backstop commit timeout: at least 60 s and at least twice
  /// the sink's own timeout, so it fires only when a sink ignores its own.
  pub(crate) fn commit_timeout(&self) -> std::time::Duration {
      std::time::Duration::from_secs(self.sink_timeout_secs.saturating_mul(2).max(60))
  }
  ```
  `main.rs:1129`: `commit_timeout: config.commit_timeout(),` (the comment above it stays).

  `config_view.rs`, above the tests:
  ```rust
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

  /// `scheme://host[:port]` of a URL: userinfo, path, query and fragment are
  /// dropped. Webhook-style URLs carry the secret in the path, which the
  /// existing log redactors (sink/mod.rs:50-120) do not cover. Anything
  /// without `://` is not echoed at all.
  pub(crate) fn url_origin(url: &str) -> String {
      let Some(i) = url.find("://") else { return "<unparseable>".to_string() };
      let (scheme, rest) = (&url[..i], &url[i + 3..]);
      let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
      let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
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
              peer_uid_check,
              shutdown_timeout_secs,
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
          v.public("wab_segment_max_lifetime_secs", u(*wab_segment_max_lifetime_secs));
          v.public("sink_max_retries", u(u64::from(*sink_max_retries)));
          v.public("sink_timeout_secs", u(*sink_timeout_secs));
          v.public("health_poll_interval_secs", u(*health_poll_interval_secs));
          v.public("shutdown_timeout_secs", u(*shutdown_timeout_secs));
          v.public("max_connections", u(*max_connections as u64));
          v.public("peer_uid_check", b(*peer_uid_check));
          v.public("tls_enabled", b(tcp_bind.is_some()));
          // Admin.
          v.admin("socket_path", s(socket_path.display()));
          v.admin("wab_dir", s(wab_dir.display()));
          v.admin("batch_size", u(*batch_size as u64));
          v.admin("batch_deadline_ms", u(*batch_deadline_ms));
          v.admin("wab_compression", s(wab_compression));
          v.admin("wab_compression_level", ConfigValue::I64(i64::from(*wab_compression_level)));
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
          v.admin("metrics_max_connections", u(*metrics_max_connections as u64));
          v.admin("connection_read_timeout_secs", u(*connection_read_timeout_secs));
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
              v.admin("sink_mysql_insert_mode", s(format!("{sink_mysql_insert_mode:?}")));
          }
          #[cfg(feature = "postgres-sink")]
          {
              v.admin("sink_postgres_table", s(sink_postgres_table));
              v.admin("sink_postgres_column", s(sink_postgres_column));
              v.admin("sink_postgres_id_column", opt_s(sink_postgres_id_column.as_ref()));
              v.admin("sink_postgres_insert_mode", s(format!("{sink_postgres_insert_mode:?}")));
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
              if sink_s3_endpoint.is_empty() { ConfigValue::Null } else { s(url_origin(sink_s3_endpoint)) },
          );
          v.admin("sink_s3_force_path_style", b(*sink_s3_force_path_style));
          v.admin("sink_s3_prefix", s(sink_s3_prefix));
          v.admin("sink_s3_partition", s(sink_s3_partition));
          v.admin("sink_s3_framing", s(sink_s3_framing));
          v.admin("sink_s3_compression", s(sink_s3_compression));
          v.redacted("sink_s3_access_key_id_set", b(!sink_s3_access_key_id.is_empty()));
          v.admin("sink_s3_storage_class", s(sink_s3_storage_class));
          v.admin("sink_s3_sse", s(sink_s3_sse));
          v.redacted("sink_s3_sse_kms_key_id_set", b(!sink_s3_sse_kms_key_id.is_empty()));
          v.admin("dead_letter_check_interval_secs", u(*dead_letter_check_interval_secs));
          v.admin("log_level", s(log_level));
          v.admin("tcp_bind", opt_s(tcp_bind.as_ref()));
          v.admin("tls_cert_path", opt_s(tls_cert_path.as_ref().map(|p| p.display())));
          v.admin("tls_key_path", opt_s(tls_key_path.as_ref().map(|p| p.display())));
          v.admin("tls_client_ca_path", opt_s(tls_client_ca_path.as_ref().map(|p| p.display())));
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
          self.entries.iter().map(|(k, (_, v))| (k.to_string(), v.clone())).collect()
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
          ConfigView { entries: BTreeMap::new(), sink_type: "noop", commit_timeout_secs: 60 }
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
          f.debug_map().entries(self.entries.iter().map(|(k, (_, v))| (k, Raw(v)))).finish()
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server status::config_view -- --test-threads=1`, then with `--all-features`. Expected: 6 passed on both lines. The cfg-gated destructure compiles under every feature combination.
  - Then verify the compile guard by mutation:
    1. Add `pub probe_field: u8,` as the last field of `Config` (config/mod.rs:602) and `probe_field: 0,` at the end of the `Ok(Config { … })` literal in `from_layers` (:1295).
    2. `cargo build -p weir-server` must FAIL with `pattern does not mention field `probe_field`` at config_view.rs.
    3. Revert both lines.
- [ ] **Step 5: Commit.** `git add crates/weir-server/src && git commit -am "feat(status): ConfigView, an exhaustive disclosure-classified view of Config"`

---

### Task 1.16: The status board: drain, sink, WAB, admin and reload cells, latches, `StatusBoard`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/status/board.rs`: add the cells and `StatusBoard` above the test module.
- Modify: `crates/weir-server/src/status/mod.rs`: re-exports, `test_board`, the lock-scan test.
- Test: `crates/weir-server/src/status/board.rs`, `crates/weir-server/src/status/mod.rs`

**Interfaces:**
- Consumes: Tasks 1.11, 1.12, 1.15; `crate::metrics::DrainStateValue`; `crate::sink::SinkHealth`; `crate::reload::{ReloadReport, ReloadTrigger, ReloadOutcome}`.
- Produces (the names PR 2 codes against are kept verbatim):
  ```rust
  pub(crate) use weir_status::SinkFailureKind;                        // re-exported from crate::status
  #[derive(Clone, Debug, PartialEq, Eq)] pub(crate) struct InFlight { pub segment: String, pub attempt: u32,
      pub retries_left: u32, pub processed: u64, pub next_retry_at_ms: Option<u64> }
  #[derive(Clone, Debug, PartialEq, Eq)] pub(crate) struct RequeueResult { pub ticket: u64, pub requeued: u64,
      pub already_queued: u64, pub scan_error: Option<String>, pub backoff_cut: bool, pub coalesced: bool, pub at_ms: u64 }
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum DrainExitReason { Stopped, DeadlineAbandoned, GaveUp }
  #[derive(Clone, Debug, PartialEq, Eq)] pub(crate) struct DrainExitInfo { pub reason: DrainExitReason,
      pub abandoned: Option<(String, u64)>, pub pending_left: usize, pub backlog_left: usize,
      pub last_panic: Option<String>, pub at_ms: u64 }
  pub(crate) struct DrainCell { pub(crate) state_since_ms: AtomicU64, pub(crate) pause_requested: AtomicBool,
      pub(crate) paused_by_uid: AtomicU64, pub(crate) paused_since_ms: AtomicU64, pub(crate) held: AtomicBool,
      pub(crate) observed_requeue: AtomicU64, pub(crate) generation: AtomicU64,
      pub(crate) in_flight: ArcSwapOption<InFlight>, pub(crate) commit_in_flight_since_ms: AtomicU64,
      pub(crate) last_progress_ms: AtomicU64, pub(crate) pending_len: AtomicUsize, pub(crate) backlog_len: AtomicUsize,
      pub(crate) replay_total: AtomicU64, pub(crate) replay_remaining: AtomicU64, pub(crate) blocked_since_ms: AtomicU64,
      pub(crate) stopping: AtomicBool, pub(crate) deadline_ms: AtomicU64,
      pub(crate) last_requeue: ArcSwapOption<RequeueResult>, pub(crate) exit: ArcSwapOption<DrainExitInfo>, /* state: private */ }
  impl DrainCell { set_state(&self, DrainStateValue); state(&self) -> Option<DrainStateValue>;
      set_pause_requested(&self, bool, Option<u32>); set_paused(&self, paused_since_ms: u64, held: bool);
      set_generation(&self, u64); set_in_flight(&self, Option<InFlight>); commit_started(&self); commit_finished(&self);
      stamp_progress(&self); set_queue_lens(&self, usize, usize); set_replay(&self, u64, u64);
      set_blocked_since(&self, u64); set_stopping(&self, Option<u64>); set_last_requeue(&self, RequeueResult);
      set_observed_requeue(&self, u64); set_exit(&self, DrainExitInfo);
      evidence(&self, heartbeat_age_secs: Option<f64>, heartbeat_changed_ms: Option<u64>,
               busy: Option<BusyEvidence>) -> Option<DrainEvidence>; }
  pub(crate) struct SinkCell { pub(crate) health_since_ms: AtomicU64, pub(crate) last_success_ms: AtomicU64,
      pub(crate) last_failure_ms: AtomicU64, pub(crate) consecutive_failures: AtomicU64,
      pub(crate) last_error: ArcSwapOption<String>, pub(crate) probe_reason: ArcSwapOption<String>, /* health, kind: private */ }
  impl SinkCell { set_health(&self, &SinkHealth, Option<String>); record_success(&self);
      record_failure(&self, SinkFailureKind, String); evidence(&self) -> Option<SinkEvidence>; }
  #[derive(Clone, Debug, Default, PartialEq, Eq)] pub(crate) struct LagReport { pub undelivered_segments: u64,
      pub undelivered_records: u64, pub oldest_created_ms: Option<u64>, pub active_created_ms: Vec<(u16, u64)>,
      pub dead_letter_bytes: u64, pub dead_letter_files: u64, pub quarantine_bytes: u64, pub quarantine_files: u64,
      pub scanned_at_ms: u64 }
  pub(crate) struct WabCell; impl WabCell { note_scan_ok(&self, now_ms: u64); first_scan_ok_ms(&self) -> Option<u64>;
      last_scan_ok_ms(&self) -> Option<u64>; set_lag(&self, LagReport); lag(&self) -> Option<Arc<LagReport>>;
      set_growth(&self, bool); growth(&self) -> Option<bool>; }
  pub(crate) struct AdminCell; impl AdminCell { set_bound(&self, path: String); set_unavailable(&self, AdminReason);
      set_disabled(&self); evidence(&self) -> AdminEvidence; }                        // PR 6 writes
  pub(crate) struct ReloadCell; impl ReloadCell { publish(&self, report: ReloadReport, now_ms: u64);
      evidence(&self) -> Option<ReloadEvidence>; }                                      // PR 6 writes
  pub(crate) struct ConditionLatches; impl ConditionLatches { new() -> Self; update(&self, &Evaluation, now_ms: u64); }
  impl weir_status::SinceLookup for ConditionLatches
  pub(crate) struct StaticInfo { pub version: &'static str, pub wire_version: u8, pub pid: u32, pub started_at_ms: u64,
      pub wab_dir: PathBuf, pub socket_path: PathBuf, pub wab_dir_dev: u64, pub wab_dir_ino: u64, pub config: Arc<ConfigView> }
  impl StaticInfo { pub(crate) fn capture(config: &Config, view: Arc<ConfigView>) -> Self; #[cfg(test)] pub(crate) fn for_test() -> Self; }
  pub(crate) const DEPTH_UNPUBLISHED: usize = usize::MAX;   // drain_depth before PR 3's first store
  pub(crate) const QUEUE_UNPUBLISHED: u64 = u64::MAX;
  pub(crate) struct StatusBoard { pub(crate) statik: StaticInfo, pub(crate) life: LifecycleCell,
      pub(crate) listeners: ListenerCells, pub(crate) shards: Arc<ShardRegistry>, pub(crate) drain: DrainCell,
      pub(crate) sink: SinkCell, pub(crate) wab: WabCell, pub(crate) admin: AdminCell, pub(crate) reload: ReloadCell,
      pub(crate) latches: ConditionLatches, pub(crate) recent: ArcSwap<RecentCounters>, pub(crate) queue_depth: AtomicU64,
      pub(crate) drain_depth: Arc<AtomicUsize>, pub(crate) channel_high_since_ms: AtomicU64,
      pub(crate) heartbeat_changed_ms: AtomicU64, pub(crate) drain_busy_since_ms: AtomicU64,
      pub(crate) drain_busy_sink_call: AtomicBool, pub(crate) wab_cap_rejecting: Arc<AtomicBool>,
      pub(crate) wab_bytes_now: Arc<AtomicU64> }
  impl StatusBoard { pub(crate) fn new(statik: StaticInfo, shard_count: usize, worker_count: usize,
      tcp_configured: bool, wab_cap_rejecting: Arc<AtomicBool>, wab_bytes_now: Arc<AtomicU64>) -> Arc<Self>;
      #[cfg(test)] pub(crate) fn for_test() -> Arc<Self>; #[cfg(test)] pub(crate) fn for_test_with(shards: usize, workers: usize) -> Arc<Self>; }
  #[cfg(test)] pub(crate) fn status::test_board(shard_count: usize, worker_count: usize) -> Arc<StatusBoard>;
  ```

- [ ] **Step 1: Write the failing tests.** Append inside `board.rs`'s `mod tests`:
  ```rust
  use crate::metrics::DrainStateValue;
  use weir_status::*;

  #[test]
  fn an_unpublished_drain_cell_is_unknown_not_healthy() {
      let d = DrainCell::new();
      assert!(d.evidence(None, None, None).is_none());
      assert_eq!(d.state(), None);
  }

  #[test]
  fn the_drain_cell_publishes_state_progress_and_exit() {
      let d = DrainCell::new();
      d.set_state(DrainStateValue::retrying_transient);
      let since = d.state_since_ms.load(Ordering::Relaxed);
      d.set_state(DrainStateValue::retrying_transient);
      assert_eq!(d.state_since_ms.load(Ordering::Relaxed), since, "since moves only on a change");
      d.set_in_flight(Some(InFlight { segment: "shard_02/seg_00000007.wab.sealed".into(), attempt: 2,
                                      retries_left: 1, processed: 64, next_retry_at_ms: Some(9) }));
      d.commit_started();
      let busy = BusyEvidence { sink_call: true, since_unix_ms: 4 };
      let e = d.evidence(Some(1.5), Some(5), Some(busy)).unwrap();
      assert_eq!(e.busy, Some(busy), "the poll task's busy reading passes through");
      assert_eq!(e.state, DrainStateCode::RetryingTransient);
      assert_eq!(e.in_flight.unwrap().segment, SegmentId { shard: 2, counter: 7 });
      assert!(e.commit_in_flight_since_unix_ms.is_some());
      assert!(e.last_progress_unix_ms.unwrap() >= 5, "max(heartbeat change, commit stamps)");
      d.commit_finished();
      assert!(d.evidence(None, None, None).unwrap().commit_in_flight_since_unix_ms.is_none());
      d.set_paused(123, true);
      d.set_pause_requested(true, Some(1000));
      let e = d.evidence(None, None, None).unwrap();
      assert!(e.paused && e.held && e.pause_requested);
      assert_eq!((e.paused_since_unix_ms, e.paused_by_uid), (Some(123), Some(1000)));
      d.set_exit(DrainExitInfo { reason: DrainExitReason::GaveUp, abandoned: None, pending_left: 0,
                                 backlog_left: 0, last_panic: Some("boom".into()), at_ms: 1 });
      let e = d.evidence(None, None, None).unwrap();
      assert_eq!(e.exit, DrainExitCode::GaveUp);
      assert_eq!(e.last_panic.as_deref(), Some("boom"));
      d.set_exit(DrainExitInfo { reason: DrainExitReason::DeadlineAbandoned, abandoned: None, pending_left: 1,
                                 backlog_left: 2, last_panic: None, at_ms: 2 });
      assert_eq!(d.evidence(None, None, None).unwrap().exit, DrainExitCode::Stopped);
  }

  #[test]
  fn the_sink_cell_tracks_health_success_and_failure_streaks() {
      let s = SinkCell::new();
      assert!(s.evidence().is_none());
      s.set_health(&crate::sink::SinkHealth::Down("refused".into()), Some("refused".into()));
      s.record_failure(SinkFailureKind::Timeout, "t1".into());
      s.record_failure(SinkFailureKind::Transient, "t2".into());
      let e = s.evidence().unwrap();
      assert_eq!(e.health, Some(SinkHealthCode::Down));
      assert_eq!(e.consecutive_failures, 2);
      assert_eq!(e.last_failure_kind, Some(SinkFailureKind::Transient));
      assert_eq!((e.last_error.as_deref(), e.probe_reason.as_deref()), (Some("t2"), Some("refused")));
      s.record_success();
      assert_eq!(s.evidence().unwrap().consecutive_failures, 0);
  }

  #[test]
  fn latches_keep_the_first_seen_time_until_the_condition_clears() {
      let l = ConditionLatches::new();
      let active = |codes: &[ConditionCode]| Evaluation {
          conditions: codes
              .iter()
              .map(|&code| Condition {
                  code, severity: Severity::Warning, affects: Affects::Ingest, since_unix_ms: None,
                  summary: String::new(), detail: None, evidence: Default::default(),
                  runbook: "".into(), gates: vec![],
              })
              .collect(),
          unevaluated: vec![],
      };
      l.update(&active(&[ConditionCode::SinkDown]), 100);
      l.update(&active(&[ConditionCode::SinkDown]), 200);
      assert_eq!(l.since(ConditionCode::SinkDown), Some(100));
      l.update(&active(&[]), 300);
      assert_eq!(l.since(ConditionCode::SinkDown), None);
      assert_eq!(l.since(ConditionCode::Unknown), None);
  }

  #[test]
  fn the_reload_cell_is_sticky_on_a_tls_failure_until_a_success() {
      use crate::reload::{ReloadOutcome, ReloadReport, ReloadTrigger};
      let r = ReloadCell::new();
      assert!(r.evidence().is_none());
      let report = |outcome, changed: Vec<&'static str>, rejected: Vec<(&'static str, String)>| ReloadReport {
          trigger: ReloadTrigger::Sighup, outcome, changed, rejected, note: None,
      };
      r.publish(report(ReloadOutcome::Failed, vec![], vec![("tls", "bad key".into())]), 10);
      let e = r.evidence().unwrap();
      assert!(e.tls_last_failed);
      assert_eq!(e.items[0].detail.as_deref(), Some("bad key"));
      r.publish(report(ReloadOutcome::Noop, vec![], vec![]), 20);
      assert!(r.evidence().unwrap().tls_last_failed, "a noop does not clear it");
      r.publish(report(ReloadOutcome::Ok, vec!["tls"], vec![]), 30);
      assert!(!r.evidence().unwrap().tls_last_failed);
      assert_eq!(r.evidence().unwrap().not_reloaded[0].item, ReloadItem::ConfigFile);
  }

  #[test]
  fn a_new_board_starts_initializing_with_unpublished_depths() {
      let b = StatusBoard::for_test_with(2, 1);
      assert_eq!(b.life.phase(), Phase::Initializing);
      assert_eq!(b.drain_depth.load(Ordering::Relaxed), DEPTH_UNPUBLISHED);
      assert_eq!(b.queue_depth.load(Ordering::Relaxed), QUEUE_UNPUBLISHED);
      assert_eq!(b.shards.shard_count(), 2);
      assert_eq!(b.admin.evidence().state, AdminState::Disabled);
      assert_eq!(*b.recent.load_full(), RecentCounters::default());
  }
  ```
  In `status/mod.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      /// Spec §9.2 rule 5: the status code takes no blocking lock, so a stalled
      /// reader can never delay an ack, a flusher or the drain. Walks every
      /// `.rs` file under src/status/ (subdirectories included), so a file a
      /// later task or PR adds (render.rs, poll.rs, PR 5's http.rs) is scanned
      /// without anyone listing it. Verified by mutation in Task 1.16 Step 4.
      #[test]
      fn no_blocking_locks_under_status() {
          let banned = [concat!("Mu", "tex"), concat!("Rw", "Lock")];
          let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/status");
          let mut pending = vec![dir.clone()];
          let mut scanned = 0;
          while let Some(d) = pending.pop() {
              for entry in std::fs::read_dir(&d).expect("src/status exists") {
                  let path = entry.unwrap().path();
                  if path.is_dir() {
                      pending.push(path);
                  } else if path.extension().is_some_and(|e| e == "rs") {
                      let src = std::fs::read_to_string(&path).unwrap();
                      for b in banned {
                          assert!(
                              !src.contains(b),
                              "{} mentions {b}; status/ must stay wait-free (spec §9.2)",
                              path.display()
                          );
                      }
                      scanned += 1;
                  }
              }
          }
          // mod.rs, board.rs, shards.rs and config_view.rs exist from this task on.
          assert!(scanned >= 4, "scanned only {scanned} files under {}", dir.display());
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status:: -- --test-threads=1`. Expected: `cannot find type `DrainCell`` / `SinkCell` / `ConditionLatches` / `ReloadCell` / `StatusBoard`.
- [ ] **Step 3: Implement** (board.rs, between `ListenerCells` and the tests; extend the imports):
  ```rust
  use super::config_view::ConfigView;
  use super::shards::ShardRegistry;
  use super::unix_ms_now;
  use crate::config::Config;
  use crate::metrics::DrainStateValue;
  use crate::reload::{ReloadOutcome, ReloadReport, ReloadTrigger};
  use arc_swap::{ArcSwap, ArcSwapOption};
  use std::path::PathBuf;
  use std::sync::atomic::AtomicUsize;
  use weir_status::{
      ALL, AdminEvidence, AdminReason, AdminState, BusyEvidence, ConditionCode, DrainEvidence, DrainExitCode, DrainStateCode,
      Evaluation, InFlightEvidence, NotReloadedEvidence, NotReloadedReason, RecentCounters, ReloadEvidence,
      ReloadItem, ReloadItemEvidence, ReloadItemOutcome, ReloadOutcomeCode, ReloadTriggerCode, RequeueEvidence,
      SegmentId, SinceLookup, SinkEvidence, SinkFailureKind, SinkHealthCode,
  };

  pub(crate) const DEPTH_UNPUBLISHED: usize = usize::MAX;
  pub(crate) const QUEUE_UNPUBLISHED: u64 = u64::MAX;
  const NO_UID: u64 = u64::MAX;

  // ── Drain ────────────────────────────────────────────────────────────────

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub(crate) struct InFlight {
      /// `wab::segment::segment_identity` of the segment.
      pub segment: String,
      pub attempt: u32,
      pub retries_left: u32,
      pub processed: u64,
      pub next_retry_at_ms: Option<u64>,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub(crate) struct RequeueResult {
      pub ticket: u64,
      pub requeued: u64,
      pub already_queued: u64,
      pub scan_error: Option<String>,
      pub backoff_cut: bool,
      pub coalesced: bool,
      pub at_ms: u64,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum DrainExitReason {
      Stopped,
      DeadlineAbandoned,
      GaveUp,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub(crate) struct DrainExitInfo {
      pub reason: DrainExitReason,
      pub abandoned: Option<(String, u64)>,
      pub pending_left: usize,
      pub backlog_left: usize,
      pub last_panic: Option<String>,
      pub at_ms: u64,
  }

  fn drain_code(v: &DrainStateValue) -> DrainStateCode {
      match v {
          DrainStateValue::draining => DrainStateCode::Draining,
          DrainStateValue::retrying_transient => DrainStateCode::RetryingTransient,
          DrainStateValue::blocked_dead_letter_full => DrainStateCode::BlockedDeadLetterFull,
          DrainStateValue::stopped => DrainStateCode::Stopped,
      }
  }

  fn drain_value(c: DrainStateCode) -> DrainStateValue {
      match c {
          DrainStateCode::Draining => DrainStateValue::draining,
          DrainStateCode::RetryingTransient => DrainStateValue::retrying_transient,
          DrainStateCode::BlockedDeadLetterFull => DrainStateValue::blocked_dead_letter_full,
          DrainStateCode::Stopped => DrainStateValue::stopped,
      }
  }

  /// Written only by the drain thread and its supervisor (PR 2), except
  /// `pause_requested` / `paused_by_uid`, which mirror DrainControl's intent
  /// (PR 2, PR 6). Unpublished until the first `set_state`: evidence is then
  /// `None` (unknown), never a healthy default.
  pub(crate) struct DrainCell {
      state: AtomicU8, // 0 = unpublished, else DrainStateCode::as_u8() + 1
      pub(crate) state_since_ms: AtomicU64,
      pub(crate) pause_requested: AtomicBool,
      pub(crate) paused_by_uid: AtomicU64,
      pub(crate) paused_since_ms: AtomicU64,
      pub(crate) held: AtomicBool,
      pub(crate) observed_requeue: AtomicU64,
      pub(crate) generation: AtomicU64,
      pub(crate) in_flight: ArcSwapOption<InFlight>,
      pub(crate) commit_in_flight_since_ms: AtomicU64,
      pub(crate) last_progress_ms: AtomicU64,
      pub(crate) pending_len: AtomicUsize,
      pub(crate) backlog_len: AtomicUsize,
      pub(crate) replay_total: AtomicU64,
      pub(crate) replay_remaining: AtomicU64,
      pub(crate) blocked_since_ms: AtomicU64,
      pub(crate) stopping: AtomicBool,
      pub(crate) deadline_ms: AtomicU64,
      pub(crate) last_requeue: ArcSwapOption<RequeueResult>,
      pub(crate) exit: ArcSwapOption<DrainExitInfo>,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 (the drain) and PR 6 (pause uid) write these
  impl DrainCell {
      pub(crate) fn new() -> Self {
          DrainCell {
              state: AtomicU8::new(0),
              state_since_ms: AtomicU64::new(NONE_MS),
              pause_requested: AtomicBool::new(false),
              paused_by_uid: AtomicU64::new(NO_UID),
              paused_since_ms: AtomicU64::new(NONE_MS),
              held: AtomicBool::new(false),
              observed_requeue: AtomicU64::new(0),
              generation: AtomicU64::new(0),
              in_flight: ArcSwapOption::empty(),
              commit_in_flight_since_ms: AtomicU64::new(NONE_MS),
              last_progress_ms: AtomicU64::new(NONE_MS),
              pending_len: AtomicUsize::new(0),
              backlog_len: AtomicUsize::new(0),
              replay_total: AtomicU64::new(0),
              replay_remaining: AtomicU64::new(0),
              blocked_since_ms: AtomicU64::new(NONE_MS),
              stopping: AtomicBool::new(false),
              deadline_ms: AtomicU64::new(NONE_MS),
              last_requeue: ArcSwapOption::empty(),
              exit: ArcSwapOption::empty(),
          }
      }
      /// Stamps `state_since_ms` only on a change.
      pub(crate) fn set_state(&self, state: DrainStateValue) {
          let enc = drain_code(&state).as_u8() + 1;
          if self.state.swap(enc, Ordering::AcqRel) != enc {
              self.state_since_ms.store(unix_ms_now(), Ordering::Relaxed);
          }
      }
      pub(crate) fn state(&self) -> Option<DrainStateValue> {
          let enc = self.state.load(Ordering::Acquire);
          if enc == 0 { None } else { DrainStateCode::from_u8(enc - 1).map(drain_value) }
      }
      pub(crate) fn set_pause_requested(&self, requested: bool, by_uid: Option<u32>) {
          self.paused_by_uid.store(by_uid.map_or(NO_UID, u64::from), Ordering::Relaxed);
          self.pause_requested.store(requested, Ordering::Release);
      }
      /// `paused_since_ms == 0` means "not paused".
      pub(crate) fn set_paused(&self, paused_since_ms: u64, held: bool) {
          self.held.store(held, Ordering::Relaxed);
          self.paused_since_ms.store(paused_since_ms, Ordering::Release);
      }
      pub(crate) fn set_generation(&self, generation: u64) {
          self.generation.store(generation, Ordering::Relaxed);
      }
      pub(crate) fn set_in_flight(&self, in_flight: Option<InFlight>) {
          self.in_flight.store(in_flight.map(Arc::new));
      }
      /// CommitGuard construction (PR 2): marks a commit in flight and stamps progress.
      pub(crate) fn commit_started(&self) {
          let now = unix_ms_now();
          self.last_progress_ms.store(now, Ordering::Relaxed);
          self.commit_in_flight_since_ms.store(now, Ordering::Release);
      }
      /// CommitGuard drop (PR 2): clears the marker on every path, unwinding included.
      pub(crate) fn commit_finished(&self) {
          self.commit_in_flight_since_ms.store(NONE_MS, Ordering::Release);
          self.last_progress_ms.store(unix_ms_now(), Ordering::Relaxed);
      }
      pub(crate) fn stamp_progress(&self) {
          self.last_progress_ms.store(unix_ms_now(), Ordering::Relaxed);
      }
      pub(crate) fn set_queue_lens(&self, pending: usize, backlog: usize) {
          self.pending_len.store(pending, Ordering::Relaxed);
          self.backlog_len.store(backlog, Ordering::Relaxed);
      }
      pub(crate) fn set_replay(&self, total: u64, remaining: u64) {
          self.replay_total.store(total, Ordering::Relaxed);
          self.replay_remaining.store(remaining, Ordering::Relaxed);
      }
      /// 0 clears.
      pub(crate) fn set_blocked_since(&self, unix_ms: u64) {
          self.blocked_since_ms.store(unix_ms, Ordering::Relaxed);
      }
      pub(crate) fn set_stopping(&self, deadline_unix_ms: Option<u64>) {
          self.deadline_ms.store(deadline_unix_ms.unwrap_or(NONE_MS), Ordering::Relaxed);
          self.stopping.store(true, Ordering::Release);
      }
      pub(crate) fn set_last_requeue(&self, result: RequeueResult) {
          self.last_requeue.store(Some(Arc::new(result)));
      }
      pub(crate) fn set_observed_requeue(&self, ticket: u64) {
          self.observed_requeue.store(ticket, Ordering::Release);
      }
      pub(crate) fn set_exit(&self, info: DrainExitInfo) {
          self.exit.store(Some(Arc::new(info)));
      }

      /// `last_progress` = max(heartbeat change, CommitGuard stamps) (spec §9.3 code 13).
      /// `busy` is the 5 s poll task's reading of `DrainHeartbeat::busy()`, as
      /// published on the board (`StatusBoard::drain_busy_*`).
      pub(crate) fn evidence(
          &self,
          heartbeat_age_secs: Option<f64>,
          heartbeat_changed_ms: Option<u64>,
          busy: Option<BusyEvidence>,
      ) -> Option<DrainEvidence> {
          let enc = self.state.load(Ordering::Acquire);
          if enc == 0 {
              return None;
          }
          let state = DrainStateCode::from_u8(enc - 1)?;
          let paused_since = self.paused_since_ms.load(Ordering::Acquire);
          let uid = self.paused_by_uid.load(Ordering::Relaxed);
          let exit = self.exit.load_full();
          let last_progress = opt_ms(self.last_progress_ms.load(Ordering::Relaxed)).max(heartbeat_changed_ms);
          Some(DrainEvidence {
              state,
              state_since_unix_ms: self.state_since_ms.load(Ordering::Relaxed),
              pause_requested: self.pause_requested.load(Ordering::Acquire),
              paused: paused_since != NONE_MS,
              held: self.held.load(Ordering::Relaxed),
              paused_since_unix_ms: opt_ms(paused_since),
              paused_by_uid: (uid != NO_UID).then(|| u32::try_from(uid).ok()).flatten(),
              generation: self.generation.load(Ordering::Relaxed),
              observed_requeue: self.observed_requeue.load(Ordering::Acquire),
              in_flight: self.in_flight.load_full().and_then(|i| {
                  Some(InFlightEvidence {
                      segment: SegmentId::parse(&i.segment)?,
                      attempt: i.attempt,
                      retries_left: i.retries_left,
                      processed_records: i.processed,
                      next_retry_at_unix_ms: i.next_retry_at_ms,
                  })
              }),
              commit_in_flight_since_unix_ms: opt_ms(self.commit_in_flight_since_ms.load(Ordering::Acquire)),
              busy,
              last_progress_unix_ms: last_progress,
              heartbeat_age_secs,
              pending_segments: self.pending_len.load(Ordering::Relaxed) as u64,
              backlog_segments: self.backlog_len.load(Ordering::Relaxed) as u64,
              replay_total: self.replay_total.load(Ordering::Relaxed),
              replay_remaining: self.replay_remaining.load(Ordering::Relaxed),
              blocked_since_unix_ms: opt_ms(self.blocked_since_ms.load(Ordering::Relaxed)),
              last_requeue: self.last_requeue.load_full().map(|r| RequeueEvidence {
                  ticket: r.ticket,
                  requeued: r.requeued,
                  already_queued: r.already_queued,
                  backoff_cut: r.backoff_cut,
                  coalesced: r.coalesced,
                  scan_error: r.scan_error.clone(),
                  at_unix_ms: r.at_ms,
              }),
              exit: match exit.as_deref().map(|i| i.reason) {
                  None => DrainExitCode::Running,
                  Some(DrainExitReason::GaveUp) => DrainExitCode::GaveUp,
                  Some(DrainExitReason::Stopped | DrainExitReason::DeadlineAbandoned) => DrainExitCode::Stopped,
              },
              last_panic: exit.as_deref().and_then(|i| i.last_panic.clone()),
          })
      }
  }

  // ── Sink ─────────────────────────────────────────────────────────────────

  /// Written only by the drain thread (PR 2). Strings arrive already
  /// redacted, stripped of control characters and capped (PR 2's
  /// `drain::publish::sanitize_admin_text`). Unpublished until the first write.
  pub(crate) struct SinkCell {
      published: AtomicBool,
      health: AtomicU8, // 0 = unknown, else SinkHealthCode::as_u8() + 1
      pub(crate) health_since_ms: AtomicU64,
      pub(crate) last_success_ms: AtomicU64,
      pub(crate) last_failure_ms: AtomicU64,
      last_failure_kind: AtomicU8, // 0 = none, else SinkFailureKind::as_u8() + 1
      pub(crate) consecutive_failures: AtomicU64,
      pub(crate) last_error: ArcSwapOption<String>,
      pub(crate) probe_reason: ArcSwapOption<String>,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 2 (the drain) writes these
  impl SinkCell {
      pub(crate) fn new() -> Self {
          SinkCell {
              published: AtomicBool::new(false),
              health: AtomicU8::new(0),
              health_since_ms: AtomicU64::new(NONE_MS),
              last_success_ms: AtomicU64::new(NONE_MS),
              last_failure_ms: AtomicU64::new(NONE_MS),
              last_failure_kind: AtomicU8::new(0),
              consecutive_failures: AtomicU64::new(0),
              last_error: ArcSwapOption::empty(),
              probe_reason: ArcSwapOption::empty(),
          }
      }
      pub(crate) fn set_health(&self, health: &crate::sink::SinkHealth, reason: Option<String>) {
          let code = match health {
              crate::sink::SinkHealth::Healthy => SinkHealthCode::Healthy,
              crate::sink::SinkHealth::Down(_) => SinkHealthCode::Down,
              // `SinkHealth` is #[non_exhaustive]: anything newer is partial health.
              _ => SinkHealthCode::Degraded,
          };
          let enc = code.as_u8() + 1;
          if self.health.swap(enc, Ordering::AcqRel) != enc {
              self.health_since_ms.store(unix_ms_now(), Ordering::Relaxed);
          }
          self.probe_reason.store(reason.map(Arc::new));
          self.published.store(true, Ordering::Release);
      }
      pub(crate) fn record_success(&self) {
          self.last_success_ms.store(unix_ms_now(), Ordering::Relaxed);
          self.consecutive_failures.store(0, Ordering::Relaxed);
          self.published.store(true, Ordering::Release);
      }
      pub(crate) fn record_failure(&self, kind: SinkFailureKind, message: String) {
          self.last_failure_ms.store(unix_ms_now(), Ordering::Relaxed);
          self.last_failure_kind.store(kind.as_u8() + 1, Ordering::Relaxed);
          self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
          self.last_error.store(Some(Arc::new(message)));
          self.published.store(true, Ordering::Release);
      }
      pub(crate) fn evidence(&self) -> Option<SinkEvidence> {
          if !self.published.load(Ordering::Acquire) {
              return None;
          }
          let health = self.health.load(Ordering::Acquire);
          let kind = self.last_failure_kind.load(Ordering::Relaxed);
          Some(SinkEvidence {
              health: if health == 0 { None } else { SinkHealthCode::from_u8(health - 1) },
              health_since_unix_ms: opt_ms(self.health_since_ms.load(Ordering::Relaxed)),
              last_success_unix_ms: opt_ms(self.last_success_ms.load(Ordering::Relaxed)),
              last_failure_unix_ms: opt_ms(self.last_failure_ms.load(Ordering::Relaxed)),
              last_failure_kind: if kind == 0 { None } else { SinkFailureKind::from_u8(kind - 1) },
              consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
              last_error: self.last_error.load_full().map(|s| (*s).clone()),
              probe_reason: self.probe_reason.load_full().map(|s| (*s).clone()),
          })
      }
  }

  // ── WAB (written only by the poll task) ──────────────────────────────────

  #[derive(Clone, Debug, Default, PartialEq, Eq)]
  pub(crate) struct LagReport {
      pub undelivered_segments: u64,
      pub undelivered_records: u64,
      /// The oldest undelivered segment's header `created_at`, unix ms.
      pub oldest_created_ms: Option<u64>,
      /// `(shard, created_at unix ms)` of each active `.wab`.
      pub active_created_ms: Vec<(u16, u64)>,
      pub dead_letter_bytes: u64,
      pub dead_letter_files: u64,
      pub quarantine_bytes: u64,
      pub quarantine_files: u64,
      pub scanned_at_ms: u64,
  }

  pub(crate) struct WabCell {
      lag: ArcSwapOption<LagReport>,
      first_scan_ok_ms: AtomicU64,
      last_scan_ok_ms: AtomicU64,
      growth: AtomicU8, // 0 unknown, 1 no, 2 yes
  }

  impl WabCell {
      pub(crate) fn new() -> Self {
          WabCell {
              lag: ArcSwapOption::empty(),
              first_scan_ok_ms: AtomicU64::new(NONE_MS),
              last_scan_ok_ms: AtomicU64::new(NONE_MS),
              growth: AtomicU8::new(0),
          }
      }
      pub(crate) fn note_scan_ok(&self, now_ms: u64) {
          let _ = self.first_scan_ok_ms.compare_exchange(NONE_MS, now_ms, Ordering::Relaxed, Ordering::Relaxed);
          self.last_scan_ok_ms.store(now_ms, Ordering::Release);
      }
      pub(crate) fn first_scan_ok_ms(&self) -> Option<u64> {
          opt_ms(self.first_scan_ok_ms.load(Ordering::Relaxed))
      }
      pub(crate) fn last_scan_ok_ms(&self) -> Option<u64> {
          opt_ms(self.last_scan_ok_ms.load(Ordering::Acquire))
      }
      pub(crate) fn set_lag(&self, lag: LagReport) {
          self.lag.store(Some(Arc::new(lag)));
      }
      pub(crate) fn lag(&self) -> Option<Arc<LagReport>> {
          self.lag.load_full()
      }
      pub(crate) fn set_growth(&self, growing: bool) {
          self.growth.store(if growing { 2 } else { 1 }, Ordering::Relaxed);
      }
      pub(crate) fn growth(&self) -> Option<bool> {
          match self.growth.load(Ordering::Relaxed) {
              0 => None,
              v => Some(v == 2),
          }
      }
  }

  // ── Admin and reload (PR 6 writes) ───────────────────────────────────────

  pub(crate) struct AdminCell {
      enabled: AtomicBool,
      state: AtomicU8,
      reason: AtomicU8, // u8::MAX = none
      path: ArcSwapOption<String>,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 6 (admin socket) writes these
  impl AdminCell {
      pub(crate) fn new() -> Self {
          AdminCell {
              enabled: AtomicBool::new(false),
              state: AtomicU8::new(AdminState::Disabled.as_u8()),
              reason: AtomicU8::new(u8::MAX),
              path: ArcSwapOption::empty(),
          }
      }
      pub(crate) fn set_bound(&self, path: String) {
          self.path.store(Some(Arc::new(path)));
          self.reason.store(u8::MAX, Ordering::Relaxed);
          self.enabled.store(true, Ordering::Relaxed);
          self.state.store(AdminState::Bound.as_u8(), Ordering::Release);
      }
      pub(crate) fn set_unavailable(&self, reason: AdminReason) {
          self.reason.store(reason.as_u8(), Ordering::Relaxed);
          self.enabled.store(true, Ordering::Relaxed);
          self.state.store(AdminState::Unavailable.as_u8(), Ordering::Release);
      }
      pub(crate) fn set_disabled(&self) {
          self.enabled.store(false, Ordering::Relaxed);
          self.state.store(AdminState::Disabled.as_u8(), Ordering::Release);
      }
      pub(crate) fn evidence(&self) -> AdminEvidence {
          AdminEvidence {
              enabled: self.enabled.load(Ordering::Relaxed),
              state: AdminState::from_u8(self.state.load(Ordering::Acquire)).unwrap_or(AdminState::Disabled),
              reason: AdminReason::from_u8(self.reason.load(Ordering::Relaxed)),
              path: self.path.load_full().map(|p| (*p).clone()),
          }
      }
  }

  pub(crate) struct ReloadCell {
      last: ArcSwapOption<ReloadReport>,
      at_ms: AtomicU64,
      tls: AtomicU8, // 0 never attempted, 1 last attempt ok, 2 last attempt failed
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 6 (Reloader) publishes
  impl ReloadCell {
      pub(crate) fn new() -> Self {
          ReloadCell { last: ArcSwapOption::empty(), at_ms: AtomicU64::new(NONE_MS), tls: AtomicU8::new(0) }
      }
      /// `tls_reload_failed` is sticky until a success (spec §9.3 code 29):
      /// only a report that names "tls" moves the TLS state.
      pub(crate) fn publish(&self, report: ReloadReport, now_ms: u64) {
          if report.changed.contains(&"tls") {
              self.tls.store(1, Ordering::Relaxed);
          } else if report.rejected.iter().any(|(item, _)| *item == "tls") {
              self.tls.store(2, Ordering::Relaxed);
          }
          self.at_ms.store(now_ms, Ordering::Relaxed);
          self.last.store(Some(Arc::new(report)));
      }
      pub(crate) fn evidence(&self) -> Option<ReloadEvidence> {
          let r = self.last.load_full()?;
          let mut items: Vec<ReloadItemEvidence> = r
              .changed
              .iter()
              .filter(|i| **i == "tls")
              .map(|_| ReloadItemEvidence { item: ReloadItem::TlsCertificates, outcome: ReloadItemOutcome::Reloaded, detail: None })
              .collect();
          items.extend(r.rejected.iter().filter(|(i, _)| *i == "tls").map(|(_, why)| ReloadItemEvidence {
              item: ReloadItem::TlsCertificates,
              outcome: ReloadItemOutcome::Failed,
              detail: Some(why.clone()),
          }));
          Some(ReloadEvidence {
              trigger: match r.trigger {
                  ReloadTrigger::Sighup => ReloadTriggerCode::Sighup,
                  ReloadTrigger::Admin => ReloadTriggerCode::Admin,
              },
              at_unix_ms: self.at_ms.load(Ordering::Relaxed),
              outcome: match r.outcome {
                  ReloadOutcome::Ok => ReloadOutcomeCode::Applied,
                  ReloadOutcome::Noop => ReloadOutcomeCode::Noop,
                  ReloadOutcome::Failed => ReloadOutcomeCode::Failed,
              },
              tls_last_failed: self.tls.load(Ordering::Relaxed) == 2,
              items,
              not_reloaded: vec![NotReloadedEvidence { item: ReloadItem::ConfigFile, reason_code: NotReloadedReason::RestartRequired }],
          })
      }
  }

  // ── Condition latches (written only by the poll task) ────────────────────

  /// First-seen unix ms of each active condition, indexed like `weir_status::ALL`.
  pub(crate) struct ConditionLatches(Box<[AtomicU64]>);

  impl ConditionLatches {
      pub(crate) fn new() -> Self {
          ConditionLatches(ALL.iter().map(|_| AtomicU64::new(NONE_MS)).collect())
      }
      fn index(code: ConditionCode) -> Option<usize> {
          ALL.iter().position(|c| *c == code)
      }
      pub(crate) fn update(&self, eval: &Evaluation, now_ms: u64) {
          for (i, code) in ALL.iter().enumerate() {
              let cell = &self.0[i];
              if eval.is_active(*code) {
                  let _ = cell.compare_exchange(NONE_MS, now_ms.max(1), Ordering::Relaxed, Ordering::Relaxed);
              } else {
                  cell.store(NONE_MS, Ordering::Relaxed);
              }
          }
      }
  }

  impl SinceLookup for ConditionLatches {
      fn since(&self, code: ConditionCode) -> Option<u64> {
          Self::index(code).and_then(|i| opt_ms(self.0[i].load(Ordering::Relaxed)))
      }
  }

  // ── The board ────────────────────────────────────────────────────────────

  pub(crate) struct StaticInfo {
      pub version: &'static str,
      pub wire_version: u8,
      pub pid: u32,
      pub started_at_ms: u64,
      pub wab_dir: PathBuf,
      pub socket_path: PathBuf,
      pub wab_dir_dev: u64,
      pub wab_dir_ino: u64,
      pub config: Arc<ConfigView>,
  }

  impl StaticInfo {
      pub(crate) fn capture(config: &Config, view: Arc<ConfigView>) -> Self {
          #[cfg(unix)]
          let (dev, ino) = {
              use std::os::unix::fs::MetadataExt;
              std::fs::metadata(&config.wab_dir).map(|m| (m.dev(), m.ino())).unwrap_or((0, 0))
          };
          #[cfg(not(unix))]
          let (dev, ino) = (0, 0);
          StaticInfo {
              version: env!("CARGO_PKG_VERSION"),
              wire_version: weir_core::WIRE_VERSION,
              pid: std::process::id(),
              started_at_ms: unix_ms_now(),
              wab_dir: config.wab_dir.clone(),
              socket_path: config.socket_path.clone(),
              wab_dir_dev: dev,
              wab_dir_ino: ino,
              config: view,
          }
      }

      #[cfg(test)]
      pub(crate) fn for_test() -> Self {
          StaticInfo {
              version: env!("CARGO_PKG_VERSION"),
              wire_version: weir_core::WIRE_VERSION,
              pid: std::process::id(),
              started_at_ms: unix_ms_now(),
              wab_dir: PathBuf::from("/var/lib/weir/wab"),
              socket_path: PathBuf::from("/run/weir/weir.sock"),
              wab_dir_dev: 1,
              wab_dir_ino: 2,
              config: Arc::new(ConfigView::empty_for_test()),
          }
      }
  }

  /// The wait-free live state (spec §5.2). No channel handle of any kind:
  /// depth arrives as `len()` snapshots in `drain_depth` (PR 2/3 write it).
  pub(crate) struct StatusBoard {
      pub(crate) statik: StaticInfo,
      pub(crate) life: LifecycleCell,
      pub(crate) listeners: ListenerCells,
      pub(crate) shards: Arc<ShardRegistry>,
      pub(crate) drain: DrainCell,
      pub(crate) sink: SinkCell,
      pub(crate) wab: WabCell,
      pub(crate) admin: AdminCell,
      pub(crate) reload: ReloadCell,
      pub(crate) latches: ConditionLatches,
      pub(crate) recent: ArcSwap<RecentCounters>,
      /// `QueueSender::len()` from the 500 ms task; `QUEUE_UNPUBLISHED` before it.
      pub(crate) queue_depth: AtomicU64,
      /// Drain-channel `len()` snapshots; `DEPTH_UNPUBLISHED` until PR 3's writers.
      pub(crate) drain_depth: Arc<AtomicUsize>,
      /// When the depth rose to >= the high-water threshold (500 ms task); 0 = below.
      pub(crate) channel_high_since_ms: AtomicU64,
      /// When the drain heartbeat last moved (5 s poll task); 0 = unknown.
      pub(crate) heartbeat_changed_ms: AtomicU64,
      /// When the drain entered its current busy operation (5 s poll task, from
      /// `DrainHeartbeat::busy()`); 0 = between operations or unknown. Written
      /// after `drain_busy_sink_call`, so a reader that sees a non-zero stamp
      /// sees that tick's kind (or, at worst, the neighbouring operation's, the
      /// same tolerance `DrainHeartbeat::busy` documents).
      pub(crate) drain_busy_since_ms: AtomicU64,
      /// The busy operation is a sink call (`BusyKind::is_sink_call`).
      pub(crate) drain_busy_sink_call: AtomicBool,
      pub(crate) wab_cap_rejecting: Arc<AtomicBool>,
      pub(crate) wab_bytes_now: Arc<AtomicU64>,
  }

  impl StatusBoard {
      pub(crate) fn new(
          statik: StaticInfo,
          shard_count: usize,
          worker_count: usize,
          tcp_configured: bool,
          wab_cap_rejecting: Arc<AtomicBool>,
          wab_bytes_now: Arc<AtomicU64>,
      ) -> Arc<Self> {
          let life = LifecycleCell::new(statik.started_at_ms);
          let shards = Arc::new(ShardRegistry::new(shard_count, worker_count, life.shutting_down_flag()));
          Arc::new(StatusBoard {
              statik,
              life,
              listeners: ListenerCells::new(cfg!(unix), tcp_configured),
              shards,
              drain: DrainCell::new(),
              sink: SinkCell::new(),
              wab: WabCell::new(),
              admin: AdminCell::new(),
              reload: ReloadCell::new(),
              latches: ConditionLatches::new(),
              recent: ArcSwap::from_pointee(RecentCounters::default()),
              queue_depth: AtomicU64::new(QUEUE_UNPUBLISHED),
              drain_depth: Arc::new(AtomicUsize::new(DEPTH_UNPUBLISHED)),
              channel_high_since_ms: AtomicU64::new(NONE_MS),
              heartbeat_changed_ms: AtomicU64::new(NONE_MS),
              drain_busy_since_ms: AtomicU64::new(NONE_MS),
              drain_busy_sink_call: AtomicBool::new(false),
              wab_cap_rejecting,
              wab_bytes_now,
          })
      }

      #[cfg(test)]
      pub(crate) fn for_test() -> Arc<Self> {
          Self::for_test_with(1, 1)
      }

      #[cfg(test)]
      pub(crate) fn for_test_with(shards: usize, workers: usize) -> Arc<Self> {
          Self::new(
              StaticInfo::for_test(),
              shards,
              workers,
              false,
              Arc::new(AtomicBool::new(false)),
              Arc::new(AtomicU64::new(0)),
          )
      }
  }
  ```
  `status/mod.rs` gains these re-exports and the test helper. The `mod` lines from Tasks 1.12 and 1.15 and the `shards::` re-export from Task 1.12 stay:
  ```rust
  pub(crate) use board::StatusBoard;
  #[cfg_attr(not(test), allow(unused_imports))] // PR 2 imports these from crate::status
  pub(crate) use board::{DrainExitInfo, DrainExitReason, InFlight, RequeueResult};
  #[cfg_attr(not(test), allow(unused_imports))]
  pub(crate) use weir_status::SinkFailureKind;

  /// A board with `shard_count` shards in `Starting` and no worker running.
  #[cfg(test)]
  pub(crate) fn test_board(shard_count: usize, worker_count: usize) -> std::sync::Arc<StatusBoard> {
      StatusBoard::for_test_with(shard_count, worker_count)
  }
  ```
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server status:: -- --test-threads=1`. Expected: all status tests pass (6 new in board.rs, plus `no_blocking_locks_under_status`).
  - Verify the scan by mutation: add `use std::sync::Mutex as _M;` at the top of board.rs. `cargo test -p weir-server --bin weir-server no_blocking_locks -- --test-threads=1` must FAIL with `src/status/board.rs mentions Mutex`. Revert.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): the status board: drain, sink, WAB, admin and reload cells and condition latches"`

---

### Task 1.17: Rendering: `StatusBoard::evidence_at`, `StatusSnapshot`, `admin_doc`, and the concurrency stress test

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/status/render.rs`
- Modify: `crates/weir-server/src/status/mod.rs` (`pub(crate) mod render;` and `#[cfg_attr(not(test), allow(unused_imports))] pub(crate) use render::StatusSnapshot;`, since nothing outside status/ names it until PR 5 / PR 6; the lock scan walks the directory, so render.rs is scanned without an edit)
- Test: `crates/weir-server/src/status/render.rs` (`mod tests`)

**Interfaces:**
- Consumes: Task 1.16 board; `crate::wab::cap::low_water`; `weir_status::{Evidence, Evaluation, AdminStatusDoc, Thresholds, IdentityEvidence, …}`.
- Produces:
  ```rust
  impl StatusBoard {
      pub(crate) fn evidence_at(&self, now_ms: u64) -> Evidence;          // loads only
      pub(crate) fn snapshot(&self, now: std::time::SystemTime) -> StatusSnapshot;
      pub(crate) fn identity(&self) -> IdentityEvidence;
  }
  pub(crate) struct StatusSnapshot { /* Evidence + IdentityEvidence */ }
  impl StatusSnapshot {
      pub(crate) fn evidence(&self) -> Evidence;
      pub(crate) fn admin_doc(&self, eval: &Evaluation) -> AdminStatusDoc;  // livez_checks [] and Thresholds::DEFAULT
  }
  /// The ONE admin rendering: PR 5's `/status` (before `weir_status::public`) and
  /// PR 6's admin `status` verb both call it, so they cannot drift (spec §11:
  /// "same model as /status"; G2).
  pub(crate) fn render_admin_doc(board: &StatusBoard, thresholds: &Thresholds,
                                 livez_checks: &[LivezCheck], now: std::time::SystemTime) -> AdminStatusDoc;
  ```
  *(Integrator addition: PR 5 and PR 6 each planned their own renderer for this; it lives here so the two parallel PRs consume one function.)*

- [ ] **Step 1: Write the failing tests** (`render.rs`, test module only):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::metrics::DrainStateValue;
      use std::sync::atomic::Ordering;
      use weir_status::*;

      #[test]
      fn a_fresh_board_reports_initializing_and_leaves_unwritten_cells_unknown() {
          let b = crate::status::test_board(2, 1);
          let e = b.evidence_at(1_800_000_000_000);
          assert_eq!(e.source, Source::Daemon);
          assert_eq!(e.phase, Some(Phase::Initializing));
          assert_eq!(e.shutting_down, Some(false));
          assert_eq!(e.shards.as_ref().unwrap().len(), 2);
          assert!(e.drain.is_none(), "drain cell unpublished (PR 2)");
          assert!(e.sink.is_none(), "sink cell unpublished (PR 2)");
          assert!(e.channel.is_none(), "drain depth unpublished (PR 3)");
          assert!(e.backlog.is_none(), "no lag scan yet");
          assert_eq!(e.queue_depth, None);
          assert_eq!(e.wab_cap.unwrap().last_scan_ok_unix_ms, None);
          assert_eq!(e.config.as_ref().unwrap().sink_type, "noop");
          assert_eq!(e.identity.as_ref().unwrap().wab_dir, "/var/lib/weir/wab");
          let ev = evaluate(&e, &Thresholds::DEFAULT, &b.latches);
          for c in [ConditionCode::DrainBackpressure, ConditionCode::DrainStopped, ConditionCode::SinkDown,
                    ConditionCode::DeliveryStalled, ConditionCode::SegmentsStranded] {
              assert!(ev.unevaluated.contains(&c), "{c:?} must be unevaluated, not healthy");
          }
          assert!(ev.is_active(ConditionCode::Starting));
          assert!(ev.is_active(ConditionCode::NoopSink));
      }

      #[test]
      fn published_cells_become_evidence() {
          let b = crate::status::test_board(1, 1);
          b.drain.set_state(DrainStateValue::draining);
          b.sink.record_success();
          b.drain_depth.store(240, Ordering::Relaxed);
          b.channel_high_since_ms.store(5, Ordering::Relaxed);
          b.queue_depth.store(3, Ordering::Relaxed);
          b.wab.set_lag(crate::status::board::LagReport {
              undelivered_segments: 2, undelivered_records: 7, oldest_created_ms: Some(1_000),
              active_created_ms: vec![(0, 2_000)], dead_letter_bytes: 9, dead_letter_files: 1,
              quarantine_bytes: 0, quarantine_files: 0, scanned_at_ms: 3_000,
          });
          let e = b.evidence_at(10_000);
          assert!(e.drain.is_some() && e.sink.is_some());
          assert_eq!(e.channel, Some(ChannelEvidence { depth: 240, capacity: 256, high_since_unix_ms: Some(5) }));
          assert_eq!(e.queue_depth, Some(3));
          let backlog = e.backlog.unwrap();
          assert_eq!((backlog.undelivered_records, backlog.oldest_undelivered_age_secs), (7, Some(9.0)));
          assert_eq!(e.shards.unwrap()[0].active_segment_age_secs, Some(8.0));
      }

      #[test]
      fn admin_doc_renders_the_daemon_identity() {
          let b = crate::status::test_board(1, 1);
          let snap = b.snapshot(std::time::SystemTime::now());
          let ev = evaluate(&snap.evidence(), &Thresholds::DEFAULT, &b.latches);
          let doc = snap.admin_doc(&ev);
          assert_eq!(doc.view, View::Admin);
          assert_eq!(doc.instance.socket_path, "/run/weir/weir.sock");
          assert_eq!(doc.pid, std::process::id());
          assert!(!doc.verdict.ready, "a board that never served is not ready");
      }

      /// `/status` (PR 5) and the admin `status` verb (PR 6) render through this
      /// one function; its verdict is the same predicate `/readyz` uses (G2),
      /// and it carries the configured liveness checks.
      #[test]
      fn render_admin_doc_shares_readyz_s_predicate_and_carries_the_livez_checks() {
          // A serving board, so the verdict is exercised on both sides: a
          // never-served board is not ready either way, which would let a
          // hard-coded `ready: false` pass.
          let b = crate::status::test_board(1, 1);
          b.shards.mark_up(0);
          std::mem::forget(crate::status::WorkerGuard::enter(std::sync::Arc::clone(&b.shards), 0));
          b.listeners.unix.set(ListenerState::Bound);
          b.life.enter(Phase::Serving, crate::status::unix_ms_now());
          b.wab.note_scan_ok(crate::status::unix_ms_now());
          let now = std::time::SystemTime::now();

          let ev = b.snapshot(now).evidence();
          assert!(readiness(&ev).ok, "precondition: a serving board is ready: {:?}", readiness(&ev).failing);
          let doc = render_admin_doc(&b, &Thresholds::DEFAULT, &[LivezCheck::NoLiveShard], now);
          assert!(doc.verdict.ready);
          assert_eq!(doc.verdict.livez_checks, vec![LivezCheck::NoLiveShard]);
          assert_eq!(doc.view, View::Admin);

          b.wab_cap_rejecting.store(true, Ordering::Relaxed);
          let ev = b.snapshot(now).evidence();
          let doc = render_admin_doc(&b, &Thresholds::DEFAULT, &[LivezCheck::NoLiveShard], now);
          assert!(!readiness(&ev).ok);
          assert!(!doc.verdict.ready);
          assert_eq!(doc.verdict.readiness_failing, readiness(&ev).failing);
      }

      /// Spec §16 "concurrent writer/reader stress test": writers hammer every
      /// cell while readers build evidence. Nothing blocks, nothing panics,
      /// and readers see writes. Progress, not time, ends each loop: every
      /// thread does at least 1,000 iterations (readers: and has seen the
      /// drain publication), so a thread the scheduler starts late still
      /// passes; a 10 s deadline turns real starvation into a failure.
      #[test]
      fn concurrent_writers_and_readers_never_block_each_other() {
          let b = crate::status::test_board(4, 2);
          let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
          let mut threads = Vec::new();
          for t in 0..2 {
              let (b, stop) = (b.clone(), stop.clone());
              threads.push(std::thread::spawn(move || {
                  let mut i = 0u64;
                  while i < 1_000 || !stop.load(Ordering::Relaxed) {
                      i += 1;
                      if t == 0 {
                          b.drain.set_state(if i % 2 == 0 { DrainStateValue::draining } else { DrainStateValue::retrying_transient });
                          b.drain.commit_started();
                          b.drain.commit_finished();
                          b.sink.record_failure(SinkFailureKind::Transient, format!("e{i}"));
                      } else {
                          b.shards.mark_up((i % 4) as usize);
                          b.shards.mark_respawning(((i + 1) % 4) as usize);
                          b.drain_depth.store((i % 256) as usize, Ordering::Relaxed);
                          b.life.enter(Phase::Serving, i);
                      }
                  }
                  i
              }));
          }
          let readers: Vec<_> = (0..2)
              .map(|_| {
                  let (b, stop) = (b.clone(), stop.clone());
                  std::thread::spawn(move || {
                      let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                      let (mut saw_drain, mut reads) = (false, 0u64);
                      loop {
                          let e = b.evidence_at(super::super::unix_ms_now());
                          let _ = evaluate(&e, &Thresholds::DEFAULT, &b.latches);
                          saw_drain |= e.drain.is_some();
                          reads += 1;
                          if saw_drain && reads >= 1_000 && stop.load(Ordering::Relaxed) {
                              break saw_drain;
                          }
                          assert!(std::time::Instant::now() < deadline, "reader starved for 10 s");
                      }
                  })
              })
              .collect();
          // Minimum overlap only; correctness no longer depends on it.
          std::thread::sleep(std::time::Duration::from_millis(200));
          stop.store(true, Ordering::Relaxed);
          for t in threads {
              assert!(t.join().unwrap() > 0, "every writer made progress");
          }
          for r in readers {
              assert!(r.join().unwrap(), "readers observed the writers' publications");
          }
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::render -- --test-threads=1`. Expected: `no method named `evidence_at` found for struct `Arc<StatusBoard>``.
- [ ] **Step 3: Implement** (`render.rs`, above the tests):
  ```rust
  //! Board → `Evidence` → document (spec §5.2 `snapshot`, `evidence`,
  //! `admin_doc`). Loads only: no I/O, no lock. Disk facts are at most one
  //! poll interval (5 s) old (spec §9.2 rule 8).
  use super::board::{DEPTH_UNPUBLISHED, QUEUE_UNPUBLISHED, StatusBoard};
  use std::sync::atomic::Ordering;
  use std::time::{SystemTime, UNIX_EPOCH};
  use weir_status::{
      AdminStatusDoc, BacklogEvidence, BusyEvidence, CapEvidence, ChannelEvidence, Evaluation, Evidence,
      IdentityEvidence, Source, Thresholds,
  };

  pub(crate) struct StatusSnapshot {
      evidence: Evidence,
      identity: IdentityEvidence,
  }

  #[cfg_attr(not(test), allow(dead_code))] // PR 5 / PR 6 call these
  impl StatusSnapshot {
      pub(crate) fn evidence(&self) -> Evidence {
          self.evidence.clone()
      }
      /// The admin document with no liveness checks selected and default
      /// thresholds; PR 5's router overwrites the three liveness fields.
      pub(crate) fn admin_doc(&self, eval: &Evaluation) -> AdminStatusDoc {
          AdminStatusDoc::build(&self.identity, &self.evidence, eval, &Thresholds::DEFAULT, &[])
      }
  }

  /// One snapshot, one evaluation, the configured liveness checks. Loads only.
  /// PR 5 (`/status`) and PR 6 (admin `status`) are its callers; until they
  /// land only the tests call it.
  #[cfg_attr(not(test), allow(dead_code, reason = "PR 5 and PR 6 call it"))]
  pub(crate) fn render_admin_doc(
      board: &StatusBoard,
      thresholds: &Thresholds,
      livez_checks: &[weir_status::LivezCheck],
      now: SystemTime,
  ) -> AdminStatusDoc {
      let snap = board.snapshot(now);
      let eval = weir_status::evaluate(&snap.evidence, thresholds, &board.latches);
      AdminStatusDoc::build(&snap.identity, &snap.evidence, &eval, thresholds, livez_checks)
  }

  fn opt(v: u64) -> Option<u64> {
      (v != 0).then_some(v)
  }

  impl StatusBoard {
      pub(crate) fn identity(&self) -> IdentityEvidence {
          let s = &self.statik;
          IdentityEvidence {
              version: s.version.to_string(),
              wire_version: s.wire_version,
              pid: s.pid,
              started_at_unix_ms: s.started_at_ms,
              wab_dir: s.wab_dir.display().to_string(),
              socket_path: s.socket_path.display().to_string(),
              wab_dir_dev: s.wab_dir_dev,
              wab_dir_ino: s.wab_dir_ino,
          }
      }

      pub(crate) fn snapshot(&self, now: SystemTime) -> StatusSnapshot {
          let now_ms = now
              .duration_since(UNIX_EPOCH)
              .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
              .unwrap_or(0);
          StatusSnapshot { evidence: self.evidence_at(now_ms), identity: self.identity() }
      }

      pub(crate) fn evidence_at(&self, now_ms: u64) -> Evidence {
          let life = &self.life;
          let lag = self.wab.lag();
          let mut shards = self.shards.shard_evidence();
          if let Some(lag) = &lag {
              for &(id, created) in &lag.active_created_ms {
                  if let Some(s) = shards.get_mut(usize::from(id)) {
                      s.active_segment_age_secs = Some(now_ms.saturating_sub(created) as f64 / 1000.0);
                  }
              }
          }
          let config = self.statik.config.evidence();
          let max = config.public.wab_max_bytes.unwrap_or(0);
          let heartbeat_changed = opt(self.heartbeat_changed_ms.load(Ordering::Relaxed));
          let heartbeat_age = heartbeat_changed.map(|t| now_ms.saturating_sub(t) as f64 / 1000.0);
          let busy = opt(self.drain_busy_since_ms.load(Ordering::Acquire)).map(|since_unix_ms| BusyEvidence {
              sink_call: self.drain_busy_sink_call.load(Ordering::Relaxed),
              since_unix_ms,
          });
          let depth = self.drain_depth.load(Ordering::Relaxed);
          let queue = self.queue_depth.load(Ordering::Relaxed);
          Evidence {
              now_unix_ms: now_ms,
              source: Source::Daemon,
              identity: Some(self.identity()),
              started_at_unix_ms: Some(self.statik.started_at_ms),
              phase: Some(life.phase()),
              phase_since_unix_ms: Some(life.phase_since_ms()),
              ever_served: Some(life.ever_served()),
              shutting_down: Some(life.is_shutting_down()),
              shutting_down_since_unix_ms: life.shutting_down_since_ms(),
              forced: Some(life.forced()),
              drain_deadline_unix_ms: life.drain_deadline_ms(),
              drain_budget_secs: life.drain_budget_secs(),
              listeners: Some(self.listeners.evidence()),
              shards: Some(shards),
              workers: Some(self.shards.worker_evidence()),
              queue_depth: (queue != QUEUE_UNPUBLISHED).then_some(queue),
              wab_cap: Some(CapEvidence {
                  max_bytes: max,
                  low_water_bytes: crate::wab::cap::low_water(max),
                  bytes_now: self.wab_bytes_now.load(Ordering::Relaxed),
                  rejecting: self.wab_cap_rejecting.load(Ordering::Relaxed),
                  first_scan_ok_unix_ms: self.wab.first_scan_ok_ms(),
                  last_scan_ok_unix_ms: self.wab.last_scan_ok_ms(),
                  growing_unbounded: self.wab.growth(),
              }),
              channel: (depth != DEPTH_UNPUBLISHED).then(|| ChannelEvidence {
                  depth: depth as u64,
                  capacity: weir_status::CHANNEL_CAPACITY,
                  high_since_unix_ms: opt(self.channel_high_since_ms.load(Ordering::Relaxed)),
              }),
              drain: self.drain.evidence(heartbeat_age, heartbeat_changed, busy),
              sink: self.sink.evidence(),
              backlog: lag.as_ref().map(|l| BacklogEvidence {
                  undelivered_segments: l.undelivered_segments,
                  undelivered_records: l.undelivered_records,
                  oldest_undelivered_age_secs: l.oldest_created_ms.map(|c| now_ms.saturating_sub(c) as f64 / 1000.0),
                  dead_letter_bytes: l.dead_letter_bytes,
                  dead_letter_max_bytes: config.public.dead_letter_max_bytes,
                  quarantine_bytes: l.quarantine_bytes,
                  quarantine_files: l.quarantine_files,
                  scanned_at_ms: l.scanned_at_ms,
              }),
              recent: Some((*self.recent.load_full()).clone()),
              reload: self.reload.evidence(),
              admin: Some(self.admin.evidence()),
              config: Some(config),
          }
      }
  }
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server status:: -- --test-threads=1`. Expected: all pass (5 new), and `no_blocking_locks_under_status` (which now also reads render.rs) still passes.
- [ ] **Step 5: Commit.** `git add crates/weir-server/src/status && git commit -m "feat(status): render the board as evidence and an admin document"`

---
### Task 1.18: Additive metrics, pre-created label sets, phase publication, and their monitoring rows

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs`:
  - label types after :296;
  - struct fields after `drain_busy_seconds` (:518);
  - registrations before `let metrics = Metrics {` in `new()`;
  - pre-creation before `(metrics, reg)` (:1132);
  - new `impl Metrics` methods.
- Modify: `crates/weir-server/src/status/board.rs`: `StatusBoard::enter_phase`, `StatusBoard::begin_shutdown`.
- Modify: `docs/monitoring.md`: a new `### Status, lifecycle and conditions` table inside `## Metric reference`, placed before the weir-attest section.
- Modify: `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`, the `expected` vec ending at :873): append the 34 PR 1 families.
- Test: `crates/weir-server/src/metrics/mod.rs` (`mod tests`), `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`), `crates/weir-server/tests/docs_drift.rs` (existing guard, unchanged)

**Interfaces:**
- Consumes: `weir_status::{CATALOGUE, Phase, Severity, SinkFailureKind}`, `crate::config::Config`.
- Produces (fields follow the existing "metric name without `weir_` and `_total`" convention, and are the names PR 2 and PR 3 consume):
  ```rust
  // label sets
  pub struct BuildInfoLabel { pub version: String, pub wire_version: String }
  pub enum StartupPhaseValue { initializing, recovering, handoff, binding, serving, shutting_down }
  pub struct StartupPhaseLabel { pub phase: StartupPhaseValue }
  pub struct ConditionLabel { pub code: String, pub severity: String }
  pub struct ShardLabel { pub shard: String }
  pub struct WorkerLabel { pub worker: String }
  pub enum DrainQueue { live, backlog }            pub struct DrainQueueLabel { pub queue: DrainQueue }
  pub enum SinkFailureValue { transient, permanent, timeout, contract_violation }
  pub struct SinkFailureLabel { pub kind: SinkFailureValue }
  pub enum NackInternalCause { queue_full, write_failed, sender_dropped, ack_timeout, wab_cap, coordinate, ownership_lost }
  pub struct NackInternalLabel { pub cause: NackInternalCause }
  pub enum NotHandedOffReason { budget, disconnected }
  pub struct NotHandedOffLabel { pub reason: NotHandedOffReason }
  impl StartupPhaseValue { pub fn of(p: Phase) -> Self; pub const ALL: [Self; 6]; }
  impl ConditionLabel { pub fn of(e: &weir_status::CatalogueEntry) -> Self; }
  impl ShardLabel { pub fn of(id: usize) -> Self; }  impl WorkerLabel { pub fn of(id: usize) -> Self; }
  impl SinkFailureValue { pub fn of(k: SinkFailureKind) -> Self; }
  // Metrics fields (all registered and pre-created)
  pub build_info: Family<BuildInfoLabel, Gauge<f64, AtomicU64>>,
  pub start_time_seconds, ready, live, wab_cap_rejecting, wab_undelivered_segments, wab_undelivered_records,
      wab_oldest_undelivered_age_seconds, drain_replay_remaining_segments, drain_heartbeat_age_seconds,
      drain_commit_in_flight_seconds, drain_paused, drain_generation, sink_last_success_timestamp_seconds,
      sink_consecutive_failures, config_wab_max_bytes, config_dead_letter_max_bytes, config_wab_segment_max_bytes,
      config_segment_max_age_seconds, config_segment_max_lifetime_seconds, config_sink_max_retries,
      config_shutdown_timeout_seconds: Gauge<f64, AtomicU64>,
  pub startup_phase: Family<StartupPhaseLabel, Gauge<f64, AtomicU64>>,
  pub condition_active: Family<ConditionLabel, Gauge<f64, AtomicU64>>,
  pub wab_shard_up, wab_handoff_blocked_seconds, wab_active_segment_age_seconds: Family<ShardLabel, Gauge<f64, AtomicU64>>,
  pub worker_up: Family<WorkerLabel, Gauge<f64, AtomicU64>>,
  pub drain_pending_segments: Family<DrainQueueLabel, Gauge<f64, AtomicU64>>,
  pub sink_last_failure_timestamp_seconds: Family<SinkFailureLabel, Gauge<f64, AtomicU64>>,
  pub records_nack_internal: Family<NackInternalLabel, Counter<u64, AtomicU64>>,
  pub connections_refused_no_live_shard, drain_segments_abandoned: Counter<u64, AtomicU64>,
  pub wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>,   // binding Q7
  impl Metrics {
      pub(crate) fn init_topology(&self, shard_count: usize, worker_count: usize);
      pub(crate) fn wab_segments_not_handed_off_total(&self) -> u64;   // budget + disconnected
      pub(crate) fn publish_build_info(&self, version: &str, wire_version: u8);
      pub(crate) fn publish_config(&self, config: &Config);
      pub(crate) fn publish_phase(&self, phase: Phase);
  }
  impl StatusBoard {
      pub(crate) fn enter_phase(&self, phase: Phase, metrics: &Metrics) -> bool;
      pub(crate) fn begin_shutdown(&self, metrics: &Metrics) -> bool;
  }
  ```
  `weir_drain_channel_depth`/`_capacity` (PR 3), `weir_drain_shutdown_deadline_seconds` and `weir_config_drain_shutdown_budget_seconds` (PR 4), `weir_http_requests_dropped_total` (PR 5), and `weir_admin_socket_up` / `weir_admin_requests_total` (PR 6) are registered by the PR that writes them.

- [ ] **Step 1: Write the failing tests** (append to `metrics::tests`):
  ```rust
  #[test]
  fn foundations_families_exist_from_boot_with_their_label_sets() {
      let (m, reg) = Metrics::new();
      m.init_topology(2, 1);
      let out = encode_to_string(&reg);
      for series in [
          "weir_startup_phase{phase=\"initializing\"} 0",
          "weir_startup_phase{phase=\"shutting_down\"} 0",
          "weir_ready 0",
          "weir_live 0",
          "weir_condition_active{code=\"starting\",severity=\"info\"} 0",
          "weir_condition_active{code=\"admin_unavailable\",severity=\"warning\"} 0",
          "weir_wab_shard_up{shard=\"1\"} 0",
          "weir_wab_handoff_blocked_seconds{shard=\"0\"} 0",
          "weir_wab_active_segment_age_seconds{shard=\"1\"} 0",
          "weir_worker_up{worker=\"0\"} 0",
          "weir_wab_cap_rejecting 0",
          "weir_drain_pending_segments{queue=\"live\"} 0",
          "weir_drain_pending_segments{queue=\"backlog\"} 0",
          "weir_sink_last_failure_timestamp_seconds{kind=\"contract_violation\"} 0",
          "weir_records_nack_internal_total{cause=\"queue_full\"} 0",
          "weir_records_nack_internal_total{cause=\"ownership_lost\"} 0",
          "weir_connections_refused_no_live_shard_total 0",
          "weir_drain_segments_abandoned_total 0",
          "weir_wab_segments_not_handed_off_total{reason=\"budget\"} 0",
          "weir_wab_segments_not_handed_off_total{reason=\"disconnected\"} 0",
      ] {
          assert!(out.contains(series), "{series:?} missing:\n{out}");
      }
      assert_eq!(out.matches("weir_condition_active{").count(), 30, "one series per daemon code");
      assert_eq!(m.wab_segments_not_handed_off_total(), 0);
  }

  #[test]
  fn build_info_config_and_phase_publish_once_and_one_hot() {
      let (m, reg) = Metrics::new();
      m.publish_build_info("4.1.0", 1);
      m.publish_phase(weir_status::Phase::Recovering);
      m.publish_phase(weir_status::Phase::Serving);
      let out = encode_to_string(&reg);
      assert!(out.contains("weir_build_info{version=\"4.1.0\",wire_version=\"1\"} 1"), "{out}");
      assert!(out.contains("weir_startup_phase{phase=\"serving\"} 1"));
      assert!(out.contains("weir_startup_phase{phase=\"recovering\"} 0"));
      let start: f64 = out
          .lines()
          .find_map(|l| l.strip_prefix("weir_start_time_seconds "))
          .expect("start time series")
          .parse()
          .unwrap();
      assert!(start > 1_700_000_000.0, "start time is stamped: {start}");
  }
  ```
  In `all_metric_names_present_in_output`, append all 34 base names to `expected`, after `"weir_drain_busy_seconds",` (that test asserts the number of `# HELP` families equals `expected.len()`, metrics/mod.rs:1246-1255, so every family this task registers must be named):
  ```rust
            "weir_build_info",
            "weir_start_time_seconds",
            "weir_startup_phase",
            "weir_ready",
            "weir_live",
            "weir_condition_active",
            "weir_wab_shard_up",
            "weir_wab_handoff_blocked_seconds",
            "weir_worker_up",
            "weir_wab_cap_rejecting",
            "weir_wab_undelivered_segments",
            "weir_wab_undelivered_records",
            "weir_wab_oldest_undelivered_age_seconds",
            "weir_wab_active_segment_age_seconds",
            "weir_drain_pending_segments",
            "weir_drain_replay_remaining_segments",
            "weir_drain_heartbeat_age_seconds",
            "weir_drain_commit_in_flight_seconds",
            "weir_drain_paused",
            "weir_drain_generation",
            "weir_sink_last_success_timestamp_seconds",
            "weir_sink_last_failure_timestamp_seconds",
            "weir_sink_consecutive_failures",
            "weir_config_wab_max_bytes",
            "weir_config_dead_letter_max_bytes",
            "weir_config_wab_segment_max_bytes",
            "weir_config_segment_max_age_seconds",
            "weir_config_segment_max_lifetime_seconds",
            "weir_config_sink_max_retries",
            "weir_config_shutdown_timeout_seconds",
            "weir_records_nack_internal",
            "weir_connections_refused_no_live_shard",
            "weir_drain_segments_abandoned",
            "weir_wab_segments_not_handed_off",
  ```
  In `crates/weir-server/tests/system.rs::metrics_all_families_registered`, append the same 34 names to its `expected` vec after `"weir_drain_busy_seconds",` (system.rs:873). That test also asserts that the count of `# HELP weir_` lines equals `expected.len()` (system.rs:895-907), and PR 1's merge gate runs `--test system`:
  ```rust
        // Foundations PR 1 (status, lifecycle and conditions)
        "weir_build_info",
        "weir_start_time_seconds",
        "weir_startup_phase",
        "weir_ready",
        "weir_live",
        "weir_condition_active",
        "weir_wab_shard_up",
        "weir_wab_handoff_blocked_seconds",
        "weir_worker_up",
        "weir_wab_cap_rejecting",
        "weir_wab_undelivered_segments",
        "weir_wab_undelivered_records",
        "weir_wab_oldest_undelivered_age_seconds",
        "weir_wab_active_segment_age_seconds",
        "weir_drain_pending_segments",
        "weir_drain_replay_remaining_segments",
        "weir_drain_heartbeat_age_seconds",
        "weir_drain_commit_in_flight_seconds",
        "weir_drain_paused",
        "weir_drain_generation",
        "weir_sink_last_success_timestamp_seconds",
        "weir_sink_last_failure_timestamp_seconds",
        "weir_sink_consecutive_failures",
        "weir_config_wab_max_bytes",
        "weir_config_dead_letter_max_bytes",
        "weir_config_wab_segment_max_bytes",
        "weir_config_segment_max_age_seconds",
        "weir_config_segment_max_lifetime_seconds",
        "weir_config_sink_max_retries",
        "weir_config_shutdown_timeout_seconds",
        "weir_records_nack_internal",
        "weir_connections_refused_no_live_shard",
        "weir_drain_segments_abandoned",
        "weir_wab_segments_not_handed_off",
  ```
  PRs 3, 4, 5 and 6 append the families they register (`weir_drain_channel_depth` / `_capacity`, `weir_config_drain_shutdown_budget_seconds` / `weir_drain_shutdown_deadline_seconds`, `weir_http_requests_dropped`, `weir_admin_requests` / `weir_admin_socket_up`) to both `expected` vecs in the same way.

  In `crates/weir-server/src/status/board.rs` tests:
  ```rust
  #[test]
  fn entering_a_phase_publishes_the_one_hot_gauge_and_shutdown_marks_shutting_down() {
      let (m, reg) = crate::metrics::Metrics::new();
      let b = StatusBoard::for_test();
      assert!(b.enter_phase(Phase::Binding, &m));
      assert!(b.begin_shutdown(&m));
      assert!(!b.begin_shutdown(&m));
      let mut out = String::new();
      prometheus_client::encoding::text::encode(&mut out, &reg).unwrap();
      assert!(out.contains("weir_startup_phase{phase=\"shutting_down\"} 1"), "{out}");
      assert!(out.contains("weir_startup_phase{phase=\"binding\"} 0"));
      assert!(!b.enter_phase(Phase::Serving, &m));
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server -- --test-threads=1 metrics:: status::board`. Expected: `no method named `init_topology``, `no method named `enter_phase``.
- [ ] **Step 3: Implement.** Label types (metrics/mod.rs, after `ConfigReloadOutcome`):
  ```rust
  // ── Foundations: status, lifecycle and conditions (spec §13) ────────────

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct BuildInfoLabel {
      pub version: String,
      pub wire_version: String,
  }

  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum StartupPhaseValue {
      initializing,
      recovering,
      handoff,
      binding,
      serving,
      shutting_down,
  }

  impl StartupPhaseValue {
      pub const ALL: [StartupPhaseValue; 6] = [
          Self::initializing, Self::recovering, Self::handoff, Self::binding, Self::serving, Self::shutting_down,
      ];
      pub fn of(p: weir_status::Phase) -> Self {
          use weir_status::Phase;
          match p {
              Phase::Initializing => Self::initializing,
              Phase::Recovering => Self::recovering,
              Phase::Handoff => Self::handoff,
              Phase::Binding => Self::binding,
              Phase::Serving => Self::serving,
              Phase::ShuttingDown => Self::shutting_down,
          }
      }
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct StartupPhaseLabel {
      pub phase: StartupPhaseValue,
  }

  /// `weir_condition_active{code,severity}`: one pre-created series per
  /// catalogue code. Strings because the code set lives in weir-status.
  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct ConditionLabel {
      pub code: String,
      pub severity: String,
  }

  impl ConditionLabel {
      pub fn of(e: &weir_status::CatalogueEntry) -> Self {
          let severity = match e.severity {
              weir_status::Severity::Info => "info",
              weir_status::Severity::Warning => "warning",
              weir_status::Severity::Critical => "critical",
          };
          ConditionLabel { code: e.code.as_str().to_string(), severity: severity.to_string() }
      }
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct ShardLabel {
      pub shard: String,
  }

  impl ShardLabel {
      pub fn of(id: usize) -> Self {
          ShardLabel { shard: id.to_string() }
      }
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct WorkerLabel {
      pub worker: String,
  }

  impl WorkerLabel {
      pub fn of(id: usize) -> Self {
          WorkerLabel { worker: id.to_string() }
      }
  }

  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum DrainQueue {
      live,
      backlog,
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct DrainQueueLabel {
      pub queue: DrainQueue,
  }

  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum SinkFailureValue {
      transient,
      permanent,
      timeout,
      contract_violation,
  }

  impl SinkFailureValue {
      pub const ALL: [SinkFailureValue; 4] = [Self::transient, Self::permanent, Self::timeout, Self::contract_violation];
      pub fn of(k: weir_status::SinkFailureKind) -> Self {
          use weir_status::SinkFailureKind as K;
          match k {
              K::Transient => Self::transient,
              K::Permanent => Self::permanent,
              K::Timeout => Self::timeout,
              K::ContractViolation => Self::contract_violation,
          }
      }
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct SinkFailureLabel {
      pub kind: SinkFailureValue,
  }

  /// Why a transient `InternalError` nack went out (spec §5.13). The wire
  /// reason is the same for all of them; only this label tells them apart.
  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum NackInternalCause {
      queue_full,
      write_failed,
      sender_dropped,
      ack_timeout,
      wab_cap,
      coordinate,
      ownership_lost,
  }

  impl NackInternalCause {
      pub const ALL: [NackInternalCause; 7] = [
          Self::queue_full, Self::write_failed, Self::sender_dropped, Self::ack_timeout,
          Self::wab_cap, Self::coordinate, Self::ownership_lost,
      ];
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct NackInternalLabel {
      pub cause: NackInternalCause,
  }

  /// Why a flusher could not hand a sealed segment to the drain (binding
  /// decision Q7): the shutdown hand-off budget ran out, or the drain was gone.
  #[allow(non_camel_case_types)]
  #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
  pub enum NotHandedOffReason {
      budget,
      disconnected,
  }

  impl NotHandedOffReason {
      pub const ALL: [NotHandedOffReason; 2] = [Self::budget, Self::disconnected];
  }

  #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
  pub struct NotHandedOffLabel {
      pub reason: NotHandedOffReason,
  }
  ```
  Struct fields (after `drain_busy_seconds`):
  ```rust
      // ── Foundations: status, lifecycle and conditions (spec §13) ──────────
      pub build_info: Family<BuildInfoLabel, Gauge<f64, AtomicU64>>,
      pub start_time_seconds: Gauge<f64, AtomicU64>,
      pub startup_phase: Family<StartupPhaseLabel, Gauge<f64, AtomicU64>>,
      pub ready: Gauge<f64, AtomicU64>,
      pub live: Gauge<f64, AtomicU64>,
      pub condition_active: Family<ConditionLabel, Gauge<f64, AtomicU64>>,
      pub wab_shard_up: Family<ShardLabel, Gauge<f64, AtomicU64>>,
      pub wab_handoff_blocked_seconds: Family<ShardLabel, Gauge<f64, AtomicU64>>,
      pub worker_up: Family<WorkerLabel, Gauge<f64, AtomicU64>>,
      pub wab_cap_rejecting: Gauge<f64, AtomicU64>,
      pub wab_undelivered_segments: Gauge<f64, AtomicU64>,
      pub wab_undelivered_records: Gauge<f64, AtomicU64>,
      pub wab_oldest_undelivered_age_seconds: Gauge<f64, AtomicU64>,
      pub wab_active_segment_age_seconds: Family<ShardLabel, Gauge<f64, AtomicU64>>,
      pub drain_pending_segments: Family<DrainQueueLabel, Gauge<f64, AtomicU64>>,
      pub drain_replay_remaining_segments: Gauge<f64, AtomicU64>,
      pub drain_heartbeat_age_seconds: Gauge<f64, AtomicU64>,
      pub drain_commit_in_flight_seconds: Gauge<f64, AtomicU64>,
      pub drain_paused: Gauge<f64, AtomicU64>,
      pub drain_generation: Gauge<f64, AtomicU64>,
      pub sink_last_success_timestamp_seconds: Gauge<f64, AtomicU64>,
      pub sink_last_failure_timestamp_seconds: Family<SinkFailureLabel, Gauge<f64, AtomicU64>>,
      pub sink_consecutive_failures: Gauge<f64, AtomicU64>,
      pub config_wab_max_bytes: Gauge<f64, AtomicU64>,
      pub config_dead_letter_max_bytes: Gauge<f64, AtomicU64>,
      pub config_wab_segment_max_bytes: Gauge<f64, AtomicU64>,
      pub config_segment_max_age_seconds: Gauge<f64, AtomicU64>,
      pub config_segment_max_lifetime_seconds: Gauge<f64, AtomicU64>,
      pub config_sink_max_retries: Gauge<f64, AtomicU64>,
      pub config_shutdown_timeout_seconds: Gauge<f64, AtomicU64>,
      pub records_nack_internal: Family<NackInternalLabel, Counter<u64, AtomicU64>>,
      pub connections_refused_no_live_shard: Counter<u64, AtomicU64>,
      pub drain_segments_abandoned: Counter<u64, AtomicU64>,
      pub wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>,
  ```
  Registrations in `new()`. Use the existing `reg!` shape, so the docs_drift parser reads them:
  ```rust
  let build_info = reg!(Family::<BuildInfoLabel, Gauge<f64, AtomicU64>>::default(), "weir_build_info",
      "Always 1; the labels carry the daemon version and the wire protocol version");
  let start_time_seconds = reg!(Gauge::<f64, AtomicU64>::default(), "weir_start_time_seconds",
      "Unix time the daemon process started");
  let startup_phase = reg!(Family::<StartupPhaseLabel, Gauge<f64, AtomicU64>>::default(), "weir_startup_phase",
      "Lifecycle phase, one-hot: initializing, recovering, handoff, binding, serving, shutting_down");
  let ready = reg!(Gauge::<f64, AtomicU64>::default(), "weir_ready",
      "1 when the daemon can accept ingest (the /readyz verdict), refreshed every 5 s");
  let live = reg!(Gauge::<f64, AtomicU64>::default(), "weir_live",
      "1 unless a selected liveness check fails (the /livez verdict), refreshed every 5 s");
  let condition_active = reg!(Family::<ConditionLabel, Gauge<f64, AtomicU64>>::default(), "weir_condition_active",
      "1 while the condition is active, 0 otherwise (also 0 while unevaluated); one series per catalogue code");
  let wab_shard_up = reg!(Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(), "weir_wab_shard_up",
      "1 while the shard's flusher is up or respawning, 0 while starting, offline or stopped");
  let wab_handoff_blocked_seconds = reg!(Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(),
      "weir_wab_handoff_blocked_seconds",
      "Seconds the shard's flusher has been blocked handing a sealed segment to a full drain channel; 0 when not blocked");
  let worker_up = reg!(Family::<WorkerLabel, Gauge<f64, AtomicU64>>::default(), "weir_worker_up",
      "1 while the worker thread is running");
  let wab_cap_rejecting = reg!(Gauge::<f64, AtomicU64>::default(), "weir_wab_cap_rejecting",
      "1 while the wab_max_bytes cap is rejecting pushes");
  let wab_undelivered_segments = reg!(Gauge::<f64, AtomicU64>::default(), "weir_wab_undelivered_segments",
      "Sealed segments not yet confirmed delivered (5 s scan)");
  let wab_undelivered_records = reg!(Gauge::<f64, AtomicU64>::default(), "weir_wab_undelivered_records",
      "Records in sealed, undelivered segments, from segment footers (5 s scan)");
  let wab_oldest_undelivered_age_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_wab_oldest_undelivered_age_seconds",
      "Upper bound on the age of the oldest undelivered record, from its segment header; 0 when none");
  let wab_active_segment_age_seconds = reg!(Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(),
      "weir_wab_active_segment_age_seconds", "Age of the shard's active (open) segment; 0 when none");
  let drain_pending_segments = reg!(Family::<DrainQueueLabel, Gauge<f64, AtomicU64>>::default(),
      "weir_drain_pending_segments", "Segments queued in the drain, by queue (live channel items, backlog)");
  let drain_replay_remaining_segments = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_drain_replay_remaining_segments", "Startup replay segments not yet delivered");
  let drain_heartbeat_age_seconds = reg!(Gauge::<f64, AtomicU64>::default(), "weir_drain_heartbeat_age_seconds",
      "Seconds since the drain heartbeat last moved (5 s poll)");
  let drain_commit_in_flight_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_drain_commit_in_flight_seconds", "Seconds the current sink commit has been in flight; 0 when none");
  let drain_paused = reg!(Gauge::<f64, AtomicU64>::default(), "weir_drain_paused",
      "1 while delivery is paused by an operator");
  let drain_generation = reg!(Gauge::<f64, AtomicU64>::default(), "weir_drain_generation",
      "Drain thread starts in this process (1 + respawns)");
  let sink_last_success_timestamp_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_sink_last_success_timestamp_seconds", "Unix time of the last successful sink commit; 0 before the first");
  let sink_last_failure_timestamp_seconds = reg!(Family::<SinkFailureLabel, Gauge<f64, AtomicU64>>::default(),
      "weir_sink_last_failure_timestamp_seconds", "Unix time of the last failed sink commit, by failure kind");
  let sink_consecutive_failures = reg!(Gauge::<f64, AtomicU64>::default(), "weir_sink_consecutive_failures",
      "Sink commits failed in a row since the last success");
  let config_wab_max_bytes = reg!(Gauge::<f64, AtomicU64>::default(), "weir_config_wab_max_bytes",
      "Effective wab_max_bytes (0 = no cap)");
  let config_dead_letter_max_bytes = reg!(Gauge::<f64, AtomicU64>::default(), "weir_config_dead_letter_max_bytes",
      "Effective dead_letter_max_bytes");
  let config_wab_segment_max_bytes = reg!(Gauge::<f64, AtomicU64>::default(), "weir_config_wab_segment_max_bytes",
      "Effective wab_segment_max_bytes");
  let config_segment_max_age_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_config_segment_max_age_seconds", "Effective wab_segment_max_age_secs (0 = idle seal off)");
  let config_segment_max_lifetime_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_config_segment_max_lifetime_seconds", "Effective wab_segment_max_lifetime_secs (0 = lifetime seal off)");
  let config_sink_max_retries = reg!(Gauge::<f64, AtomicU64>::default(), "weir_config_sink_max_retries",
      "Effective sink_max_retries");
  let config_shutdown_timeout_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
      "weir_config_shutdown_timeout_seconds", "Effective shutdown_timeout_secs");
  let records_nack_internal = reg!(Family::<NackInternalLabel, Counter<u64, AtomicU64>>::default(),
      "weir_records_nack_internal", "Transient InternalError nacks, by cause");
  let connections_refused_no_live_shard = reg!(Counter::<u64, AtomicU64>::default(),
      "weir_connections_refused_no_live_shard", "New connections refused because no shard could accept records");
  let drain_segments_abandoned = reg!(Counter::<u64, AtomicU64>::default(), "weir_drain_segments_abandoned",
      "Segments the drain thread left partly delivered when it exited on a stop (the shutdown deadline, or a stop while the segment was paused or blocked); they stay sealed and unconfirmed and are re-delivered from record 0 on the next start");
  let wab_segments_not_handed_off = reg!(Family::<NotHandedOffLabel, Counter<u64, AtomicU64>>::default(),
      "weir_wab_segments_not_handed_off",
      "Sealed segments a flusher could not hand to the drain, by reason: budget (the 1 s shutdown hand-off budget ran out) or disconnected (the drain was gone); durable, delivered on restart");
  ```
  Add every new binding to the `Metrics { … }` struct literal. Pre-creation, before `(metrics, reg)`:
  ```rust
  for phase in StartupPhaseValue::ALL {
      metrics.startup_phase.get_or_create(&StartupPhaseLabel { phase }).set(0.0);
  }
  for e in weir_status::CATALOGUE {
      metrics.condition_active.get_or_create(&ConditionLabel::of(e)).set(0.0);
  }
  for queue in [DrainQueue::live, DrainQueue::backlog] {
      metrics.drain_pending_segments.get_or_create(&DrainQueueLabel { queue }).set(0.0);
  }
  for kind in SinkFailureValue::ALL {
      metrics.sink_last_failure_timestamp_seconds.get_or_create(&SinkFailureLabel { kind }).set(0.0);
  }
  for cause in NackInternalCause::ALL {
      let _ = metrics.records_nack_internal.get_or_create(&NackInternalLabel { cause });
  }
  for reason in NotHandedOffReason::ALL {
      let _ = metrics.wab_segments_not_handed_off.get_or_create(&NotHandedOffLabel { reason });
  }
  ```
  Methods:
  ```rust
  impl Metrics {
      /// Pre-creates the per-shard and per-worker series (spec §9.2 rule 6:
      /// after startup, `get_or_create` only ever takes the read lock).
      pub(crate) fn init_topology(&self, shard_count: usize, worker_count: usize) {
          for s in 0..shard_count {
              let l = ShardLabel::of(s);
              self.wab_shard_up.get_or_create(&l).set(0.0);
              self.wab_handoff_blocked_seconds.get_or_create(&l).set(0.0);
              self.wab_active_segment_age_seconds.get_or_create(&l).set(0.0);
          }
          for w in 0..worker_count {
              self.worker_up.get_or_create(&WorkerLabel::of(w)).set(0.0);
          }
      }

      /// Both causes of `weir_wab_segments_not_handed_off_total` summed: what
      /// PR 3's DST invariant and PR 4's T3 summary read.
      #[cfg_attr(not(test), allow(dead_code))] // PR 3 / PR 4 call this
      pub(crate) fn wab_segments_not_handed_off_total(&self) -> u64 {
          NotHandedOffReason::ALL
              .into_iter()
              .map(|reason| self.wab_segments_not_handed_off.get_or_create(&NotHandedOffLabel { reason }).get())
              .sum()
      }

      pub(crate) fn publish_build_info(&self, version: &str, wire_version: u8) {
          self.build_info
              .get_or_create(&BuildInfoLabel { version: version.to_string(), wire_version: wire_version.to_string() })
              .set(1.0);
          let now = std::time::SystemTime::now()
              .duration_since(std::time::UNIX_EPOCH)
              .map_or(0.0, |d| d.as_secs_f64());
          self.start_time_seconds.set(now);
      }

      pub(crate) fn publish_config(&self, c: &crate::config::Config) {
          self.config_wab_max_bytes.set(c.wab_max_bytes as f64);
          self.config_dead_letter_max_bytes.set(c.dead_letter_max_bytes as f64);
          self.config_wab_segment_max_bytes.set(c.wab_segment_max_bytes as f64);
          self.config_segment_max_age_seconds.set(c.wab_segment_max_age_secs as f64);
          self.config_segment_max_lifetime_seconds.set(c.wab_segment_max_lifetime_secs as f64);
          self.config_sink_max_retries.set(f64::from(c.sink_max_retries));
          self.config_shutdown_timeout_seconds.set(c.shutdown_timeout_secs as f64);
      }

      pub(crate) fn publish_phase(&self, phase: weir_status::Phase) {
          let current = StartupPhaseValue::of(phase);
          for p in StartupPhaseValue::ALL {
              self.startup_phase.get_or_create(&StartupPhaseLabel { phase: p }).set(if p == current { 1.0 } else { 0.0 });
          }
      }
  }
  ```
  `board.rs`, `impl StatusBoard`:
  ```rust
  /// Moves the lifecycle phase and publishes `weir_startup_phase`.
  pub(crate) fn enter_phase(&self, phase: Phase, metrics: &crate::metrics::Metrics) -> bool {
      let moved = self.life.enter(phase, unix_ms_now());
      if moved {
          metrics.publish_phase(phase);
      }
      moved
  }

  /// T0 (spec §7 a-b): the shared flag, the phase, and its gauge. True for the
  /// first caller only. PR 4's Lifecycle calls this; PR 1's signal task does.
  pub(crate) fn begin_shutdown(&self, metrics: &crate::metrics::Metrics) -> bool {
      let first = self.life.begin_shutdown(unix_ms_now());
      if first {
          metrics.publish_phase(Phase::ShuttingDown);
      }
      first
  }
  ```
  `docs/monitoring.md`: in `## Metric reference`, immediately before `### Integrity / tamper evidence (weir-attest)`, add:
  ```markdown
  ### Status, lifecycle and conditions

  Added by Foundations. The conditions behind `weir_condition_active` are documented under Conditions (one `cond-<code>` heading each). Families marked *(written from PR n)* exist at 0 from boot and move once the named part of Foundations is in the build.

  | Metric | Type | Meaning |
  |---|---|---|
  | `weir_build_info{version,wire_version}` | gauge | Always 1; the labels carry the daemon and wire-protocol versions. Join on it to label dashboards by version. |
  | `weir_start_time_seconds` | gauge | Unix time the process started. `time() - weir_start_time_seconds` is uptime; a reset means a restart. |
  | `weir_startup_phase{phase}` | gauge | One-hot lifecycle phase: `initializing`, `recovering`, `handoff`, `binding`, `serving`, `shutting_down`. A daemon stuck in `recovering` is replaying a large or slow WAB. |
  | `weir_ready` | gauge | 1 when the daemon can accept ingest: serving, not shutting down, not cap-rejecting, at least one live shard, no listener down. Refreshed every 5 s. Sink health never gates it. |
  | `weir_live` | gauge | 1 unless a check selected in `livez_checks` fails. With the default empty list it is always 1 while the process answers. |
  | `weir_condition_active{code,severity}` | gauge | 1 while the condition is active; 0 when inactive **or unevaluated** (missing evidence). One pre-created series per code. Alert on `severity="critical"`. |
  | `weir_wab_shard_up{shard}` | gauge | 1 while the shard's flusher is up or respawning. 0 means new pushes for that shard nack; see `shard_offline`. |
  | `weir_wab_handoff_blocked_seconds{shard}` | gauge | Seconds the shard's flusher has been blocked handing a sealed segment to a full drain channel. Non-zero means delivery is stalling ingest (acks wait), not slow storage. *(written from PR 3)* |
  | `weir_worker_up{worker}` | gauge | 1 while the worker thread runs. 0 after startup means the shards it owns cannot accept records. |
  | `weir_wab_cap_rejecting` | gauge | 1 while `wab_max_bytes` is rejecting pushes. Since Foundations it clears on its own once the WAB drains below 90% of the cap, without traffic. |
  | `weir_wab_undelivered_segments` | gauge | Sealed segments not yet confirmed delivered, from a read-only 5 s scan. |
  | `weir_wab_undelivered_records` | gauge | Records in those segments, from their footers. |
  | `weir_wab_oldest_undelivered_age_seconds` | gauge | Upper bound on the age of the oldest undelivered record (from its segment header); 0 when nothing is undelivered. |
  | `weir_wab_active_segment_age_seconds{shard}` | gauge | Age of the shard's open segment. With both seal timers 0 a quiet shard's records wait here until rotation or shutdown. |
  | `weir_drain_pending_segments{queue}` | gauge | Segments queued in the drain: `live` (from the channel) and `backlog` (startup replay and rescans). *(written from PR 2)* |
  | `weir_drain_replay_remaining_segments` | gauge | Startup replay segments not yet delivered. *(written from PR 2)* |
  | `weir_drain_heartbeat_age_seconds` | gauge | Seconds since the drain heartbeat last moved. Read with `weir_drain_busy_seconds`. |
  | `weir_drain_commit_in_flight_seconds` | gauge | Seconds the current sink commit has been in flight; 0 when none. *(written from PR 2)* |
  | `weir_drain_paused` | gauge | 1 while an operator has paused delivery. Ingest keeps buffering. *(written from PR 2)* |
  | `weir_drain_generation` | gauge | Drain thread starts in this process (1 + respawns). *(written from PR 2)* |
  | `weir_sink_last_success_timestamp_seconds` | gauge | Unix time of the last successful sink commit; 0 before the first. *(written from PR 2)* |
  | `weir_sink_last_failure_timestamp_seconds{kind}` | gauge | Unix time of the last failed commit, by `kind`: `transient`, `permanent`, `timeout`, `contract_violation`. *(written from PR 2)* |
  | `weir_sink_consecutive_failures` | gauge | Commits failed in a row since the last success. *(written from PR 2)* |
  | `weir_config_wab_max_bytes` | gauge | Effective `wab_max_bytes` (0 = no cap). |
  | `weir_config_dead_letter_max_bytes` | gauge | Effective `dead_letter_max_bytes`. |
  | `weir_config_wab_segment_max_bytes` | gauge | Effective `wab_segment_max_bytes`. |
  | `weir_config_segment_max_age_seconds` | gauge | Effective `wab_segment_max_age_secs` (0 = off). |
  | `weir_config_segment_max_lifetime_seconds` | gauge | Effective `wab_segment_max_lifetime_secs` (0 = off). |
  | `weir_config_sink_max_retries` | gauge | Effective `sink_max_retries`. |
  | `weir_config_shutdown_timeout_seconds` | gauge | Effective `shutdown_timeout_secs`. |
  | `weir_records_nack_internal_total{cause}` | counter | Transient `InternalError` nacks by cause: `queue_full`, `write_failed`, `sender_dropped`, `ack_timeout`, `wab_cap`, `coordinate`, `ownership_lost`. The wire reason is the same for all of them; this is how to tell them apart. |
  | `weir_connections_refused_no_live_shard_total` | counter | New connections refused because no shard could accept records. *(written from PR 3)* |
  | `weir_drain_segments_abandoned_total` | counter | Segments left partly delivered when the drain thread exited on a stop: at the shutdown deadline, or while the segment was held by a pause or blocked on a full dead letter. They stay sealed and unconfirmed and are re-delivered from record 0 on the next start. *(written from PR 2)* |
  | `weir_wab_segments_not_handed_off_total{reason}` | counter | Sealed segments a flusher could not hand to the drain, by `reason`: `budget` (shutdown hand-off budget spent) or `disconnected` (drain gone). Durable; delivered on restart. *(written from PR 3)* |
  ```
  PR 8 removes the "*(written from PR n)*" notes as each part ships (they go in one release).
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1 metrics:: status::board`. Expected: all pass.
  - `cargo test -p weir-server --test docs_drift every_registered_metric_is_in_the_monitoring_reference`. Expected: passes (34 new families, 34 rows).
  - `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1`. Expected: passes (the `# HELP weir_` count matches the extended list).
  - Mutation check: delete the `weir_ready` row and rerun the docs_drift test. It must FAIL listing `weir_ready`. Restore the row.
- [ ] **Step 5: Commit.** `git commit -am "feat(metrics): status, lifecycle and condition families, pre-created from boot"`

---
### Task 1.19: Extract the poll tasks into `status/poll.rs` (mechanical move)

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/status/poll.rs`
- Modify: `crates/weir-server/src/main.rs`:
  - delete lines 70-740, from `/// Total live shard-segment bytes` through the end of `fn drain_is_healthy`;
  - delete test modules `wab_bytes_tests` (:1972-2086), `tests` (:2087-2233), `drain_is_healthy_tests` (:2234-2332), `apply_wab_scan_tests` (:2333-2432), `drain_heartbeat_stall_tests` (:2433-2789) and `drain_stall_log_tests` (:2790-end);
  - drop `drain_appears_stalled` from the `use drain::{…}` line (:56).
  - Checked against 2491883 (3adc5f5): :70-740 is still `/// Total live shard-segment bytes` through `fn drain_is_healthy`. The lock-integrity items (`LOCK_INTEGRITY_INTERVAL` :749, `apply_lock_integrity` :760, `spawn_lock_integrity_thread` :825) and `lock_integrity_tests` (:1825-1971) sit outside every cut range and stay in main.rs. Anchor the cut on the item names if the lines have moved.
- Modify: `crates/weir-server/src/status/mod.rs`: `pub(crate) mod poll;` (the lock scan walks the directory, so poll.rs is scanned without an edit).
- Test: the moved test modules, unchanged except for paths.

**Interfaces:**
- Consumes: `crate::drain::{BusyKind, DrainHeartbeat, drain_appears_stalled}`, `crate::metrics::*`.
- Produces (moved; visibility raised from private-in-main to `pub(crate)` only where main still calls them):
  ```rust
  pub(crate) fn compute_wab_bytes_on_disk(wab_dir: &Path) -> Option<u64>;
  pub(crate) struct DrainLiveness;  pub(crate) fn DrainLiveness::new(now: Instant, health_poll_interval: Duration, commit_timeout: Duration) -> Self;
  pub(crate) struct WabPoll;        pub(crate) fn WabPoll::new(metrics: Arc<Metrics>, wab_bytes_now: Arc<AtomicU64>, wab_max_bytes: u64, heartbeat: DrainHeartbeat, liveness: DrainLiveness) -> Self;
  pub(crate) async fn WabPoll::tick<F, Fut>(&mut self, now: Instant, scan: F) -> Option<DrainStallVerdict>;
  pub(crate) enum DrainStallVerdict;   // raised because `WabPoll::tick` returns it
  ```
  Everything else (`apply_wab_scan`, `should_warn_wab_growth`, `drain_is_healthy`, `drain_stall_verdict`, the constants, `dirent_fault`) stays module-private.

- [ ] **Step 1: Write the failing test.**
  - Create `crates/weir-server/src/status/poll.rs` and **cut** the six test modules listed above from main.rs into it, verbatim.
  - Rename the module that was called `tests` to `growth_warning_tests`; poll.rs gets its own `tests` in Task 1.20.
  - Add `pub(crate) mod poll;` to status/mod.rs.
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::poll -- --test-threads=1`. Expected: dozens of `cannot find function `compute_wab_bytes_on_disk` in module `super`` / `cannot find struct `WabPoll`` errors.
- [ ] **Step 3: Implement.** Cut main.rs:70-740 into poll.rs above the test modules. Add this header:
  ```rust
  //! The status poll tasks (spec §5.11), moved out of main.rs unchanged: the
  //! 5 s WAB-size scan feeding the `wab_max_bytes` cap and the growth warning,
  //! and the drain-heartbeat stall check with its busy gauge. Later sections
  //! of this file add the cap refresh, the lag scan, the counter windows, the
  //! evaluation and the 500 ms queue-depth task.
  use std::path::Path;
  use std::sync::Arc;
  use std::time::Duration;

  use tracing::{info, warn};

  use crate::drain::{self, DrainHeartbeat, drain_appears_stalled};
  ```
  - Inside the moved code, the old `drain::BusyKind` paths keep working through the `use crate::drain::{self, …}` import, so no text change is needed. `crate::metrics::…` paths are already absolute.
  - Make these `pub(crate)`: `compute_wab_bytes_on_disk`, `struct DrainLiveness` and `DrainLiveness::new`, `struct WabPoll`, `WabPoll::new` and `WabPoll::tick`.
  - Make `enum DrainStallVerdict` (main.rs:234) `pub(crate)`, because `WabPoll::tick` returns it: a `pub(crate)` fn returning a module-private type trips rustc's warn-by-default `private_interfaces` lint, which gate 5's `clippy -D warnings` turns into an error. Its payload `drain::BusyKind` is already `pub(crate)`.
  - In `main.rs`, change the 5 s task's closure to call `status::poll::compute_wab_bytes_on_disk(&wab_dir)`, and the constructors to `status::poll::WabPoll::new(...)` and `status::poll::DrainLiveness::new(...)`.
  - The `#[cfg(test)] fn dirent_fault_hook` / `#[cfg(not(test))]` pair and `mod dirent_fault` move as they are.
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server status::poll -- --test-threads=1`. Expected: every moved test passes. Before the move, record `cargo test -p weir-server --bin weir-server -- --list 2>/dev/null | grep -cE "^(wab_bytes_tests|tests|drain_is_healthy_tests|apply_wab_scan_tests|drain_heartbeat_stall_tests|drain_stall_log_tests)::"`. After it, `... | grep -c "^status::poll::"` must report the same number.
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1`. Expected: the full bin suite passes.
- [ ] **Step 5: Commit.** `git add crates/weir-server/src && git commit -am "refactor(status): move the poll-task helpers out of main into status::poll"`

---

### Task 1.20: Cap refresh without traffic, scan stamps, the growth flag, and the synchronous startup cap gate

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/status/poll.rs`: `WabPoll` gains `status: Option<PollStatus>` and `with_status`; `tick` evaluates the cap after a successful scan; new `PollStatus`; new `startup_cap_gate`.
- Test: `crates/weir-server/src/status/poll.rs` (new `mod tests`)

**Interfaces:**
- Consumes: `crate::wab::cap::evaluate`, `StatusBoard::{wab, wab_cap_rejecting, wab_bytes_now}`, `Metrics::wab_cap_rejecting`.
- Produces:
  ```rust
  pub(crate) struct PollStatus { /* board, wab_dir, thresholds, coalesce_hint (+ Tasks 1.21-1.22 fields) */ }
  impl PollStatus { pub(crate) fn new(board: Arc<StatusBoard>, wab_dir: PathBuf, coalesce_hint: Arc<AtomicU64>) -> Self; }
  impl WabPoll { pub(crate) fn with_status(self, status: PollStatus) -> Self; }
  /// Spec §6 step 9. Returns the scanned size, or None when the scan failed.
  pub(crate) fn startup_cap_gate(wab_dir: &Path, wab_max_bytes: u64, board: &StatusBoard, metrics: &Metrics) -> Option<u64>;
  ```

- [ ] **Step 1: Write the failing tests** (new module at the end of poll.rs):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::metrics::Metrics;
      use crate::status::StatusBoard;
      use std::sync::atomic::{AtomicU64, Ordering};
      use std::time::Instant;

      fn secs(n: u64) -> Duration {
          Duration::from_secs(n)
      }

      fn wab_with_bytes(label: &str, bytes: usize) -> std::path::PathBuf {
          let root = crate::testutil::scratch_dir(&format!("poll_{label}"));
          let shard = root.join("shard_00");
          crate::testutil::mkdir_p(&shard);
          std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; bytes]).unwrap();
          root
      }

      fn poll_for(board: &Arc<StatusBoard>, metrics: &Arc<Metrics>, cap: u64, dir: std::path::PathBuf) -> WabPoll {
          WabPoll::new(
              Arc::clone(metrics),
              Arc::clone(&board.wab_bytes_now),
              cap,
              DrainHeartbeat::new(),
              DrainLiveness::new(Instant::now(), secs(30), secs(60)),
          )
          .with_status(PollStatus::new(Arc::clone(board), dir, Arc::new(AtomicU64::new(200))))
      }

      #[test]
      fn startup_gate_rejects_a_restart_over_the_cap_before_any_listener_exists() {
          let dir = wab_with_bytes("gate_over", 2_000);
          let board = StatusBoard::for_test();
          let m = Metrics::new().0;
          assert_eq!(startup_cap_gate(&dir, 1_000, &board, &m), Some(2_000));
          assert!(board.wab_cap_rejecting.load(Ordering::Relaxed));
          assert_eq!(board.wab_bytes_now.load(Ordering::Relaxed), 2_000);
          assert!(board.wab.first_scan_ok_ms().is_some());
          assert_eq!(m.wab_cap_rejecting.get(), 1.0);
          let under = StatusBoard::for_test();
          assert_eq!(startup_cap_gate(&dir, 10_000, &under, &m), Some(2_000));
          assert!(!under.wab_cap_rejecting.load(Ordering::Relaxed));
      }

      #[test]
      fn a_failed_startup_scan_fails_closed_only_when_a_cap_is_set() {
          let missing = crate::testutil::scratch_dir("poll_gate_missing").join("no_such_wab_dir");
          let m = Metrics::new().0;
          let capped = StatusBoard::for_test();
          assert_eq!(startup_cap_gate(&missing, 1_000, &capped, &m), None);
          assert!(capped.wab_cap_rejecting.load(Ordering::Relaxed), "fail-closed: nack until a scan succeeds");
          assert!(capped.wab.first_scan_ok_ms().is_none());
          let uncapped = StatusBoard::for_test();
          startup_cap_gate(&missing, 0, &uncapped, &m);
          assert!(!uncapped.wab_cap_rejecting.load(Ordering::Relaxed));
      }

      #[tokio::test]
      async fn the_poll_clears_the_cap_with_zero_pushes() {
          let board = StatusBoard::for_test();
          let m = Arc::new(Metrics::new().0);
          board.wab_cap_rejecting.store(true, Ordering::Relaxed);
          let mut poll = poll_for(&board, &m, 1_000, std::path::PathBuf::from("/nonexistent"));
          poll.tick(Instant::now(), || async { Ok(Some(100)) }).await;
          assert!(!board.wab_cap_rejecting.load(Ordering::Relaxed), "drained below low water: released without a push");
          assert!(board.wab.last_scan_ok_ms().is_some());
          assert_eq!(m.wab_cap_rejecting.get(), 0.0);
      }

      #[tokio::test]
      async fn a_failed_poll_scan_leaves_the_cap_and_the_scan_stamp_alone() {
          let board = StatusBoard::for_test();
          let m = Arc::new(Metrics::new().0);
          board.wab_cap_rejecting.store(true, Ordering::Relaxed);
          let mut poll = poll_for(&board, &m, 1_000, std::path::PathBuf::from("/nonexistent"));
          poll.tick(Instant::now(), || async { Ok(None) }).await;
          assert!(board.wab_cap_rejecting.load(Ordering::Relaxed), "fail-closed");
          assert!(board.wab.last_scan_ok_ms().is_none());
      }

      #[tokio::test]
      async fn the_growth_verdict_is_published_on_every_successful_scan() {
          let board = StatusBoard::for_test();
          let m = Arc::new(Metrics::new().0);
          let mut poll = poll_for(&board, &m, 0, std::path::PathBuf::from("/nonexistent"));
          assert_eq!(board.wab.growth(), None);
          poll.tick(Instant::now(), || async { Ok(Some(10)) }).await;
          assert_eq!(board.wab.growth(), Some(false), "a one-sample window is not growth");
      }
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::poll::tests -- --test-threads=1`. Expected: `cannot find function `startup_cap_gate``, `no method named `with_status``, `cannot find struct `PollStatus``.
- [ ] **Step 3: Implement.** In poll.rs:
  ```rust
  use crate::metrics::Metrics;
  use crate::status::{StatusBoard, unix_ms_now};
  use std::path::PathBuf;
  use std::sync::atomic::{AtomicU64, Ordering};

  /// The status half of the 5 s task (spec §5.11). `None` in the moved unit
  /// tests, which exercise the warning logic alone.
  pub(crate) struct PollStatus {
      board: Arc<StatusBoard>,
      wab_dir: PathBuf,
      thresholds: weir_status::Thresholds,
      coalesce_hint: Arc<AtomicU64>,
  }

  impl PollStatus {
      pub(crate) fn new(board: Arc<StatusBoard>, wab_dir: PathBuf, coalesce_hint: Arc<AtomicU64>) -> Self {
          PollStatus { board, wab_dir, thresholds: weir_status::Thresholds::DEFAULT, coalesce_hint }
      }
  }

  fn flag(v: bool) -> f64 {
      if v { 1.0 } else { 0.0 }
  }

  /// Spec §6 step 9: the first size scan and cap evaluation run synchronously,
  /// after recovery and before any producer listener exists. Without it a
  /// daemon restarted over its cap reads `wab_bytes_now = 0`, reports ready
  /// and admits pushes until the first asynchronous scan. Fail-closed: if the
  /// scan fails and a cap is set, start rejecting until a poll scan succeeds.
  pub(crate) fn startup_cap_gate(wab_dir: &Path, wab_max_bytes: u64, board: &StatusBoard, metrics: &Metrics) -> Option<u64> {
      match compute_wab_bytes_on_disk(wab_dir) {
          Some(bytes) => {
              metrics.wab_bytes_on_disk.set(bytes as f64);
              board.wab_bytes_now.store(bytes, Ordering::Relaxed);
              board.wab.note_scan_ok(unix_ms_now());
              let rejecting = crate::wab::cap::evaluate(wab_max_bytes, bytes, &board.wab_cap_rejecting);
              metrics.wab_cap_rejecting.set(flag(rejecting));
              if rejecting {
                  warn!(
                      wab_bytes = bytes,
                      wab_max_bytes,
                      "the WAB is over wab_max_bytes at startup; pushes nack until delivery brings it below {}",
                      crate::wab::cap::low_water(wab_max_bytes)
                  );
              }
              Some(bytes)
          }
          None => {
              if wab_max_bytes > 0 {
                  board.wab_cap_rejecting.store(true, Ordering::Relaxed);
                  metrics.wab_cap_rejecting.set(1.0);
                  warn!(
                      wab_dir = %wab_dir.display(),
                      "the startup WAB size scan failed; with wab_max_bytes set, pushes nack (fail-closed) \
                       until a scan succeeds (every 5 s)"
                  );
              }
              None
          }
      }
  }
  ```
  `WabPoll`:
  - add the field `status: Option<PollStatus>` (initialised `None` in `new`);
  - add
    ```rust
    pub(crate) fn with_status(mut self, status: PollStatus) -> Self {
        self.status = Some(status);
        self
    }
    ```
  - replace the body of `tick` after `let logged = self.check_drain_liveness(now);` with:
  ```rust
  // On a failed scan the last known size stands, the cap flag is left as it
  // is (fail-closed) and this tick contributes no growth sample; see
  // `apply_wab_scan`. The status steps below still run.
  if let Some(bytes) = apply_wab_scan(scan().await, &mut self.samples, &self.metrics, &self.wab_bytes_now) {
      if let Some(st) = &self.status {
          st.board.wab.note_scan_ok(unix_ms_now());
          // Spec §5.5: the refresh that clears the flag without traffic.
          let rejecting = crate::wab::cap::evaluate(self.wab_max_bytes, bytes, &st.board.wab_cap_rejecting);
          self.metrics.wab_cap_rejecting.set(flag(rejecting));
      }
      let drain_healthy = drain_is_healthy(&self.metrics);
      let growing = should_warn_wab_growth(&self.samples, self.wab_max_bytes, drain_healthy);
      if let Some(st) = &self.status {
          st.board.wab.set_growth(growing);
      }
      if growing && growth_warning_is_due(self.last_growth_warned, std::time::Instant::now()) {
          warn!(
              wab_bytes = bytes,
              growth_bytes = bytes.saturating_sub(self.samples[0]),
              window_secs = WAB_GROWTH_WINDOW * 5,
              "WAB is growing while the sink is not healthy and no wab_max_bytes \
               is set — producers are still being acked into an unbounded buffer. \
               Set wab_max_bytes to bound it."
          );
          self.last_growth_warned = Some(std::time::Instant::now());
      }
  }
  logged
  ```
  The warning text is unchanged. The `due && should_warn(..)` pair becomes `growing && due`, which is the same predicate with the evaluation order swapped, so the moved `growth_warning_*` tests still hold. `thresholds` and `coalesce_hint` stay unused until Task 1.22, and their `dead_code` warnings are expected until then.
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server status::poll -- --test-threads=1`. Expected: all pass, the moved tests included (5 new).
  - Mutation: in `tick`, move the `crate::wab::cap::evaluate` call out of the `Some(bytes)` arm so that it also runs on a failed scan, passing `0`. `a_failed_poll_scan_leaves_the_cap_and_the_scan_stamp_alone` must FAIL. Revert.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): the poll task refreshes the WAB cap without traffic; a synchronous startup gate fails closed"`

---

### Task 1.21: The read-only lag scan with a bounded footer cache

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/status/poll.rs`: `FooterCache`, `scan_wab_lag`, `PollStatus::refresh_lag`, and the `footer_cache` / `lag_failing` fields.
- Test: `crates/weir-server/src/status/poll.rs` (`mod tests`)

**Interfaces:**
- Consumes: `crate::wab::read_segment_record_count` (wab/mod.rs:668), `weir_wab::format::{parse_segment_header, confirmed_path_for, SEGMENT_HEADER_LEN, EXT_SEALED, EXT_ACTIVE}`, `board::LagReport`, `WabCell::set_lag`.
- Produces:
  ```rust
  #[derive(Default)] pub(crate) struct FooterCache { /* HashMap<PathBuf, SealedMeta> */ }
  impl FooterCache { pub(crate) fn len(&self) -> usize; }
  pub(crate) const FOOTER_CACHE_CAP: usize = 65_536;
  /// Read-only: never calls check_confirmed, never feeds wab_bytes_now, tolerates NotFound.
  pub(crate) fn scan_wab_lag(wab_dir: &Path, cache: &mut FooterCache, now_ms: u64) -> std::io::Result<LagReport>;
  ```

- [ ] **Step 1: Write the failing tests** (append to poll.rs `tests`):
  ```rust
  use crate::wab::segment::{WabSegment, segment_path};
  use weir_wab::format::{Compression, confirmed_path_for};

  fn sealed(shard_dir: &Path, counter: u64, records: usize) -> std::path::PathBuf {
      let mut seg = WabSegment::create(&segment_path(shard_dir, counter), 0, Compression::None).unwrap();
      for i in 0..records {
          seg.write_record(format!("r{i}").as_bytes()).unwrap();
      }
      seg.seal().unwrap()
  }

  #[test]
  fn the_lag_scan_counts_undelivered_work_and_never_touches_the_wab() {
      let root = crate::testutil::scratch_dir("poll_lag");
      let _ = std::fs::remove_dir_all(&root);
      let shard = root.join("shard_00");
      crate::testutil::mkdir_p(&shard);
      sealed(&shard, 1, 3);
      let delivered = sealed(&shard, 2, 5);
      std::fs::write(confirmed_path_for(&delivered), b"sidecar").unwrap();
      let mut active = WabSegment::create(&segment_path(&shard, 3), 0, Compression::None).unwrap();
      active.write_record(b"open").unwrap();
      for (dir, files) in [("dead_letter", 1usize), ("quarantine", 2)] {
          crate::testutil::mkdir_p(root.join(dir));
          for i in 0..files {
              std::fs::write(root.join(dir).join(format!("f{i}")), vec![0u8; 10]).unwrap();
          }
      }
      let listing = || {
          let mut v: Vec<_> = std::fs::read_dir(&shard).unwrap().map(|e| e.unwrap().path()).collect();
          v.sort();
          v
      };
      let before = listing();
      let mut cache = FooterCache::default();
      let now = unix_ms_now() + 60_000;
      let lag = scan_wab_lag(&root, &mut cache, now).unwrap();
      assert_eq!((lag.undelivered_segments, lag.undelivered_records), (1, 3), "the confirmed one is delivered");
      assert!(lag.oldest_created_ms.is_some_and(|c| now - c >= 60_000));
      assert_eq!(lag.active_created_ms.len(), 1);
      assert_eq!((lag.dead_letter_files, lag.dead_letter_bytes), (1, 10));
      assert_eq!((lag.quarantine_files, lag.quarantine_bytes), (2, 20));
      assert_eq!(lag.scanned_at_ms, now);
      assert_eq!(before, listing(), "read-only: nothing created, renamed or deleted");
      assert_eq!(cache.len(), 1);
      drop(active);
  }

  #[test]
  fn the_footer_cache_forgets_deleted_segments() {
      let root = crate::testutil::scratch_dir("poll_lag_evict");
      let _ = std::fs::remove_dir_all(&root);
      let shard = root.join("shard_00");
      crate::testutil::mkdir_p(&shard);
      let seg = sealed(&shard, 1, 2);
      let mut cache = FooterCache::default();
      scan_wab_lag(&root, &mut cache, unix_ms_now()).unwrap();
      assert_eq!(cache.len(), 1);
      std::fs::remove_file(&seg).unwrap();
      let lag = scan_wab_lag(&root, &mut cache, unix_ms_now()).unwrap();
      assert_eq!((lag.undelivered_segments, cache.len()), (0, 0));
  }

  #[tokio::test]
  async fn refresh_lag_publishes_onto_the_board_and_keeps_the_last_report_on_failure() {
      let root = crate::testutil::scratch_dir("poll_lag_board");
      let _ = std::fs::remove_dir_all(&root);
      crate::testutil::mkdir_p(root.join("shard_00"));
      sealed(&root.join("shard_00"), 1, 4);
      let board = StatusBoard::for_test();
      let mut st = PollStatus::new(Arc::clone(&board), root.clone(), Arc::new(AtomicU64::new(200)));
      st.refresh_lag(unix_ms_now()).await;
      assert_eq!(board.wab.lag().unwrap().undelivered_records, 4);
      std::fs::remove_dir_all(&root).unwrap();
      st.refresh_lag(unix_ms_now()).await;
      assert_eq!(board.wab.lag().unwrap().undelivered_records, 4, "a failed scan leaves the last report (stale, not zero)");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::poll::tests -- --test-threads=1`. Expected: `cannot find function `scan_wab_lag``, `cannot find struct `FooterCache``, `no method named `refresh_lag``.
- [ ] **Step 3: Implement** (poll.rs):
  ```rust
  use crate::status::board::LagReport;
  use std::collections::{HashMap, HashSet};
  use std::io::{self, Read};
  use weir_wab::format::{EXT_ACTIVE, EXT_SEALED, SEGMENT_HEADER_LEN, confirmed_path_for, parse_segment_header};

  pub(crate) const FOOTER_CACHE_CAP: usize = 65_536;

  #[derive(Clone, Copy)]
  struct SealedMeta {
      records: u64,
      created_ms: Option<u64>,
  }

  /// Footer and header facts of sealed segments, keyed by path. A sealed
  /// segment never changes, so one read per segment lifetime. Entries not seen
  /// by a scan are dropped (the segment was delivered and deleted); new
  /// entries stop being cached at `FOOTER_CACHE_CAP` (spec §20 (c)).
  #[derive(Default)]
  pub(crate) struct FooterCache {
      entries: HashMap<PathBuf, SealedMeta>,
  }

  impl FooterCache {
      pub(crate) fn len(&self) -> usize {
          self.entries.len()
      }
  }

  /// `created_at` from a segment header, as unix ms. `None` for a header not
  /// fully written yet (a just-created active segment) or one that does not parse.
  fn header_created_ms(path: &Path) -> io::Result<Option<u64>> {
      let mut buf = [0u8; SEGMENT_HEADER_LEN];
      let mut f = std::fs::File::open(path)?;
      if f.read_exact(&mut buf).is_err() {
          return Ok(None);
      }
      Ok(parse_segment_header(&buf).ok().and_then(|h| u64::try_from(h.created_at).ok()).map(|ns| ns / 1_000_000))
  }

  fn dir_files_and_bytes(dir: &Path) -> io::Result<(u64, u64)> {
      let mut files = 0;
      let mut bytes = 0;
      for entry in std::fs::read_dir(dir)? {
          let entry = entry?;
          match entry.metadata() {
              Ok(m) if m.is_file() => {
                  files += 1;
                  bytes += m.len();
              }
              Ok(_) => {}
              Err(e) if e.kind() == io::ErrorKind::NotFound => {}
              Err(e) => return Err(e),
          }
      }
      Ok((files, bytes))
  }

  /// The read-only lag scan (spec §5.11 step 3). It never feeds `wab_bytes_now`
  /// and never calls `check_confirmed` (whose quarantine side effects belong to
  /// the drain); a sealed segment with a `.confirmed` sidecar beside it is
  /// delivered and is skipped. Files vanishing mid-scan (the drain deletes
  /// confirmed segments continuously) are tolerated.
  pub(crate) fn scan_wab_lag(wab_dir: &Path, cache: &mut FooterCache, now_ms: u64) -> io::Result<LagReport> {
      let mut r = LagReport { scanned_at_ms: now_ms, ..LagReport::default() };
      let mut seen = HashSet::new();
      for entry in std::fs::read_dir(wab_dir)? {
          let entry = entry?;
          if !entry.file_type()?.is_dir() {
              continue;
          }
          let path = entry.path();
          let name = entry.file_name();
          let name = name.to_string_lossy();
          match name.as_ref() {
              "dead_letter" => (r.dead_letter_files, r.dead_letter_bytes) = dir_files_and_bytes(&path)?,
              "quarantine" => (r.quarantine_files, r.quarantine_bytes) = dir_files_and_bytes(&path)?,
              n => {
                  let Some(shard) = n.strip_prefix("shard_").and_then(|s| s.parse::<u16>().ok()) else { continue };
                  scan_shard(&path, shard, cache, &mut seen, &mut r)?;
              }
          }
      }
      cache.entries.retain(|p, _| seen.contains(p));
      Ok(r)
  }

  fn scan_shard(dir: &Path, shard: u16, cache: &mut FooterCache, seen: &mut HashSet<PathBuf>, r: &mut LagReport) -> io::Result<()> {
      for entry in std::fs::read_dir(dir)? {
          let path = entry?.path();
          let Some(file) = path.file_name().and_then(|f| f.to_str()) else { continue };
          if file.ends_with(EXT_SEALED) {
              if confirmed_path_for(&path).exists() {
                  continue; // delivered; the drain deletes it after its sidecar
              }
              let meta = match cache.entries.get(&path) {
                  Some(m) => *m,
                  None => {
                      let records = match crate::wab::read_segment_record_count(&path) {
                          Ok(n) => n,
                          Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                          // Unreadable footer: count the segment, not its records.
                          Err(_) => 0,
                      };
                      let created_ms = header_created_ms(&path).unwrap_or(None);
                      let m = SealedMeta { records, created_ms };
                      if cache.len() < FOOTER_CACHE_CAP {
                          cache.entries.insert(path.clone(), m);
                      }
                      m
                  }
              };
              seen.insert(path);
              r.undelivered_segments += 1;
              r.undelivered_records += meta.records;
              if let Some(c) = meta.created_ms {
                  r.oldest_created_ms = Some(r.oldest_created_ms.map_or(c, |o| o.min(c)));
              }
          } else if file.ends_with(EXT_ACTIVE)
              && let Ok(Some(c)) = header_created_ms(&path)
          {
              // A let-chain, not a nested `if let`: clippy's `collapsible_if`
              // lints the nested form on edition 2024 / MSRV 1.88.
              r.active_created_ms.push((shard, c));
          }
      }
      Ok(())
  }
  ```
  Add to `PollStatus` the fields `footer_cache: Option<FooterCache>` (init `Some(FooterCache::default())`) and `lag_failing: bool` (init `false`), and:
  ```rust
  impl PollStatus {
      /// Runs the lag scan off the runtime (spawn_blocking). The cache moves
      /// into the blocking task and back. A failure keeps the last report
      /// (stale, never zero) and WARNs once per failing episode.
      pub(crate) async fn refresh_lag(&mut self, now_ms: u64) {
          let mut cache = self.footer_cache.take().unwrap_or_default();
          let dir = self.wab_dir.clone();
          let joined = tokio::task::spawn_blocking(move || {
              let result = scan_wab_lag(&dir, &mut cache, now_ms);
              (cache, result)
          })
          .await;
          match joined {
              Ok((cache, Ok(lag))) => {
                  self.footer_cache = Some(cache);
                  self.board.wab.set_lag(lag);
                  self.lag_failing = false;
              }
              Ok((cache, Err(e))) => {
                  self.footer_cache = Some(cache);
                  if !self.lag_failing {
                      warn!(error = %e, "WAB lag scan failed; backlog figures stay at their last value until it succeeds");
                  }
                  self.lag_failing = true;
              }
              // Cancelled at runtime shutdown; the cache is rebuilt on the next scan.
              Err(_) => {}
          }
      }
  }
  ```
  (`EXT_ACTIVE` is `.wab` and `EXT_SEALED` is `.wab.sealed`. The sealed check runs first because `.wab.sealed` does not end with `.wab`, so the order does not matter for correctness, but it reads naturally.)
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server status::poll -- --test-threads=1`. Expected: all pass (3 new).
- [ ] **Step 5: Commit.** `git commit -am "feat(status): a read-only WAB lag scan with a bounded footer cache"`

---

### Task 1.22: Counter windows, evaluation, latches, status gauges, transition logs, and the 500 ms task

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/status/poll.rs`: `RecentWindow`, the `PollStatus` fields `windows` and `prev_active`, `refresh_recent`, `publish`, `log_transitions`, `publish_gauges`, `update_channel_high`, `spawn_queue_depth_task`, and the `tick` tail.
- Modify: `crates/weir-server/src/status/poll.rs` (`DrainLiveness::frozen_for`)
- Modify: `crates/weir-server/src/testutil.rs` (add `capture_logs`, `#[cfg(test)]`)
- Test: `crates/weir-server/src/status/poll.rs`

**Interfaces:**
- Consumes: `weir_status::{evaluate, readiness, liveness, CATALOGUE, ACK_TIMEOUT_WINDOW_SECS, FSYNC_WINDOW_SECS, PANIC_WINDOW_SECS}`, `StatusBoard::evidence_at`, the Task 1.18 metric fields, `queue::QueueSender`.
- Produces:
  ```rust
  pub(crate) struct RecentWindow; impl RecentWindow { pub(crate) fn new(start: Instant) -> Self;
      pub(crate) fn delta(&mut self, now: Instant, current: u64, window: Duration) -> u64; }
  impl DrainLiveness { pub(crate) fn frozen_for(&self, now: Instant) -> Duration; }
  pub(crate) fn publish_gauges(m: &Metrics, ev: &Evidence, eval: &Evaluation, ready: bool, live: bool);
  pub(crate) fn update_channel_high(board: &StatusBoard, now_ms: u64, high_depth: u64);
  pub(crate) fn spawn_queue_depth_task(queue_tx: QueueSender<WorkUnit>, metrics: Arc<Metrics>, board: Arc<StatusBoard>) -> tokio::task::JoinHandle<()>;
  #[cfg(test)] pub(crate) fn testutil::capture_logs<R>(f: impl FnOnce() -> R) -> (R, String);
  ```
  PR 3 adds, after the line `metrics.queue_depth.set(depth as f64);` in `spawn_queue_depth_task`, the `weir_drain_channel_depth` gauge. Its depth source (`board.drain_depth`) and `update_channel_high` are already wired here.

- [ ] **Step 1: Write the failing tests** (append to poll.rs `tests`):
  ```rust
  use weir_status::{ConditionCode, Phase, ListenerState};

  #[test]
  fn a_window_counts_increases_within_it_and_forgets_older_ones() {
      let t0 = Instant::now();
      let mut w = RecentWindow::new(t0);
      assert_eq!(w.delta(t0 + secs(5), 2, secs(300)), 2, "younger than the window: counted since process start");
      assert_eq!(w.delta(t0 + secs(200), 2, secs(300)), 2);
      assert_eq!(w.delta(t0 + secs(400), 2, secs(300)), 0, "no increase in the last 300 s");
      assert_eq!(w.delta(t0 + secs(405), 5, secs(300)), 3);
  }

  #[test]
  fn the_channel_high_water_mark_is_stamped_once_and_cleared_below() {
      let b = StatusBoard::for_test();
      update_channel_high(&b, 100, 230);
      assert_eq!(b.channel_high_since_ms.load(Ordering::Relaxed), 0, "unpublished depth is not high");
      b.drain_depth.store(240, Ordering::Relaxed);
      update_channel_high(&b, 100, 230);
      update_channel_high(&b, 200, 230);
      assert_eq!(b.channel_high_since_ms.load(Ordering::Relaxed), 100);
      b.drain_depth.store(10, Ordering::Relaxed);
      update_channel_high(&b, 300, 230);
      assert_eq!(b.channel_high_since_ms.load(Ordering::Relaxed), 0);
  }

  fn serving_board() -> Arc<StatusBoard> {
      let b = crate::status::test_board(2, 1);
      b.shards.mark_up(0);
      b.shards.mark_up(1);
      let guard = crate::status::WorkerGuard::enter(Arc::clone(&b.shards), 0);
      std::mem::forget(guard); // keep the worker "running" for the test's life
      b.listeners.unix.set(ListenerState::Bound);
      b.life.enter(Phase::Serving, unix_ms_now());
      b
  }

  #[tokio::test]
  async fn one_tick_evaluates_publishes_gauges_and_latches() {
      let root = crate::testutil::scratch_dir("poll_full_tick");
      let _ = std::fs::remove_dir_all(&root);
      crate::testutil::mkdir_p(root.join("shard_00"));
      sealed(&root.join("shard_00"), 1, 2);
      let board = serving_board();
      let (m, reg) = Metrics::new();
      m.init_topology(2, 1);
      let m = Arc::new(m);
      let mut poll = poll_for(&board, &m, 0, root);
      poll.tick(Instant::now(), || async { Ok(Some(10)) }).await;
      assert_eq!(m.ready.get(), 1.0);
      assert_eq!(m.live.get(), 1.0);
      assert_eq!(m.wab_undelivered_segments.get(), 1.0);
      assert!(board.heartbeat_changed_ms.load(Ordering::Relaxed) > 0);
      assert_eq!(board.drain_busy_since_ms.load(Ordering::Relaxed), 0, "an idle drain publishes no busy stamp");
      assert_eq!(board.recent.load_full().ack_timeouts_5m, Some(0));
      // The test board's ConfigView is the noop sink: one active condition.
      assert!(board.latches.since(ConditionCode::NoopSink).is_some());
      let mut out = String::new();
      prometheus_client::encoding::text::encode(&mut out, &reg).unwrap();
      assert!(out.contains("weir_condition_active{code=\"noop_sink\",severity=\"warning\"} 1"), "{out}");
      assert!(out.contains("weir_wab_shard_up{shard=\"1\"} 1"));
      assert!(out.contains("weir_worker_up{worker=\"0\"} 1"));
  }

  #[tokio::test]
  async fn condition_transitions_are_logged_once_each_way() {
      let board = serving_board();
      let m = Arc::new(Metrics::new().0);
      let mut st = PollStatus::new(Arc::clone(&board), std::path::PathBuf::from("/nonexistent"), Arc::new(AtomicU64::new(200)));
      st.refresh_recent(Instant::now(), &m);
      let (_, first) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
      assert!(first.contains("condition raised") && first.contains("noop_sink"), "{first}");
      let (_, second) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
      assert!(!second.contains("condition raised"), "no repeat while it stays active: {second}");
      board.wab_cap_rejecting.store(true, Ordering::Relaxed);
      let (_, third) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
      assert!(third.contains("WARN") && third.contains("wab_cap_rejecting"), "{third}");
      board.wab_cap_rejecting.store(false, Ordering::Relaxed);
      let (_, fourth) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
      assert!(fourth.contains("condition cleared") && fourth.contains("wab_cap_rejecting"), "{fourth}");
      assert_eq!(m.ready.get(), 1.0);
  }

  #[test]
  fn stranded_since_last_success_rebases_on_each_new_success() {
      let board = StatusBoard::for_test();
      let m = Metrics::new().0;
      let mut st = PollStatus::new(Arc::clone(&board), std::path::PathBuf::from("/nonexistent"), Arc::new(AtomicU64::new(200)));
      st.refresh_recent(Instant::now(), &m);
      assert_eq!(board.recent.load_full().stranded_since_last_success, None, "unpublished sink cell");
      board.sink.record_success();
      m.drain_segments_stranded.inc();
      st.refresh_recent(Instant::now(), &m);
      assert_eq!(board.recent.load_full().stranded_since_last_success, Some(1));
      // `record_success` stamps whole milliseconds; make the second one distinct.
      std::thread::sleep(Duration::from_millis(2));
      board.sink.record_success();
      st.refresh_recent(Instant::now(), &m);
      st.refresh_recent(Instant::now(), &m);
      assert_eq!(board.recent.load_full().stranded_since_last_success, Some(0));
  }

  #[tokio::test]
  async fn a_failed_size_scan_still_evaluates() {
      let board = serving_board();
      let m = Arc::new(Metrics::new().0);
      let mut poll = poll_for(&board, &m, 0, std::path::PathBuf::from("/nonexistent"));
      poll.tick(Instant::now(), || async { Ok(None) }).await;
      assert!(board.latches.since(ConditionCode::WabScanStale).is_some(), "the status steps ran without a size scan");
  }
  ```
  (`since` is the `SinceLookup` method; add `use weir_status::SinceLookup;` to the test module.)
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server status::poll::tests -- --test-threads=1`. Expected: `cannot find struct `RecentWindow``, `cannot find function `update_channel_high``, `no method named `refresh_recent` / `publish``, `cannot find function `capture_logs``.
- [ ] **Step 3: Implement.**
  `testutil.rs`:
  ```rust
  /// Runs `f` with a thread-local tracing subscriber writing to a buffer, and
  /// returns its result and everything logged (no ANSI, TRACE and up). For
  /// tests that assert on log lines; `with_default` is per thread, so `f` must
  /// log on the calling thread (a current-thread `block_on` inside is fine).
  #[cfg(test)]
  pub(crate) fn capture_logs<R>(f: impl FnOnce() -> R) -> (R, String) {
      use std::sync::{Arc, Mutex};
      #[derive(Clone, Default)]
      struct Buf(Arc<Mutex<Vec<u8>>>);
      impl std::io::Write for Buf {
          fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
              self.0.lock().unwrap().extend_from_slice(b);
              Ok(b.len())
          }
          fn flush(&mut self) -> std::io::Result<()> {
              Ok(())
          }
      }
      let buf = Buf::default();
      let writer = buf.clone();
      let subscriber = tracing_subscriber::fmt()
          .with_ansi(false)
          .with_max_level(tracing::Level::TRACE)
          .with_writer(move || writer.clone())
          .finish();
      let r = tracing::subscriber::with_default(subscriber, f);
      let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
      (r, out)
  }
  ```
  poll.rs:
  ```rust
  use crate::metrics::{ConditionLabel, ShardLabel, SinkFailureLabel, SinkFailureValue, WorkerLabel};
  use crate::models::WorkUnit;
  use crate::queue::QueueSender;
  use std::collections::{BTreeSet, VecDeque};
  use std::time::Instant;
  use weir_status::{ConditionCode, Evaluation, Evidence, RecentCounters, Severity, ShardState};

  /// Counter increase over a sliding window, from 5 s samples. A baseline
  /// sample (start, 0) makes "younger than the window" count from process
  /// start, which is when every counter was 0.
  pub(crate) struct RecentWindow {
      samples: VecDeque<(Instant, u64)>,
  }

  impl RecentWindow {
      pub(crate) fn new(start: Instant) -> Self {
          RecentWindow { samples: VecDeque::from([(start, 0)]) }
      }
      pub(crate) fn delta(&mut self, now: Instant, current: u64, window: Duration) -> u64 {
          self.samples.push_back((now, current));
          if let Some(cutoff) = now.checked_sub(window) {
              while self.samples.len() >= 2 && self.samples[1].0 <= cutoff {
                  self.samples.pop_front();
              }
          }
          current.saturating_sub(self.samples[0].1)
      }
  }

  struct RecentWindows {
      ack_timeouts: RecentWindow,
      fsync_failures: RecentWindow,
      flusher_panics: RecentWindow,
  }

  fn age_secs(now_ms: u64, then_ms: u64) -> f64 {
      now_ms.saturating_sub(then_ms) as f64 / 1000.0
  }

  /// Every gauge the evaluation and the evidence feed (spec §5.11 step 6).
  pub(crate) fn publish_gauges(m: &Metrics, ev: &Evidence, eval: &Evaluation, ready: bool, live: bool) {
      let now = ev.now_unix_ms;
      m.ready.set(flag(ready));
      m.live.set(flag(live));
      for e in weir_status::CATALOGUE {
          m.condition_active.get_or_create(&ConditionLabel::of(e)).set(flag(eval.is_active(e.code)));
      }
      for s in ev.shards.iter().flatten() {
          let l = ShardLabel::of(usize::from(s.id));
          m.wab_shard_up.get_or_create(&l).set(flag(matches!(s.state, ShardState::Up | ShardState::Respawning)));
          m.wab_handoff_blocked_seconds
              .get_or_create(&l)
              .set(s.handoff_blocked_since_unix_ms.map_or(0.0, |t| age_secs(now, t)));
          m.wab_active_segment_age_seconds.get_or_create(&l).set(s.active_segment_age_secs.unwrap_or(0.0));
      }
      for w in ev.workers.iter().flatten() {
          m.worker_up.get_or_create(&WorkerLabel::of(usize::from(w.id))).set(flag(w.running));
      }
      if let Some(c) = &ev.wab_cap {
          m.wab_cap_rejecting.set(flag(c.rejecting));
      }
      if let Some(b) = &ev.backlog {
          m.wab_undelivered_segments.set(b.undelivered_segments as f64);
          m.wab_undelivered_records.set(b.undelivered_records as f64);
          m.wab_oldest_undelivered_age_seconds.set(b.oldest_undelivered_age_secs.unwrap_or(0.0));
          // Cadence change (spec §13): the 5 s task now also sets these.
          m.dead_letter_bytes_on_disk.set(b.dead_letter_bytes as f64);
          m.quarantine_bytes_on_disk.set(b.quarantine_bytes as f64);
      }
      if let Some(d) = &ev.drain {
          m.drain_commit_in_flight_seconds.set(d.commit_in_flight_since_unix_ms.map_or(0.0, |t| age_secs(now, t)));
      }
      if let Some(s) = &ev.sink {
          m.sink_last_success_timestamp_seconds.set(s.last_success_unix_ms.map_or(0.0, |t| t as f64 / 1000.0));
          m.sink_consecutive_failures.set(s.consecutive_failures as f64);
          if let (Some(t), Some(k)) = (s.last_failure_unix_ms, s.last_failure_kind) {
              m.sink_last_failure_timestamp_seconds
                  .get_or_create(&SinkFailureLabel { kind: SinkFailureValue::of(k) })
                  .set(t as f64 / 1000.0);
          }
      }
  }

  /// Stamps when the drain channel rose to `high_depth` and clears it below
  /// (spec §9.3 code 8's "≥ 230 for 10 s"). Unpublished depth is not high.
  pub(crate) fn update_channel_high(board: &StatusBoard, now_ms: u64, high_depth: u64) {
      let depth = board.drain_depth.load(Ordering::Relaxed);
      if depth != crate::status::board::DEPTH_UNPUBLISHED && depth as u64 >= high_depth {
          let _ = board.channel_high_since_ms.compare_exchange(0, now_ms.max(1), Ordering::Relaxed, Ordering::Relaxed);
      } else {
          board.channel_high_since_ms.store(0, Ordering::Relaxed);
      }
  }

  /// The 500 ms task (spec §5.11): queue depth into the gauge and the board,
  /// and the channel high-water stamp. Holds a `queue_tx` clone: main aborts
  /// and awaits it at shutdown (from PR 4; today `drop(rt)` releases it).
  pub(crate) fn spawn_queue_depth_task(
      queue_tx: QueueSender<WorkUnit>,
      metrics: Arc<Metrics>,
      board: Arc<StatusBoard>,
  ) -> tokio::task::JoinHandle<()> {
      tokio::spawn(async move {
          let mut interval = tokio::time::interval(Duration::from_millis(500));
          let high = weir_status::Thresholds::DEFAULT.channel_high_depth;
          loop {
              interval.tick().await;
              let depth = queue_tx.len();
              metrics.queue_depth.set(depth as f64);
              board.queue_depth.store(depth as u64, Ordering::Relaxed);
              update_channel_high(&board, unix_ms_now(), high);
          }
      })
  }
  ```
  `PollStatus` gains the fields `windows: RecentWindows`, initialised with `RecentWindow::new(Instant::now())` ×3; `prev_active: BTreeSet<ConditionCode>`, initialised empty; and, for spec §9.3 code 18 (`segments_stranded`), `stranded_baseline: u64` (0), `stranded_prev: u64` (0) and `seen_success_ms: Option<u64>` (None). Methods:
  ```rust
  impl PollStatus {
      pub(crate) fn refresh_recent(&mut self, now: Instant, metrics: &Metrics) {
          // Strandings since the last sink success. When a new success shows up,
          // rebase on the count at the previous tick, so a strand inside this
          // window is never hidden (errs toward firing). `None` while the sink
          // cell is unpublished: the rule then stays unevaluated. PR 2 Task 2.2
          // (`Publisher::sink_success`) publishes the cell; nothing else is needed.
          let stranded_now = metrics.drain_segments_stranded.get();
          let stranded = self.board.sink.evidence().map(|s| {
              if s.last_success_unix_ms != self.seen_success_ms {
                  self.seen_success_ms = s.last_success_unix_ms;
                  self.stranded_baseline = self.stranded_prev;
              }
              stranded_now.saturating_sub(self.stranded_baseline)
          });
          self.stranded_prev = stranded_now;
          let w = &mut self.windows;
          let recent = RecentCounters {
              ack_timeouts_5m: Some(w.ack_timeouts.delta(now, metrics.ack_timeout.get(), Duration::from_secs(weir_status::ACK_TIMEOUT_WINDOW_SECS))),
              fsync_failures_15m: Some(w.fsync_failures.delta(now, metrics.wab_fsync_failures.get(), Duration::from_secs(weir_status::FSYNC_WINDOW_SECS))),
              flusher_panics_15m: Some(w.flusher_panics.delta(now, metrics.wab_flusher_panics.get(), Duration::from_secs(weir_status::PANIC_WINDOW_SECS))),
              stranded_since_last_success: stranded,
              drain_panics_total: Some(metrics.drain_panics.get()),
              recovery_segments_failed: Some(metrics.recovery_segments_failed.get()),
              recovery_quarantine_copy_failed: Some(metrics.recovery_quarantine_copy_failed.get()),
              coalesce_ewma_us: Some(self.coalesce_hint.load(Ordering::Relaxed)),
          };
          self.board.recent.store(Arc::new(recent));
      }

      /// Spec §5.11 steps 6-7: evaluate, latch, publish, log transitions.
      pub(crate) fn publish(&mut self, now_ms: u64, metrics: &Metrics) -> Evaluation {
          let ev = self.board.evidence_at(now_ms);
          let eval = weir_status::evaluate(&ev, &self.thresholds, &self.board.latches);
          self.board.latches.update(&eval, now_ms);
          let ready = weir_status::readiness(&ev).ok;
          // PR 5 passes the configured livez_checks; none exist yet, so `[]`.
          let live = weir_status::liveness(&ev, &self.thresholds, &[]).ok;
          publish_gauges(metrics, &ev, &eval, ready, live);
          self.log_transitions(&eval);
          eval
      }

      /// Raising a warning or critical condition WARNs; raising an info one
      /// (e.g. `starting` on every boot) and every clear log at INFO.
      fn log_transitions(&mut self, eval: &Evaluation) {
          let now: BTreeSet<ConditionCode> = eval.conditions.iter().map(|c| c.code).collect();
          for c in eval.conditions.iter().filter(|c| !self.prev_active.contains(&c.code)) {
              if c.severity == Severity::Info {
                  info!(code = c.code.as_str(), severity = "info", "condition raised: {}", c.summary);
              } else {
                  warn!(code = c.code.as_str(), severity = ?c.severity, "condition raised: {}", c.summary);
              }
          }
          for code in self.prev_active.difference(&now) {
              info!(code = code.as_str(), "condition cleared");
          }
          self.prev_active = now;
      }
  }
  ```
  `DrainLiveness`:
  ```rust
  pub(crate) fn frozen_for(&self, now: std::time::Instant) -> Duration {
      now.saturating_duration_since(self.heartbeat_changed_at)
  }
  ```
  `WabPoll::tick`: insert after the closing brace of `if let Some(bytes) = apply_wab_scan(..) { … }` and before the final `logged` (not inside that block, or a failed size scan would skip evaluation and freeze `wab_scan_stale`, `weir_ready` and the latches; `a_failed_size_scan_still_evaluates` catches that):
  ```rust
  // Status steps (spec §5.11 3-6). They run on every tick, a failed size
  // scan included: the lag scan and the counters are independent of it.
  if let Some(mut st) = self.status.take() {
      let now_ms = unix_ms_now();
      let frozen = self.liveness.frozen_for(now);
      st.board
          .heartbeat_changed_ms
          .store(now_ms.saturating_sub(u64::try_from(frozen.as_millis()).unwrap_or(u64::MAX)).max(1), Ordering::Relaxed);
      // The busy signal, read after the size scan so the stamp is current; the
      // same reading `check_drain_liveness` classifies, one tick later at worst.
      match self.heartbeat.busy() {
          Some((kind, busy_for)) => {
              st.board.drain_busy_sink_call.store(kind.is_sink_call(), Ordering::Relaxed);
              let since = now_ms.saturating_sub(u64::try_from(busy_for.as_millis()).unwrap_or(u64::MAX)).max(1);
              st.board.drain_busy_since_ms.store(since, Ordering::Release);
          }
          None => st.board.drain_busy_since_ms.store(0, Ordering::Release),
      }
      self.metrics.drain_heartbeat_age_seconds.set(frozen.as_secs_f64());
      st.refresh_lag(now_ms).await;
      st.refresh_recent(now, &self.metrics);
      st.publish(now_ms, &self.metrics);
      self.status = Some(st);
  }
  ```
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server status:: -- --test-threads=1`. Expected: all pass (6 new), and the lock scan (now including poll.rs) still passes. `capture_logs` lives in testutil.rs, outside status/.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): the poll task evaluates conditions, latches them and publishes the status gauges"`

---
### Task 1.23: Wire the board into `main` and the Unix listener

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/main.rs`:
  - metrics block (:976-987);
  - WAB and worker spawn (:1065-1109);
  - replay (:1417-1432);
  - runtime body (:1434-1699, `Ok::<(), std::io::Error>(())` at :1698): the reloader's `tls_configured`, the 500 ms task, the 5 s task, the TCP block, the signal task, `SocketConfig` (:1665).
  - Checked against 3adc5f5: the lock-integrity thread is spawned at main.rs:989-1004, right after `Metrics::new` and `acquire_wab_dir_lock` (:973). Add the `init_topology` / `publish_*` calls inside the metrics block, and put the `// ── Status board` block after the integrity-thread block (before `// ── Preflight`, :1006), so nothing new runs between acquiring the lock and starting its integrity check.
- Modify: `crates/weir-server/src/socket/mod.rs`:
  - `SocketConfig` (:46-89) gains `board`;
  - `run` (:119-305) marks the Unix listener and the serving phase;
  - test literals at :1197 (`default_config`) and :2106 get `board: None`.
  - Checked against 3adc5f5: the stricter non-Linux probe lives in `probe_socket` (socket/mod.rs:810), called inside `bind_hardened`'s stale-socket path (:692) and from preflight.rs:73; wrapping `bind_hardened`'s error in `run` does not interact with it.
- Test: `crates/weir-server/src/socket/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: everything from Tasks 1.11-1.22.
- Produces:
  ```rust
  pub struct SocketConfig { /* existing fields */, pub board: Option<std::sync::Arc<crate::status::StatusBoard>> }
  // `socket::run` with `board: Some(b)`: after bind_hardened succeeds, b.listeners.unix = Bound and
  // b.enter_phase(Serving); a validation/bind failure sets Failed; on return, Closed.
  ```
  `main` holds `board: Arc<StatusBoard>`, created after the metrics registry and the lock-integrity thread, and before `wab::spawn`. PR 3 and PR 4 build on it. PR 3 keeps the `SocketConfig.board` hook across its bind/serve split with one writer per mark (README Q6): `socket::bind` marks the Unix listener `Bound`/`Failed`, `socket::bind_all` marks TCP `Bound`/`Failed`, and `socket::serve` enters `Serving` and marks Unix `Closed`. PR 4 Task 4.7 moves `serve`'s `Closed` mark to the lame duck and has `tcp::run` mark TCP `Closed`/`Failed`. `main` marks no listener, except in `discard_listeners` on the pre-serving path. In PR 1, `run` is both halves, so it writes the Unix marks itself.

- [ ] **Step 1: Write the failing test** (socket/mod.rs `mod tests`, next to `full_run_start_then_shutdown_removes_its_own_socket`):
  ```rust
  #[tokio::test]
  #[cfg(unix)]
  async fn run_marks_the_unix_listener_bound_then_serving_then_closed() {
      use weir_status::{ListenerState, Phase};
      let dir = umask_immune_dir("run_marks_board");
      let path = dir.join("weir.sock");
      let board = crate::status::test_board(1, 1);
      let mut cfg = default_config(path.clone());
      cfg.board = Some(std::sync::Arc::clone(&board));
      let (shutdown_tx, shutdown_rx) = oneshot::channel();
      let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
      let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
      assert_eq!(board.listeners.unix.get(), ListenerState::Pending);
      let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
      let deadline = Instant::now() + Duration::from_secs(3);
      while UnixStream::connect(&path).await.is_err() {
          assert!(Instant::now() < deadline, "run() never started listening");
          tokio::time::sleep(Duration::from_millis(5)).await;
      }
      assert_eq!(board.listeners.unix.get(), ListenerState::Bound);
      assert_eq!(board.life.phase(), Phase::Serving);
      assert!(board.life.ever_served());
      shutdown_tx.send(()).unwrap();
      time::timeout(Duration::from_secs(3), handle).await.unwrap().unwrap().unwrap();
      assert_eq!(board.listeners.unix.get(), ListenerState::Closed);
      std::fs::remove_dir_all(&dir).ok();
  }

  #[tokio::test]
  #[cfg(unix)]
  async fn a_bind_failure_marks_the_unix_listener_failed() {
      let board = crate::status::test_board(1, 1);
      let mut cfg = default_config(PathBuf::from("relative/weir.sock"));
      cfg.board = Some(std::sync::Arc::clone(&board));
      let (_shutdown_tx, shutdown_rx) = oneshot::channel();
      let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
      let sem = std::sync::Arc::new(Semaphore::new(1));
      assert!(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem).await.is_err());
      assert_eq!(board.listeners.unix.get(), weir_status::ListenerState::Failed);
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server socket::tests::run_marks -- --test-threads=1`. Expected: `no field `board` on type `SocketConfig``.
- [ ] **Step 3: Implement.**
  `socket/mod.rs`, `SocketConfig` gains a last field:
  ```rust
  /// The status board, when the daemon runs one. `run` marks the Unix
  /// listener Bound and the daemon Serving once the socket is bound (every
  /// other listener is bound before `run` is called), Failed on a bind
  /// error and Closed on return. PR 3's bind/serve split keeps the hook.
  pub board: Option<std::sync::Arc<crate::status::StatusBoard>>,
  ```
  In `run`, replace the first four statements (`validate_socket_path(..)?;`, `let bound = bind_hardened(..)?;`, `let listener = &bound.listener;` and the `info!(.., "socket listening")` line, socket/mod.rs:126-130) with:
  ```rust
  let mark = |state: weir_status::ListenerState| {
      if let Some(b) = &config.board {
          b.listeners.unix.set(state);
      }
  };
  validate_socket_path(&config.socket_path).inspect_err(|_| mark(weir_status::ListenerState::Failed))?;
  let bound = bind_hardened(&config.socket_path).inspect_err(|_| mark(weir_status::ListenerState::Failed))?;
  let listener = &bound.listener;

  info!(path = %config.socket_path.display(), "socket listening");
  if let Some(b) = &config.board {
      b.listeners.unix.set(weir_status::ListenerState::Bound);
      b.enter_phase(weir_status::Phase::Serving, &metrics);
  }
  ```
  Just before the final `Ok(())`: `mark(weir_status::ListenerState::Closed);`. The closure borrows `config` immutably and `run` reads `config` only immutably afterwards, so the borrow checker accepts it. If it does not (the template clones fields from `config`), inline the `if let` at the three sites instead. Add `board: None,` to both test literals.

  `main.rs`, metrics block:
  ```rust
  let (metrics_struct, registry) = metrics::Metrics::new();
  metrics_struct.init_topology(config.shard_count, config.worker_count);
  metrics_struct.publish_build_info(env!("CARGO_PKG_VERSION"), weir_core::WIRE_VERSION);
  metrics_struct.publish_config(&config);
  let metrics = Arc::new(metrics_struct);
  // (existing registry Arc and wab_dir_lock_held lines unchanged)
  ```
  Leave the `// ── wab_dir lock integrity` block (`wab_ownership_lost` and `spawn_lock_integrity_thread`) where it is, directly after the metrics block, and insert the following after it, before `// ── Preflight`:
  ```rust
  // ── Status board (spec §6 step 3) ─────────────────────────────────────────
  //
  // Created before the WAB so the flushers' supervisors and the workers'
  // drop guards publish into the same registry the poll task and (from PR 5)
  // the probes read. `wab_bytes_now` / `wab_cap_rejecting` move up from inside
  // the runtime: the startup cap gate below writes them before any listener
  // exists, and every listener clones these same Arcs.
  #[cfg(all(unix, feature = "tls"))]
  let tcp_configured = config.tcp_bind.is_some();
  #[cfg(not(all(unix, feature = "tls")))]
  let tcp_configured = false;
  let wab_bytes_now = Arc::new(AtomicU64::new(0));
  let wab_cap_rejecting = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let config_view = Arc::new(status::config_view::ConfigView::from(&config));
  let board = status::StatusBoard::new(
      status::board::StaticInfo::capture(&config, Arc::clone(&config_view)),
      config.shard_count,
      config.worker_count,
      tcp_configured,
      Arc::clone(&wab_cap_rejecting),
      Arc::clone(&wab_bytes_now),
  );
  board.enter_phase(weir_status::Phase::Initializing, &metrics);
  ```
  - WAB and workers:
    - put `board.enter_phase(weir_status::Phase::Recovering, &metrics);` immediately before `wab::spawn` (recovery runs inside it until PR 3 splits it);
    - delete `worker_registry` and pass `Arc::clone(&board.shards)` to both `wab::spawn` and `worker::spawn_workers_with(.., Some(Arc::clone(&board.shards)))`;
    - before the workers take `coalesce_hint`, add `let coalesce_hint_for_poll = Arc::clone(&coalesce_hint);`.
  - Replay: immediately before `wab::replay_unconfirmed(…)`, add `board.enter_phase(weir_status::Phase::Handoff, &metrics);`. After `drop(replay_tx);`:
    ```rust
    // Spec §6 step 9: the first size scan and cap evaluation run here,
    // synchronously, before any producer listener exists (fail-closed).
    status::poll::startup_cap_gate(&config.wab_dir, config.wab_max_bytes, &board, &metrics);
    ```
  - Inside `rt.block_on(async { … })`:
    1. Delete the `let tls_configured = …` pair and pass `tcp_configured` to `reload::Reloader::new`.
    2. Replace the 500 ms task (:1476-1486 at 3adc5f5; Task 1.19's move shifts every main.rs line, so locate it by its text) with
       ```rust
       let _queue_depth_task =
           status::poll::spawn_queue_depth_task(queue_tx.clone(), Arc::clone(&metrics), Arc::clone(&board));
       ```
    3. Delete `let wab_bytes_now = …` (:1496 at 3adc5f5) and `let wab_cap_rejecting = …` (:1502) together with their comments; the outer bindings are used instead.
    4. In the 5 s task, build the poll as
       ```rust
       let mut poll = status::poll::WabPoll::new(
           metrics_w,
           wab_bytes_bg,
           cap_for_warning,
           drain_heartbeat_for_poll,
           status::poll::DrainLiveness::new(
               std::time::Instant::now(),
               health_poll_interval_for_stall,
               commit_timeout_for_stall,
           ),
       )
       .with_status(status::poll::PollStatus::new(
           Arc::clone(&board),
           config.wab_dir.clone(),
           coalesce_hint_for_poll,
       ));
       ```
    5. Immediately before the `#[cfg(all(unix, feature = "tls"))] let (tcp_shutdown_tx, tcp_join) = {` block, add `board.enter_phase(weir_status::Phase::Binding, &metrics);`.
    6. In the TCP branch, after `info!(addr = %actual_addr, "TCP+mTLS listener bound");`, add `board.listeners.tcp.set(weir_status::ListenerState::Bound);`. Before `let tcp_join = tokio::spawn(async move {`, add `let tcp_board = Arc::clone(&board);`. Replace the `if let Err(e) = res { … }` inside it with:
       ```rust
       match res {
           Ok(()) => tcp_board.listeners.tcp.set(weir_status::ListenerState::Closed),
           Err(e) => {
               tcp_board.listeners.tcp.set(weir_status::ListenerState::Failed);
               tracing::error!(error = %e, "TCP+mTLS listener exited with error");
           }
       }
       ```
    7. Signal task: before `tokio::spawn(async move {`, add `let board_sig = Arc::clone(&board); let metrics_sig = Arc::clone(&metrics);`. Inside the spawned task, after both cfg blocks that log the signal and before `#[cfg(all(unix, feature = "tls"))] if let Some(tx) = tcp_shutdown_tx`, add:
       ```rust
       // T0 (spec §7 a-b): the board says shutting_down before any listener
       // closes, so a flusher whose input closes from here on reads as a clean
       // `Stopped`, not `Offline(FlusherExited)`. PR 4's Lifecycle takes this over.
       board_sig.begin_shutdown(&metrics_sig);
       ```
    8. In `SocketConfig { … }`, add `board: Some(Arc::clone(&board)),`.
    9. In the `#[cfg(not(unix))]` arm, before `let _ = shutdown_rx.await;`, add `board.enter_phase(weir_status::Phase::Serving, &metrics);`.
  - Add `use std::sync::atomic::AtomicU64;` if it is not already imported (main.rs:49 imports it).
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1`. Expected: all socket tests pass, including the 2 new ones.
  - `cargo test -p weir-server --test system -- --test-threads=1 sigterm socket_disappears smoke`. Expected: the existing shutdown and smoke system tests pass against the rebuilt binary.
- [ ] **Step 5: Commit.** `git commit -am "feat(status): wire the status board into startup, the listeners and the signal path"`

---

### Task 1.24: SP-D logging: one startup line with the ConfigView, ANSI only on a terminal, a WARN on a bad `log_level`

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Create: `crates/weir-server/src/logging.rs`
- Modify: `crates/weir-server/src/main.rs`:
  - add `mod logging;`;
  - the tracing init (:919-924);
  - the "weir starting" line (:953-959);
  - move `let config_view = …` from Task 1.23 up to just before that line.
- Test: `crates/weir-server/src/logging.rs` (`mod tests`), `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes: `status::config_view::ConfigView` (its `Debug`).
- Produces:
  ```rust
  pub(crate) fn logging::env_filter_for(level: &str) -> (tracing_subscriber::EnvFilter, Option<String>);
  pub(crate) fn logging::init(level: &str) -> Option<String>;   // returns the invalid-level warning to emit
  ```

- [ ] **Step 1: Write the failing tests.** `crates/weir-server/src/logging.rs` (test module only):
  ```rust
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
          assert!(w.contains("weir_server=notalevel") && w.contains("\"info\""), "{w}");
          assert_eq!(filter.to_string(), "info");
      }
  }
  ```
  `crates/weir-server/tests/system.rs`:
  ```rust
  /// SP-D (spec §5.13): the startup line names the version, the wire version
  /// and the effective config, and a log redirected to a file carries no
  /// ANSI escape codes.
  #[test]
  fn startup_log_names_version_and_config_and_carries_no_ansi() {
      let srv = weir_server!("startup_line").log_level("info").start();
      let log = fs::read_to_string(srv.tmp_dir.join("weir.log")).unwrap();
      let line = log.lines().find(|l| l.contains("weir starting")).unwrap_or_else(|| panic!("no startup line:\n{log}"));
      assert!(line.contains(&format!("version={}", env!("CARGO_PKG_VERSION"))), "{line}");
      assert!(line.contains("wire_version=1"), "{line}");
      assert!(line.contains("\"wab_dir_lock\": \"required\""), "the ConfigView rides the line: {line}");
      assert!(!log.contains('\u{1b}'), "no ANSI escapes in a file:\n{log}");
  }
  ```
  Add `mod logging;` to main.rs.
- [ ] **Step 2: Run it to verify it fails.**
  - `cargo test -p weir-server --bin weir-server logging:: -- --test-threads=1`. Expected: `cannot find function `env_filter_for``.
  - `cargo test -p weir-server --test system startup_log_names -- --test-threads=1`. Expected: FAILS on `version=` (absent today). The ANSI assertion fails too: tracing-subscriber's default `with_ansi(true)` writes escapes into weir.log.
- [ ] **Step 3: Implement.** `logging.rs`, above the tests:
  ```rust
  //! Tracing setup (spec §5.13, SP-D). No new dependencies.
  use std::io::IsTerminal;
  use tracing_subscriber::EnvFilter;

  /// The filter for `log_level`, and the warning to log when it does not
  /// parse. Previously an invalid level fell back to `info` silently
  /// (main.rs:919-920), so a typo looked like a working config.
  pub(crate) fn env_filter_for(level: &str) -> (EnvFilter, Option<String>) {
      match EnvFilter::try_new(level) {
          Ok(f) => (f, None),
          Err(e) => (
              EnvFilter::new("info"),
              Some(format!("log_level {level:?} is not a valid filter ({e}); logging at \"info\" instead")),
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
  ```
  `main.rs`, replacing :919-924:
  ```rust
  let log_level_warning = logging::init(&config.log_level);
  if let Some(w) = log_level_warning {
      warn!("{w}");
  }
  ```
  `main.rs`, replacing the "weir starting" `info!` (:953-959) and moving the ConfigView up from Task 1.23:
  ```rust
  // One line with everything needed to reproduce the run (spec §5.13): the
  // ConfigView carries no secret (the S3 secret is never entered, the sink URL
  // is reduced to scheme://host[:port]).
  let config_view = Arc::new(status::config_view::ConfigView::from(&config));
  // `%` records the version with Display: a bare `&str` field is formatted
  // with Debug by tracing-subscriber and would print `version="4.1.0"`.
  info!(
      version = %env!("CARGO_PKG_VERSION"),
      wire_version = weir_core::WIRE_VERSION,
      socket = %config.socket_path.display(),
      wab_dir = %config.wab_dir.display(),
      shards = config.shard_count,
      workers = config.worker_count,
      config = ?config_view,
      "weir starting"
  );
  ```
  Delete the later `let config_view = …` line that Task 1.23 added.
- [ ] **Step 4: Run to verify it passes.**
  - `cargo test -p weir-server --bin weir-server logging:: -- --test-threads=1`. Expected: 2 passed.
  - `cargo test -p weir-server --test system startup_log_names -- --test-threads=1`. Expected: passes. The existing `log.contains("weir starting")` system tests (system.rs:2318, :2575) still pass: the message text is unchanged.
- [ ] **Step 5: Commit.** `git add crates/weir-server && git commit -am "feat(logging): one startup line with the config view; colour only on a terminal; warn on a bad log_level"`

---

### Task 1.25: SP-D logging in the drain: segment identity on commit-path lines, and a WARN when a dead-letter-blocked episode starts

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs`:
  - `process_segment`, the two `commit_batch(…).await` calls (:2150, :2170);
  - `enter_blocked` (:1917-1940);
  - one `use tracing::Instrument;`;
  - two tests appended to `mod tests`. The PR 2 additions-only gate is measured against PR 2's own merge base, which already contains these.
  - Checked against 2491883: the `(dev, ino)` pin checks live in drain/ledger.rs (:295, :337, :515) and the 10-minute escalation in `local_read_escalation_due` (drain/mod.rs:1838); neither is at the two `commit_batch(..).await` sites (:2150, :2170 in `process_segment`, :1953), which are the only lines this task changes there.
- Test: `crates/weir-server/src/drain/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: `crate::testutil::capture_logs` (Task 1.22); the existing `segment_identity`, `make_sealed_segment`, `fast_config`, `noop_metrics`, `MockSink::with_responses`, `MockError::Transient`, `block_on`, `tmp_dir`, and the test-shim `process_segment(seg, sink, cfg, metrics, dl, skip)`.
- Produces: no new items. Behaviour:
  - every log line emitted inside `commit_batch` carries the span `segment{segment=shard_NN/seg_NNNNNNNN.wab.sealed}`;
  - `enter_blocked` WARNs once per episode.
  - The drain panic payload is logged by PR 2 (reconciliation 5).

- [ ] **Step 1: Write the failing tests** (append inside drain/mod.rs `mod tests`):
  ```rust
  #[test]
  fn commit_path_log_lines_name_the_segment() {
      use super::dead_letter::DeadLetterWriter;
      let dir = tmp_dir("commit_log_segment");
      let segment = make_sealed_segment(&dir, 0, &[b"r1"]);
      let mut dl = DeadLetterWriter::open(&dir).unwrap();
      let config = fast_config(dir.clone());
      let metrics = noop_metrics();
      let sink = MockSink::with_responses([Err(MockError::Transient)]);
      let (_, logs) = crate::testutil::capture_logs(|| {
          block_on(process_segment(&segment, &sink, &config, &metrics, &mut dl, 0))
      });
      let line = logs
          .lines()
          .find(|l| l.contains("transient sink error"))
          .unwrap_or_else(|| panic!("no transient line:\n{logs}"));
      assert!(line.contains("shard_00/seg_00000001"), "the commit line must name its segment: {line}");
  }

  #[test]
  fn a_dead_letter_blocked_episode_warns_once_when_it_starts() {
      let metrics = noop_metrics();
      let mut episode = None;
      let seg = PathBuf::from("/w/shard_00/seg_00000004.wab.sealed");
      let (_, first) = crate::testutil::capture_logs(|| super::enter_blocked(seg.clone(), &metrics, &mut episode, 0, 4096));
      assert!(
          first.contains("WARN") && first.contains("dead_letter/ is full") && first.contains("shard_00/seg_00000004"),
          "{first}"
      );
      let (_, again) = crate::testutil::capture_logs(|| super::enter_blocked(seg.clone(), &metrics, &mut episode, 0, 4096));
      assert!(!again.contains("dead_letter/ is full"), "the same episode does not repeat it: {again}");
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::commit_path_log drain::tests::a_dead_letter_blocked`. Expected: both FAIL (no segment on the line; no WARN at episode start).
- [ ] **Step 3: Implement.** drain/mod.rs: add `use tracing::Instrument;` to the imports. In `process_segment`, right after `let segment_name = segment_identity(segment);` (:2059), add:
  ```rust
  // SP-D (spec §5.13): every line logged inside a commit names the segment.
  // ERROR level so the span is enabled under any `log_level` that lets a
  // commit-path WARN or ERROR through.
  let segment_span = tracing::error_span!("segment", segment = %segment_name);
  ```
  At both `commit_batch(` call sites (:2150 and :2170), change `)\n.await;` to `)\n.instrument(segment_span.clone())\n.await;`.

  In `enter_blocked`, inside the `get_or_insert_with` closure before `Instant::now()`:
  ```rust
  warn!(
      segment = %segment_identity(&segment),
      needed_bytes = needed,
      "drain: dead_letter/ is full; delivery is blocked until space is freed \
       (weir-ctl dl requeue / dl drop) or dead_letter_max_bytes is raised. Ingest \
       continues and the WAB grows (condition drain_blocked_dead_letter_full)"
  );
  ```
  (`warn!` is already imported in drain/mod.rs. The closure borrows `segment` before it moves into the returned state.)
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server drain:: -- --test-threads=1`. Expected: all drain tests pass (2 new).
- [ ] **Step 5: Commit.** `git commit -am "feat(drain): commit-path log lines name their segment; a dead-letter-blocked episode warns when it starts"`

---

### Task 1.26: `weir_records_nack_internal_total{cause}` at every transient nack site

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/src/socket/connection.rs`:
  - the admission arms (:363-385, :413-432);
  - `handle_push` (:567-713);
  - `handle_push_batch` (:727-835).
- Test: `crates/weir-server/src/socket/connection.rs` (`mod tests`)

**Interfaces:**
- Consumes: `crate::metrics::{NackInternalCause, NackInternalLabel}` (Task 1.18).
- Produces: `fn count_internal_nack(metrics: &Metrics, cause: NackInternalCause, n: u64)` (private). Wire bytes are unchanged; only counters move. The sum over causes equals the transient `InternalError` nacks at the push sites. The decode-error fallback (`nack_for_decode_error`'s `_` arm) is a framing failure and is not counted here.

- [ ] **Step 1: Write the failing tests** (append to connection.rs `mod tests`):
  ```rust
  use crate::metrics::{NackInternalCause, NackInternalLabel};

  fn internal(m: &Metrics, cause: NackInternalCause) -> u64 {
      m.records_nack_internal.get_or_create(&NackInternalLabel { cause }).get()
  }

  async fn push_once(cfg: ConnectionConfig, ack: Option<bool>) -> Arc<Metrics> {
      let m = Arc::new(Metrics::new().0);
      let mut c = spawn_handler_acking_with(cfg, Arc::clone(&m), ack).await;
      c.write_all(&push_frame(b"x")).await.unwrap();
      assert_eq!(read_response(&mut c).await.0, MessageType::Nack);
      m
  }

  #[tokio::test]
  async fn flusher_side_nacks_name_their_cause() {
      let m = push_once(test_cfg(), Some(false)).await;
      assert_eq!(internal(&m, NackInternalCause::write_failed), 1);
      let m = push_once(test_cfg(), None).await;
      assert_eq!(internal(&m, NackInternalCause::sender_dropped), 1);
      assert_eq!(internal(&m, NackInternalCause::write_failed), 0);
  }

  #[tokio::test]
  async fn admission_nacks_name_their_cause() {
      let cap = ConnectionConfig {
          wab_max_bytes: 1_000,
          wab_bytes_now: Arc::new(AtomicU64::new(5_000)),
          wab_cap_rejecting: Arc::new(AtomicBool::new(false)),
          ..test_cfg()
      };
      assert_eq!(internal(&push_once(cap, Some(true)).await, NackInternalCause::wab_cap), 1);
      let lost = ConnectionConfig { wab_ownership_lost: Arc::new(AtomicBool::new(true)), ..test_cfg() };
      assert_eq!(internal(&push_once(lost, Some(true)).await, NackInternalCause::ownership_lost), 1);
  }

  #[tokio::test]
  async fn queue_and_timeout_nacks_name_their_cause() {
      // Queue: no receivers, so the push fails at once.
      let (client, server) = UnixStream::pair().unwrap();
      let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
      drop(queue_rx);
      let m = Arc::new(Metrics::new().0);
      tokio::spawn(handle_connection(server, queue_tx, test_cfg(), Arc::clone(&m), never_shutdown_rx()));
      let mut client = client;
      client.write_all(&push_frame(b"q")).await.unwrap();
      read_response(&mut client).await;
      assert_eq!(internal(&m, NackInternalCause::queue_full), 1);

      // Ack timeout: the unit is received and its sender parked.
      let (client, server) = UnixStream::pair().unwrap();
      let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
      std::thread::spawn(move || {
          let rx = queue_rx.get(0);
          let mut parked = Vec::new();
          while let Ok(unit) = rx.recv() {
              parked.push(unit.ack_tx);
          }
      });
      let m = Arc::new(Metrics::new().0);
      let cfg = ConnectionConfig { ack_timeout: Duration::from_millis(100), ..test_cfg() };
      tokio::spawn(handle_connection(server, queue_tx, cfg, Arc::clone(&m), never_shutdown_rx()));
      let mut client = client;
      client.write_all(&push_frame(b"t")).await.unwrap();
      read_response(&mut client).await;
      assert_eq!(internal(&m, NackInternalCause::ack_timeout), 1);
  }

  #[tokio::test]
  async fn a_batch_counts_each_failed_record_by_cause() {
      let m = Arc::new(Metrics::new().0);
      let mut c = spawn_handler_acking_with(test_cfg(), Arc::clone(&m), Some(false)).await;
      let frame = Envelope::new(
          Header::new(MessageType::PushBatch, Durability::Durable, 0),
          weir_core::encode_batch_body(&[b"b1", b"b2", b"b3"]),
      )
      .encode();
      c.write_all(&frame).await.unwrap();
      assert_eq!(read_response(&mut c).await.0, MessageType::AckBatch);
      assert_eq!(internal(&m, NackInternalCause::write_failed), 3);
  }
  ```
- [ ] **Step 2: Run it to verify it fails.** `cargo test -p weir-server --bin weir-server socket::connection::tests -- --test-threads=1`. Expected: the 4 new tests FAIL with counts of 0.
- [ ] **Step 3: Implement.** Add near `admission_refusal`:
  ```rust
  use crate::metrics::{NackInternalCause, NackInternalLabel};

  /// `weir_records_nack_internal_total{cause}` (spec §5.13): which of the
  /// transient causes behind one wire reason (`InternalError`) fired.
  fn count_internal_nack(metrics: &Metrics, cause: NackInternalCause, n: u64) {
      if n > 0 {
          metrics.records_nack_internal.get_or_create(&NackInternalLabel { cause }).inc_by(n);
      }
  }

  impl AdmissionRefusal {
      fn cause(self) -> NackInternalCause {
          match self {
              AdmissionRefusal::OwnershipLost => NackInternalCause::ownership_lost,
              AdmissionRefusal::WabCap => NackInternalCause::wab_cap,
          }
      }
  }
  ```
  Sites:
  - **Both admission arms.** After the `records_nack … .inc();` that follows `if refusal == AdmissionRefusal::WabCap { … }`, add `count_internal_nack(&metrics, refusal.cause(), 1);`. This is one per refused frame, matching the `records_nack` increment there.
  - **`handle_push`, `if push_result.is_err()`.** Add `count_internal_nack(metrics, NackInternalCause::queue_full, 1);`.
  - **`handle_push`, the coordinate `Ok(None) | Err(_)` arm.** Add `count_internal_nack(metrics, NackInternalCause::coordinate, 1);`.
  - **`handle_push`, the `Ok(_) =>` arm.** Split it into two arms with identical bodies, except that each counts its own cause:
    ```rust
    Ok(Ok(_)) => {
        // The flusher resolved the ack false: the write or fsync failed.
        send_nack(stream, WireNack::InternalError, &[], write_timeout).await?;
        metrics.records_nack.get_or_create(&NackLabel { tier: tv, reason: MetricNack::internal_error }).inc();
        count_internal_nack(metrics, NackInternalCause::write_failed, 1);
        Ok(())
    }
    Ok(Err(_)) => {
        // The flusher dropped the sender (a panic).
        send_nack(stream, WireNack::InternalError, &[], write_timeout).await?;
        metrics.records_nack.get_or_create(&NackLabel { tier: tv, reason: MetricNack::internal_error }).inc();
        count_internal_nack(metrics, NackInternalCause::sender_dropped, 1);
        Ok(())
    }
    ```
  - **`handle_push`, `Err(_elapsed)`.** Add `count_internal_nack(metrics, NackInternalCause::ack_timeout, 1);`.
  - **`handle_push_batch`.** Before `receivers.resize_with(n, || None);`, add `let enqueued_ok = receivers.iter().filter(|r| r.is_some()).count() as u64;`. Add `let mut write_failed = 0u64; let mut dropped = 0u64;` next to `let mut acked = 0u64;`, and replace the `Ok(_) => {}` arm of the await loop with:
    ```rust
    Ok(Ok(_)) => write_failed += 1,
    Ok(Err(_)) => dropped += 1,
    ```
    After `if timed_out { metrics.ack_timeout.inc(); }`, add:
    ```rust
    count_internal_nack(metrics, NackInternalCause::queue_full, n as u64 - enqueued_ok);
    count_internal_nack(metrics, NackInternalCause::write_failed, write_failed);
    count_internal_nack(metrics, NackInternalCause::sender_dropped, dropped);
    // Everything enqueued but not resolved before the shared deadline.
    count_internal_nack(metrics, NackInternalCause::ack_timeout, enqueued_ok - acked - write_failed - dropped);
    ```
    (`enqueued_ok >= acked + write_failed + dropped` holds: each counted record had a receiver.)
- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1`. Expected: all pass, the existing nack tests included (the wire bytes are unchanged).
- [ ] **Step 5: Commit.** `git commit -am "feat(socket): count transient nacks by cause"`

---

### Task 1.27: System tests: SIGTERM exits within the bound, and a serving daemon reports ready

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `crates/weir-server/tests/system.rs` (two tests after `server_exits_and_socket_disappears_after_sigterm`, :998)
- Create: `crates/weir-status/tests/fixtures/metrics_4_1.txt` (written by the second test under `WEIR_BLESS=1`)
- Modify: `crates/weir-status/src/exposition.rs` (`captured_expositions_parse` appended to `mod tests`)
- Test: the same files

**Interfaces:**
- Consumes: the testkit (`weir_server!`, `.shutdown_timeout_secs`, `.start`, `.client`, `.child_pid`, `.wait_for_exit`, `.scrape_metrics`), `libc::kill` (libc is a weir-server dependency, and system.rs already calls `libc::` at :1607).
- Produces: spec §16 system test 13, which every later Foundations PR must keep green.

- [ ] **Step 1: Write the tests.**
  ```rust
  /// Spec §16 system test 13, a gate from PR 1 onward: SIGTERM exits within
  /// shutdown_timeout + 5 s with an idle producer connection still open, and
  /// exits 0. It guards the sequencing every later Foundations PR rewrites
  /// (signal task, lame duck, joins, drain stop).
  #[test]
  fn sigterm_exits_within_the_bound_with_an_idle_connection_open() {
      const SHUTDOWN_TIMEOUT_SECS: u64 = 2;
      let bound = Duration::from_secs(SHUTDOWN_TIMEOUT_SECS + 5);
      let mut srv = weir_server!("sigterm_bound").shutdown_timeout_secs(SHUTDOWN_TIMEOUT_SECS).start();
      let mut busy = srv.client();
      busy.push(b"acked-before-sigterm", Durability::Durable).unwrap();
      let mut idle = srv.client();
      idle.health_check().unwrap(); // connected, then left idle across the signal
      let t0 = Instant::now();
      unsafe {
          libc::kill(srv.child_pid() as libc::pid_t, libc::SIGTERM);
      }
      let status = srv
          .wait_for_exit(bound)
          .unwrap_or_else(|| panic!("weir-server still running {bound:?} after SIGTERM"));
      assert!(status.success(), "exit status {status}");
      assert!(t0.elapsed() < bound, "took {:?}", t0.elapsed());
      drop((busy, idle));
  }

  /// The PR 1 status gauges on a real daemon: after the first 5 s poll a
  /// serving daemon reports ready, serving, every shard up, the cap clear,
  /// and the noop sink as an active warning condition.
  #[test]
  fn a_serving_daemon_reports_ready_through_the_new_gauges() {
      let srv = weir_server!("status_gauges").shard_count(2).start();
      let deadline = Instant::now() + Duration::from_secs(10);
      let body = loop {
          let body = srv.scrape_metrics();
          if body.contains("weir_ready 1") {
              break body;
          }
          assert!(Instant::now() < deadline, "weir_ready never reached 1:\n{body}");
          thread::sleep(Duration::from_millis(250));
      };
      for series in [
          "weir_startup_phase{phase=\"serving\"} 1",
          "weir_wab_shard_up{shard=\"0\"} 1",
          "weir_wab_shard_up{shard=\"1\"} 1",
          "weir_wab_cap_rejecting 0",
          "weir_condition_active{code=\"noop_sink\",severity=\"warning\"} 1",
          "weir_condition_active{code=\"starting\",severity=\"info\"} 0",
      ] {
          assert!(body.contains(series), "{series:?} missing:\n{body}");
      }
      assert!(body.contains(&format!("weir_build_info{{version=\"{}\"", env!("CARGO_PKG_VERSION"))), "{body}");
      // Spec §16: weir-status's `evidence_from_metrics` is tested against a
      // captured 4.1 exposition. This is the capture; re-bless on an intended
      // family change and review the diff.
      if std::env::var_os("WEIR_BLESS").is_some() {
          let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
              .join("../weir-status/tests/fixtures/metrics_4_1.txt");
          std::fs::write(&path, &body).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
      }
  }
  ```
  Append to `exposition.rs`'s `mod tests` (both captures are real scrapes: Task 1.9's from v4.0.1, this task's from the PR 1 daemon):
  ```rust
  #[test]
  fn captured_expositions_parse() {
      let old = evidence_from_metrics(include_str!("../tests/fixtures/metrics_4_0_1.txt"), NOW);
      assert_eq!(old.phase, None);
      assert!(old.sink.is_some() && old.drain.is_some() && old.config.is_some());
      assert!(!readiness(&old).ok, "an old daemon's readiness is unknown, not ready");
      assert_eq!(active_conditions_from_metrics(include_str!("../tests/fixtures/metrics_4_0_1.txt")), None);
      let new = evidence_from_metrics(include_str!("../tests/fixtures/metrics_4_1.txt"), NOW);
      assert_eq!(new.phase, Some(Phase::Serving));
      assert!(new.shards.is_some() && new.workers.is_some() && new.wab_cap.is_some()
          && new.backlog.is_some() && new.listeners.is_some());
      assert!(readiness(&new).ok, "{:?}", readiness(&new).failing);
      assert!(active_conditions_from_metrics(include_str!("../tests/fixtures/metrics_4_1.txt")).is_some());
  }
  ```
- [ ] **Step 2: Run them.** `cargo test -p weir-server --test system -- --test-threads=1 sigterm_exits_within serving_daemon_reports`. With Tasks 1.18-1.23 in place both should pass. To see the gate bite, run the second test against Task 1.17's commit (before the gauges exist): it fails with `weir_ready never reached 1`. The first test passes on today's main too. It is a forward guard (spec: "guards the detached-era sequencing"), not a fix, so it has no red phase in this PR.
- [ ] **Step 3: Implement.** No production change. If the first test fails, the cause is in Task 1.23's signal-path edit: `begin_shutdown` must not block, and the oneshots must still fire. Fix it there, not by loosening `bound`.
- [ ] **Step 4: Run to verify it passes.**
  - `WEIR_BLESS=1 cargo test -p weir-server --test system serving_daemon_reports -- --test-threads=1`, then `grep -c '^weir_startup_phase' crates/weir-status/tests/fixtures/metrics_4_1.txt`. Expected: the test passes and the count is `6` (one series per phase). Do not hand-edit the capture.
  - `cargo test -p weir-status captured_expositions_parse`. Expected: 1 passed.
  - `cargo test -p weir-server --test system`, with no `--test-threads` (the way CI runs it). Expected: the whole system suite passes, the two new tests included.
- [ ] **Step 5: Commit.** `git add crates/weir-status/tests/fixtures/metrics_4_1.txt && git commit -am "test(system): SIGTERM exits within the bound; a serving daemon reports ready"`

---

### Task 1.28: Docs, changelog, lock files and the full gate

*PR-level merge gate: PR 1's **Merge gate** (top of the PR 1 section).*

**Files:**
- Modify: `CHANGELOG.md` (`## [Unreleased]`: a `### Added` and a `### Changed` entry for Foundations PR 1; whether these move under their own release heading is decided at release time, see the open questions)
- Modify: `docs/operations/configuration.md`, in the `wab_max_bytes` section: "the cap flag is refreshed every 5 s without traffic, and a restart over the cap rejects from the first push"
- Modify: `docs/architecture.md:56`: the startup line now includes the version and the ConfigView
- Modify: `fuzz/Cargo.lock` (regenerated)
- Test: the merge gate at the top of this section

**Interfaces:**
- Consumes: everything above.
- Produces: no code.

- [ ] **Step 1: Write the docs.**

  `CHANGELOG.md` under `## [Unreleased]`, `### Added`:
  ```markdown
  - **A status model the whole daemon shares (`weir-status`).** A new published crate holds the
    `weir.status/v1` types, the 30-code Conditions catalogue and one pure evaluator. The daemon
    publishes its live state to a wait-free status board, and a 5 s task evaluates it into new
    gauges: `weir_ready`, `weir_live`, `weir_condition_active{code,severity}` (one series per code),
    `weir_startup_phase{phase}`, `weir_wab_shard_up{shard}`, `weir_worker_up{worker}`,
    `weir_wab_undelivered_{segments,records}`, `weir_wab_oldest_undelivered_age_seconds`,
    `weir_build_info` and the `weir_config_*` effective-config gauges, among 34 new families (see
    docs/monitoring.md "Status, lifecycle and conditions"). Missing evidence makes a condition
    *unevaluated*, never healthy.
  - `weir_records_nack_internal_total{cause}` splits the transient `InternalError` nacks into
    `queue_full`, `write_failed`, `sender_dropped`, `ack_timeout`, `wab_cap`, `coordinate` and
    `ownership_lost`. The wire bytes are unchanged.
  ```
  `### Changed`:
  ```markdown
  - **The `wab_max_bytes` cap clears without traffic.** The 5 s size scan now re-evaluates the cap,
    so a daemon that drained below 90% of the cap resumes accepting before the next push arrives.
    A failed scan leaves the flag as it was (fail-closed).
  - **A restart over the cap rejects from the first push.** A synchronous size scan runs after
    recovery, before the socket binds. If that scan fails with a cap set, pushes nack until a
    scan succeeds.
  - `weir_dead_letter_bytes_on_disk` and `weir_quarantine_bytes_on_disk` are also refreshed by the
    5 s task.
  - Logging: the "weir starting" line carries `version`, `wire_version` and the effective config
    (no secrets: sink URLs are reduced to scheme://host:port). Colour is emitted only when stdout
    is a terminal. An invalid `log_level` is logged as a WARN instead of silently becoming `info`.
    Commit-path log lines name their segment. A dead-letter-blocked episode WARNs when it starts.
  - Build: `arc-swap` is always compiled (it was optional behind `tls`).
  ```
- [ ] **Step 2: Refresh the fuzz lock.** `(cd fuzz && cargo metadata --format-version 1 >/dev/null)`, then `git diff --stat fuzz/Cargo.lock`. Expected: `weir-status` and `arc-swap` entries change. The chaos workspace does not depend on weir-server; confirm with `(cd chaos && cargo metadata --format-version 1 >/dev/null) && git diff --exit-code chaos/Cargo.lock`.
- [ ] **Step 3: Run the full merge gate** (the nine items at the top of this section), in this order:
  1. `cargo fmt --check`
  2. the four `cargo clippy` lines
  3. `cargo test -p weir-status`
  4. the bin suites, with and without `--all-features`
  5. the system, docs_drift, demo_drift and tls_listener tests
  6. the DST line
  7. the Windows-library `cargo check`
  8. `act -W .github/workflows/ci.yml -j <job>` for every job, container jobs included

  Expected: all green. Any remaining `dead_code` warning names an item a later PR consumes. Give it the `#[cfg_attr(not(test), allow(dead_code))] // PR n …` marker rather than deleting it.
- [ ] **Step 4: Owner-run load check** (beast): `cargo test -p weir-server --test load --release -- --nocapture` for the Durable herd and the Buffered profile, before and after the branch. Expected: within ±3%.
- [ ] **Step 5: Commit.** `git add CHANGELOG.md docs fuzz/Cargo.lock && git commit -m "docs: status foundations in the changelog, configuration and architecture notes"`
