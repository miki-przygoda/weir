# weir 4.1 "Foundations" design: status snapshot, drain control, bounded lifecycle, health and admin surfaces (SP-D + SP-F + SP-E)

Status: proposed, revision 2 (review applied). Base: main @ 18c931a (weir 4.0.1). It ships alongside the SP-A/SP-B safety-floor spec, which is a separate document. Section 18 names the slots that spec fills.

Revision 2 in brief:
- The board no longer holds a drain-channel `Sender`. Depth is a wait-free `AtomicUsize` that both ends write.
- The flusher hand-off is a loop of short timed sends, so a flusher that was already blocked before SIGTERM still sees shutdown.
- Pause takes effect at commit-batch granularity.
- The admin socket's default path moves off the producer-shared directory and is derived from `wab_dir`.
- Public redaction is enforced by the types.
- HTTP probes get their own permit pool.
- The existing drain tests compile unchanged.
- Chaos becomes a manual pre-release gate, with the harness work it needs now in scope.


> **Owner review status (2026-10-05).** The owner approved this spec in advance ("I approve the spec") and went AFK, granting full autonomy to plan and implement it. Every open owner decision in section 20 was therefore taken **autonomously, using the recommended option**, and is flagged for the owner to confirm or overturn. Nothing here was pushed or released.
>
> | Decision (section 20) | Taken (recommended) |
> |---|---|
> | Where the evaluator, status types and admin protocol types live | A new crate, `weir-status` (weir-ctl must evaluate offline without depending on weir-server) |
> | Admin socket peer policy | Daemon euid or root; documented in threat-model.md as a deliberate divergence from the producer rule |
> | `drain_shutdown_budget_secs` | Starts after the flusher joins; default 10 s |
> | Delivery order once ingest opens before replay finishes | Alternate live and backlog segments |
> | Default liveness checks | None (`[]`); the ingest-dead set recommended in docs; `drain_wedged` only after SP-B has soaked |
> | Exit status of a forced or orphaned-drain exit | 0 |
> | Pause in force at SIGTERM | Honour the pause |
> | Pause granularity | Before the next commit batch (holds the F05 cursor) |
> | `drain.retry_stranded` during a backoff | Cuts it short, rate-limited |
> | `log_format=json` | Deferred to SP-O |
> | `reload` scope | TLS only in this cut |
> | Probes vs Prometheus on one listener | Split connection pools now; separate health listener deferred to SP-O |
> | Condition thresholds | Constants |
> | Ship order | The safety floor ships first as its own release. Version numbers in this document say 4.1; if the safety floor takes 4.1.0, Foundations becomes 4.2.0. Chosen at release time. |
> | TCP listener failed while Unix is up | Not ready |
> | Chaos campaign | Manual pre-release gate (needs beast + root), not a per-PR gate |

---

## 1. Context and problem

weir is a single process. Its lifecycle lives in local variables in `main` and is driven by whichever channel happens to close. Operators run into the following today.

- **Startup cannot be observed, and it waits on sink speed.**
  - Recovery runs inside `wab::spawn` (main.rs:559, wab/mod.rs:262-283).
  - Replay then pushes every unconfirmed sealed segment with **blocking** sends into the bounded(256) drain channel (main.rs:885, wab/mod.rs:459-487).
  - Only after that does the runtime start (main.rs:890), metrics bind (main.rs:898-912), and the producer socket bind inside `socket::run` (main.rs:1215, socket/mod.rs:114-115).
  - So a large backlog behind a slow sink keeps the producer socket and `/metrics` down, and `WeirInstanceDown` fires during recovery.
- **The drain's lifetime is tied to channel disconnect.**
  - `RecvTimeoutError::Disconnected => break 'outer` (drain/mod.rs:693), and the supervisor treats `Ok(())` as shutdown (drain/mod.rs:257-258).
  - With `shard_count=1`, a flusher that dies makes the drain exit as if shutting down. The flusher can die from the panic budget (wab/mod.rs:194-204) or the counter-scan give-up (wab/mod.rs:684-698). The result is a zombie: no ingest, no delivery, still "running".
- **Shutdown is unbounded.**
  - `drop(rt)` (main.rs:1248) kills `/metrics` immediately. Workers, flushers and the drain are then joined with no deadline (main.rs:1252-1258).
  - The drain tries to deliver the whole queue.
  - The only signal handler is a one-shot task (main.rs:1172-1196), so a second SIGTERM is swallowed.
  - The socket file is unlinked only after the connection drain (socket/mod.rs:286), so new connects pile up in the backlog.
  - A flusher already blocked in `drain_tx.send` (wab/mod.rs:978) waits for the drain to take an item, and the drain may be inside a commit lasting up to `commit_timeout` (≥ 60 s, main.rs:594).
- **There is no health or status surface.**
  - The metrics handler ignores the request line and returns metrics for any path (metrics/server.rs:88-125).
  - `deploy/systemd/weir-readiness.sh` gates on sink health and lifetime counters (lines 98-135), so a recovered flusher panic sticks at NOT READY forever.
  - Docker's HEALTHCHECK only opens a TCP connection (Dockerfile:127-128).
- **There is no way to intervene without a restart.** No pause, no "retry stranded now", no structured reload. SIGHUP kills the default non-tls build; SP-A fixes that.
- **Diagnosis means reading raw fields.** Nothing says *why*. A full drain channel makes flushers block at the rotation send (wab/mod.rs:978) before acks resolve (wab/mod.rs:1050). Producers then see ACK_TIMEOUT NACKs for records that are durable, which is routinely misdiagnosed as a storage problem.

## 2. Goals and non-goals

**Goals**

- **G1.** A wait-free status model. A stalled reader can never delay an ack, a flusher or the drain.
- **G2.** A typed Conditions list that answers *why*. A single pure evaluator computes it once, and every consumer shares it: `/status`, `/readyz`, `/livez`, admin `status`, `weir_condition_active`, the readiness script, and offline `weir-ctl status`. Every consumer's ready/not-ready answer equals `/readyz`'s.
- **G3.** An explicit drain lifetime (DrainControl), plus operator verbs: pause, resume, retry-stranded.
- **G4.** Observable, non-blocking startup. Health, metrics and admin are up before recovery, and replay is handed to the drain in memory.
- **G5.** Bounded shutdown with a lame-duck phase. The bound holds whatever the drain is doing. A second signal shortens shutdown but never skips the worker and flusher joins.
- **G6.** Readiness means "can accept ingest". Liveness is opt-in and covers only states a restart fixes.
- **G7.** Everything is additive. The wire protocol, on-disk format, existing metrics and existing `--json` shapes are unchanged.
- **G8.** Disclosure is bounded by the type system. The public document cannot carry free text, and only the daemon's own uid or root can reach the admin surface.

**Non-goals for this cut.** Each has a reserved seam; see §18.

- Re-reading the config file, and the live-reload framework (SP-L).
- Escalation policy (SP-G).
- Strand cool-down and breaker (SP-H).
- Sink hot-swap (SP-M).
- Socket activation and sd_notify (SP-N).
- k8s manifests (SP-O).
- The TLS cert-expiry gauge (SP-L).
- `log_format=json` (owner decision; recommended to defer to SP-O).
- State-changing HTTP.
- New wire MessageTypes.
- Closing connections pinned to an offline shard (SP-G).

## 3. Owner decisions (binding)

1. **Order.** Foundations is SP-D + SP-F + SP-E. The safety floor ships alongside as a separate small spec: the SP-A lock, an always-installed SIGHUP handler, refusing to steal a live socket, and the SP-B drain resend-loop fixes.
2. **Control channel.**
   - (a) Read-only HTTP routes `/livez`, `/readyz`, `/startupz` and `/status` (redacted JSON) on the existing metrics listener. Paths match exactly; every other path still returns metrics.
   - (b) A separate admin Unix socket: mode 0600, peer uid checked, JSON-lines.
   - No new producer-socket MessageTypes and no state-changing HTTP.
3. **Admin verbs.**
   - `status`: full view, same model as `/status`.
   - `reload`: structured result; reloads TLS certs only; SIGHUP goes through the same path.
   - `drain.pause` / `drain.resume`: stop and restart delivery at a segment boundary while ingest keeps buffering. In-memory only.
   - `drain.retry_stranded`.
4. **Readiness = ingest-capable.** Ready means all of: recovery done, producer socket bound, not shutting down, not WAB-cap-rejecting, and at least one shard up. New connections are routed only to live shards. Sink and drain health never gate readiness. `/livez` is opt-in and fails only on restart-fixable states.

*Note on owner decision 3.* Pause still means "no new segment starts while paused". Revision 2 also lets a pause stop *between commit batches inside* a segment, holding the F05 cursor. This keeps the verb's zero-duplicate guarantee and makes it effective in seconds rather than one whole 256 MiB segment (§5.4). If the owner reads "segment boundary" literally, the batch-level stop is one flag to remove.

## 4. Architecture overview

```
                     signals (TERM/INT/HUP)
                             |
                      +--------------+  watch<ShutdownLevel> (wait_for, not changed)
                      |  Lifecycle   |---------------------------------+
                      | (signal task)|                                 v
                      +--------------+                     +-----------------------+
                       |  |   |   \ Reloader (SIGHUP|admin) | socket::run / tcp::run|
             begin_    |  |   |    \                       | (lame duck, live-shard|
             shutdown  |  |   |     +---------+            |  routing)             |
                       v  |   |                            +-----------+-----------+
   +-------------------+  |   |   loads only (atomics / ArcSwap)       | push
   |  DrainControl     |  |   |  +---------------------------+         v
   |  (intent, atomics)|  |   +->|        StatusBoard        |   QueueSender (64k)
   +---------+---------+  |      | lifecycle | listeners     |         |
             |            |      | shards (+handoff_blocked) |<- WorkerCell drop guards
             v            |      | drain cell | sink cell    |         v
   +-------------------+  |      | wab cell | reload | admin |    workers (N)
   | drain thread       |-+----->| latches | recent         |         |
   | (current-thread)   | publish| drain_depth: AtomicUsize  |<-- flushers store tx.len()
   | pending | backlog  |-------->|   (drain stores rx.len()) |    after each hand-off
   | held(paused)      |         +-----+---------------+-----+    flushers (per shard):
   +---------+---------+               |               |          seal+fsync -> hand_off
             ^                         |               |          (send_timeout 100 ms loop;
             |   bounded(256) drain channel  <-------------------- 1 s budget once shutting down)
             |   (ONE Receiver: the drain. NO Sender or Receiver clone outside the pipeline)
           sink                conditions::evaluate        poll tasks: 5 s (scan, cap,
                               (weir-status, pure)         lag, latches) / 500 ms (queue depth)
                                /        |        \
       HTTP router (metrics listener)   admin socket (0600, uid,     weir-ctl status (offline)
       head pool -> route pools          identity in every reply)    metrics + disk -> same evaluator
       /livez /readyz /startupz /status  status reload drain.*
       any other path -> metrics (byte-identical body)
```

**Data-flow rules**

- **Single writer per cell.**
  - Atomic stores happen on the owning thread.
  - ArcSwap stores happen only on the drain thread, the poll task, the Reloader and the admin server (for its own cell).
  - The one shared cell is `drain_depth`. Its stores are last-writer-wins snapshots of `len()`, never inc/dec.
- **Readers only load.** They then call the pure `weir_status::evaluate`.
- **Control is intent; the board is observation.** Every command returns a generation or ticket, and the drain publishes the one it has observed.
- **The drain owns the drain channel's only Receiver.** The flushers own its only Senders. Nothing else holds a handle to it: no board probe, no poll task, no admin. That keeps today's disconnect semantics honest in every PR of the sequence, including while the drain is still wired Detached. It also keeps the fail-fast `SendError` path when the drain dies (wab/mod.rs:978-980).

## 5. Components

### 5.1 `weir-status` (shared interpretation; placement is an owner decision, §20)

**Responsibility.** It holds:
- the `weir.status/v1` document types, in two renderings: `AdminStatusDoc` and `PublicStatusDoc`;
- `ConditionCode` and the condition catalogue;
- the pure evaluator and the probe predicates;
- the Prometheus-exposition → Evidence parser;
- the admin protocol envelope types.

It does no I/O and reads no clock (`now` is passed in).

**Placement constraint (verified).** weir-ctl is a published crate. It depends on core, client, wab and attest, and it cannot reach weir-server internals: weir-server's `lib.rs` re-exports only `weir_wab::format`. weir-core deliberately excludes serde (weir-core/Cargo.toml:19). So the shared evaluator lives either in a new published crate or behind a feature in a crate weir-ctl already uses (weir-wab). See §20.

**Interface.**
```rust
pub struct Evidence { now_unix_ms: u64, source: Source,
    phase: Option<Phase>, phase_since_unix_ms: Option<u64>, shutting_down: Option<bool>,
    listeners: Option<Vec<ListenerEvidence>>, shards: Option<Vec<ShardEvidence>>,
    wab_cap: Option<CapEvidence>, drain: Option<DrainEvidence>, sink: Option<SinkEvidence>,
    backlog: Option<BacklogEvidence>, recent: Option<RecentCounters>,
    reload: Option<ReloadEvidence>, admin: Option<AdminEvidence>, config: Option<ConfigEvidence> }
pub fn evaluate(e: &Evidence, t: &Thresholds, latches: &dyn SinceLookup) -> Evaluation
pub struct Evaluation { conditions: Vec<Condition>, unevaluated: Vec<ConditionCode> }
pub fn readiness(e: &Evidence) -> ProbeVerdict   // ok + failing codes + per-check list
pub fn liveness(e: &Evidence, t: &Thresholds, checks: &[LivezCheck]) -> ProbeVerdict
pub fn startup(e: &Evidence) -> ProbeVerdict

#[derive(Serialize, Deserialize)]
pub struct Condition {
    code: ConditionCode, severity: Severity, affects: Affects /*ingest|delivery|durability*/,
    since_unix_ms: Option<u64>,
    summary: String,                       // PUBLIC: rendered from a template whose parameters
                                           // are numbers, enums, codes, shard ids, segment ids only
    detail: Option<String>,                // ADMIN ONLY: may interpolate free text (panic msg, uid…)
    evidence: BTreeMap<String, EvidenceValue>,
    runbook: Cow<'static, str>,            // owned on deserialize (weir-ctl parses Strings)
    gates: Vec<Gate> }
pub enum EvidenceValue { U64(u64), F64(f64), Bool(bool), Code(String), Segment(SegmentId),
                         Shards(Vec<u16>), Text(String) /* admin only; stripped from public */ }

#[serde(rename_all = "snake_case")]
pub enum ConditionCode { …, #[serde(other)] Unknown }   // an older weir-ctl tolerates newer codes
pub const ALL: &[ConditionCode];
pub mod exposition { pub fn evidence_from_metrics(body: &str, now_unix_ms: u64) -> Evidence }
pub mod admin { Request, Response, Envelope, InstanceId, AdminError, result structs }
```

**Rules**
- Missing evidence makes a condition *unevaluated*, never "false".
- An `Unknown` code read from a newer daemon is shown verbatim (the raw string is kept in `evidence["code_raw"]`), with severity from the wire. It is never dropped.
- **Public rendering is structural.** `PublicStatusDoc` is a separate type, built by `fn public(&AdminStatusDoc) -> PublicStatusDoc`. Its only `String`-typed fields are closed-enum renderings, codes, segment identities and the template-rendered `summary`. `detail`, `EvidenceValue::Text` and every free-text field do not exist on the public type, so they cannot be serialized into it.
- **Dependencies:** serde and serde_json only, both already in the lock tree.

### 5.2 `status::StatusBoard` (wait-free live state)

**Placement:** NEW `crates/weir-server/src/status/{mod.rs,board.rs,shards.rs,poll.rs,render.rs,config_view.rs}`.

**Interface.**
```rust
pub(crate) struct StatusBoard {
  statik: StaticInfo,             // version, wire_version, pid, started_at, sink_type, instance id,
                                  // Arc<ConfigView> (§5.14)
  life: LifecycleCell,            // phase AtomicU8 + since, shutting_down + since, forced, ever_served
  listeners: ListenerCells,       // unix/tcp: NotConfigured|Pending|Bound|Closed|Failed
  shards: ShardRegistry,          // §5.3 (incl. per-shard handoff_blocked_since)
  workers: Box<[WorkerCell]>,     // running AtomicBool + exited_at (drop guard in the worker thread)
  drain: DrainCell,               // written only by the drain thread + its supervisor
  sink: SinkCell,                 // written only by the drain thread
  wab: WabCell,                   // written only by the poll task (ArcSwap<WabScan>) + first_scan_ok_ms
  admin: AdminCell,               // Disabled | Bound{path} | Unavailable{reason_code}
  reload: ArcSwapOption<ReloadReport>,
  latches: ConditionLatches,      // [AtomicU64; N_CODES] first-seen ms, poll task only
  recent: ArcSwap<RecentCounters>,// counter-delta windows, poll task only
  drain_depth: Arc<AtomicUsize>,  // len() snapshots stored by flushers (after each hand-off) and the
                                  // drain (each wait slice). NO channel handle of any kind.
  wab_cap_rejecting: Arc<AtomicBool>, wab_bytes_now: Arc<AtomicU64>, // existing atomics (main.rs:946/952), created earlier
}
fn snapshot(&self, now: SystemTime) -> StatusSnapshot          // loads only
impl StatusSnapshot { fn evidence(&self) -> Evidence; fn admin_doc(&self, ev: &Evaluation) -> AdminStatusDoc }
```

**DrainCell fields**
- `state` (mirrors `DrainStateValue`) and `state_since`.
- `paused_since` (0 = not paused), `held` (bool: paused mid-segment), `observed_requeue`.
- `generation`: one bump per `drain_thread` start.
- `in_flight: ArcSwapOption<InFlight{segment_identity, attempt, retries_left, processed, next_retry_at}>`.
- `commit_in_flight_since` (0 = none) and `last_progress_ms`. Both are maintained by an RAII `CommitGuard` (§5.4), so every early `return` from `commit_batch` clears them.
- `pending_len`, `backlog_len`, `replay_total`, `replay_remaining`.
- `blocked_since`, `stopping`, `deadline_ms`.
- `last_requeue: ArcSwapOption<RequeueResult>`, `exit: ArcSwapOption<DrainExitInfo>`.

**SinkCell fields**
- `health` and `health_since`.
- `last_success_ms`, `last_failure_ms`, `last_failure_kind`, `consecutive_failures`.
- `last_error: ArcSwapOption<String>`, admin-only. Stored after:
  - `sink::redact_url_password` (sink/mod.rs:50);
  - `redact_query_credentials` (sink/mod.rs:86);
  - stripping control characters;
  - capping at 2048 bytes on a char boundary.
- `probe_reason`: admin-only, stored the same way.

### 5.3 `status::ShardRegistry`

**States.** `Starting | Up | Respawning | Offline(RespawnExhausted|CounterScanFailed|FlusherExited) | Stopped`, plus `since_ms`, `respawns` and `handoff_blocked_since_ms` (0 = not blocked).

**Flusher exit reason (NEW, fixes a lost reason).** Today the counter-scan give-up is a plain `return` (wab/mod.rs:697) that the supervisor cannot tell apart from a clean exit (`Ok(()) => return`, wab/mod.rs:186). The change:
- `flusher_thread` returns `enum FlusherExit { InputClosed, CounterScanFailed }`.
- `run_with_panic_supervision`'s factory bound changes from `B: FnOnce()` (wab/mod.rs:177) to `B: FnOnce() -> FlusherExit`.
- The supervisor is the single writer of terminal states:
  - `InputClosed`: `Stopped` if shutting down, else `Offline(FlusherExited)`;
  - `CounterScanFailed`: `Offline(CounterScanFailed)`;
  - panic budget exhausted: `Offline(RespawnExhausted)`;
  - each caught panic: `Respawning`.
- `flusher_thread` itself writes only `Up`, right after `scan_and_advance_counter` succeeds (wab/mod.rs:680-685).
- This is also the `SupervisorExit` seam SP-G needs.

**Hand-off blocked marker.** `hand_off` (§5.5) sets `handoff_blocked_since_ms` on its first 100 ms timeout and clears it on success. This is the truthful "delivery is stalling ingest" signal. It costs nothing on the non-blocking path.

**Ingest-capable** means the shard state is `Up|Respawning` AND the worker owning partition `shard_id % worker_count` is running. `Respawning` counts because the bounded batch queue survives a respawn (wab/mod.rs:160-165).

**Interface.**
- `pick_live(n: u64) -> Option<u32>`: the (n mod k)-th ingest-capable shard. O(shard_count ≤ 256) loads per accept.
- `ingest_capable_count()`, `all_settled()` (no shard is `Starting`), `view()`.
- DST (`wab/dst.rs`) passes a detached registry.

### 5.4 `drain::DrainControl` (intent) and the drain state-machine edits

**Placement.** NEW `drain/control.rs`. DrainControl is **not** a field of `DrainConfig` (drain/mod.rs:107). It travels in a new `DrainRuntime` bundle, so the 10 `DrainConfig { .. }` test literals stay as they are.

```rust
#[derive(Clone)] pub(crate) struct DrainControl(Arc<Inner>);
Inner { mode: Lifetime /*Attached|Detached*/, epoch: Instant,
        absorb: AtomicBool, stop: AtomicBool, input_closed: AtomicBool,
        deadline_ms: AtomicU64 /*u64::MAX = none; ms since epoch; only ever lowered via fetch_min*/,
        pause: AtomicBool, requeue_req: AtomicU64, generation: AtomicU64 }
pub(crate) struct DrainRuntime { control: DrainControl, board: Option<Arc<StatusBoard>>,
                                 initial_backlog: Vec<PathBuf> }

// control side
fn attached() -> Self; fn detached() -> Self;
fn begin_shutdown(&self)                     // T0: absorb := true
fn request_stop(&self, budget: Duration)     // after flusher joins: input_closed, stop, deadline.fetch_min(now+budget)
fn force_stop(&self)                         // absorb, stop, deadline.fetch_min(now)
fn request_pause(&self) -> u64; fn request_resume(&self) -> u64; fn request_requeue(&self) -> u64
// drain side
fn deadline_passed(&self) -> bool; fn time_left(&self) -> Option<Duration>
async fn deadline_reached(&self)             // polls every 50 ms on the current-thread rt
fn pause_requested(&self) -> bool; fn requeue_requested(&self) -> u64
```

Because both `request_stop` and `force_stop` use `fetch_min`, the deadline only ever moves earlier. A second SIGTERM during T1 followed by main's T2 `request_stop(10 s)` therefore keeps the forced (earlier) deadline. `force_stop` sets `stop`, so the exit predicate and the commit `select!` agree.

**Entry points: the existing tests compile unchanged (verified).** Tests reach the drain through:
- `spawn` (13 call sites, e.g. drain/mod.rs:2516, 3505, 4098);
- `drain_thread` (6 sites, e.g. 1633, 3052);
- `process_segment` (8 sites, e.g. 2225, 2938);
- `commit_batch` (4 sites, e.g. 2785, 4144).

So each keeps its current signature as a thin wrapper that supplies `DrainRuntime::detached()` (no board, `Detached` control, empty backlog):
- `pub fn spawn(rx, sink, cfg, metrics, hb)` → `spawn_with(rx, sink, cfg, metrics, hb, DrainRuntime)`;
- `drain_thread(..)` → `drain_thread_with(.., &DrainRuntime)`;
- `process_segment(seg, sink, cfg, metrics, dl, skip)` → `process_segment_ctl(.., &DrainCtx)`;
- `commit_batch(..)` → `commit_batch_ctl(.., &DrainCtx)`.

PR 2 gate: `git diff` of the `mod tests` block of drain/mod.rs shows **additions only**.

**Lifetime**
- **Attached** (production, from PR 4):
  - `Disconnected` is a non-event. The drain logs one WARN ("all flushers gone; continuing to deliver the on-disk backlog"), sets a local `channel_closed`, and replaces channel waits with `thread::sleep(slice)`.
  - It exits only when `stop && (deadline_passed || (input_closed && pending.is_empty() && backlog.is_empty() && held.is_none() && rx.is_empty()))`, or when the panic budget is exhausted.
  - With `shard_count=1` and a dead flusher, `Disconnected` really does happen now, because nothing else holds a Sender. That is exactly the case Attached mode exists for.
- **Detached** keeps today's semantics exactly: Disconnected means deliver everything, then exit.
- NEW test: an attached drain with every flusher Sender dropped stays alive, keeps delivering rescanned segments, and exits only on `request_stop`. This is the zombie regression test.

**Wait helper.** `fn wait_slice(rx, ctx, dur, pending) -> Wait{Segment, TimedOut, Stop, Disconnected}` replaces the three `recv_timeout` sites (drain/mod.rs:690, 782, 895). It:
- uses a fixed slice of `min(dur, 50 ms)`, with a **50 ms floor while `pause_requested()`**, so a paused hold can never spin on `recv_timeout(0)`;
- in absorb mode, first runs `while let Ok(p) = rx.try_recv() { pending.push_back(p) }`;
- bumps the heartbeat;
- stores `rx.len()` into `board.drain_depth`.

**Two queues**
- `pending` is today's queue: channel items, FIFO.
- `backlog` (NEW) holds the startup replay list and every disk-scan requeue.
- When both have work, the Draining arm alternates one `pending`/channel segment with one `backlog` segment. This keeps the channel served at ≥ 50% of the drain rate while replay runs, so flushers do not block at wab/mod.rs:978.
- The bounded(256) channel remains the implicit disk bound. Outside shutdown, nothing sends without blocking (§5.5).

**Rescans (automatic and verb-driven) go through one function.** `requeue_unconfirmed(cfg, metrics, rx, &mut pending, &mut backlog, in_flight) -> RequeueResult`:
1. try_recv-drains the channel into `pending` first, so the ≤ 256 sealed paths still in flight in the channel are visible to dedup;
2. calls `scan_unconfirmed_sealed`;
3. deduplicates against `pending ∪ backlog ∪ held ∪ in_flight`;
4. pushes into `backlog`.

The auto-resume in `probe_and_resume_stranded` (drain/mod.rs:409-421) calls it. Its idle gate changes from `pending.is_empty()` to `pending.is_empty() && backlog.is_empty()`, so during replay it no longer rescans every `health_poll_interval` and double-queues channel items.

**Pause**
- **Draining arm head.** Pause applies after the health-poll block (drain/mod.rs:650-677), so probes and gauges keep running, and before taking the next segment. While paused, the arm loops on `wait_slice(50 ms)`, moving channel arrivals into `pending` so flushers never block. It publishes `paused_since`.
- **Inside a segment, at commit-batch granularity (NEW).**
  - `process_segment_ctl` checks `pause_requested()` before each `commit_batch`. When set, it returns `ProcessResult::Paused{processed}`.
  - The drain enters the internal `DrainState::Held{segment, processed, resume: Fresh | Retry{retries_left, next_delay}}`.
  - No commit is in flight at that check, and the F05 cursor equals the committed prefix, so resuming adds **zero** duplicates.
  - Pause latency is therefore at most one in-flight commit (≤ `commit_timeout`), not one 256 MiB segment.
- **RetryingTransient backoff.** The wait condition becomes `remaining.is_zero() && !pause_requested()` (drain/mod.rs:782). The slice floor above prevents the busy-spin that the existing `remaining.min(50ms)` (drain/mod.rs:786) would otherwise cause. `retries_left` and the F05 `processed` cursor are kept.
- **Health while held.** `Held` and the paused RetryingTransient hold both run the wall-clock health poll (probe, gauge refresh, `requeue_unconfirmed`) on the `health_poll_interval` cadence, with `in_flight = Some(segment)`. Today that poll is reached only after the wait (drain/mod.rs:803-811), so sink health would otherwise freeze for the whole pause.
- **BlockedDeadLetterFull** is unchanged. It never commits while blocked, and it unblocks into RetryingTransient, which holds.
- **Gauges.** `weir_drain_state` keeps its one-hot values. `Held` publishes the value of the state it came from. `weir_drain_paused` is the separate gauge.
- **Persistence.** The pause flag lives in DrainControl, so it survives a drain respawn and can be set before the drain spawns. A respawn while `Held` loses the cursor and re-delivers that segment from record 0 (the existing H2 window).

**Retry stranded**
- **Coalescing.** A request that arrives while a previous ticket is still unobserved folds into it, and its ticket resolves together with that one. The drain runs at most one `requeue_unconfirmed` per **5 s** (`REQUEUE_MIN_INTERVAL`). A request inside the window returns the last `RequeueResult` with `coalesced: true`.
- **When the drain observes a new ticket** (at the Draining head, in any wait slice, or in Held):
  1. run `requeue_unconfirmed` (bypassing the health-edge gate);
  2. if in RetryingTransient, cut the remaining backoff — **at most once per retry episode**, and never below `base_retry_delay`. W2 (drain/mod.rs:739-761) is preserved: the backoff is never compressed into an instant re-fire, and a looping caller cannot hammer a failing sink. `retries_left` and the cursor are kept;
  3. publish `RequeueResult{ticket, requeued, already_queued, scan_error, backoff_cut, coalesced, at}` and bump `weir_drain_segments_resumed_total`.

**Stop and deadline (attached mode)**
- **Absorb mode (from T0)** also skips health probes, `requeue_unconfirmed` and the Draining-arm dead-letter and quarantine gauge refresh. None of them is useful during shutdown, and `probe_health` can block for up to `health_probe_timeout` (drain/mod.rs:366-373), which would let it overrun the exit grace. The Blocked arm keeps its local `dead_letter.rescan()`, which is bounded local I/O, so an operator freeing headroom still unblocks it.
- **Draining** checks `deadline_passed()` before taking a segment.
- **`process_segment_ctl`** checks the deadline before each `commit_batch` and returns `ProcessResult::Abandoned{processed}`.
- **`commit_batch_ctl`** does `select!{ r = timeout(commit_timeout, sink.commit(batch)) => r, _ = ctx.deadline_reached() => return BatchResult::Abandoned }`. These are the same drop semantics as today's `commit_timeout` (drain/mod.rs:1392), with no retry bump and no backoff.
- **RetryingTransient:** if `wait_end > deadline`, it exits at once and leaves the segment unconfirmed.
- **Blocked:** on `stop`, it exits as today's `channel_closed` path does (drain/mod.rs:977-984).
- **Held or paused at stop time:** it exits immediately without resuming (owner decision, §20).
- `Abandoned` maps to drain exit. It never reaches `confirm_and_delete` (drain/mod.rs:1015-1016).
- **On exit**, `drain_thread_with` calls `rt.shutdown_timeout(100 ms)` instead of dropping its runtime. A sink's leftover `spawn_blocking` work (a DNS resolver thread, for example) can then never make `Runtime::drop` wait without limit and push a clean exit onto the orphan path (§7).

**Publication**
- `set_state` is called wherever `set_drain_state` is today.
- `in_flight` is set when a segment starts.
- **CommitGuard:** `let _g = ctx.commit_guard();` is taken immediately before the `select!` in `commit_batch_ctl`. Construction stores `commit_in_flight_since = now`, stamps `last_progress_ms` and bumps the heartbeat. `Drop` clears the marker and does the same stamp and bump. Every path — success, the `commit_timeout` early return (drain/mod.rs:1407), the contract-violation return (drain/mod.rs:1431), Abandoned, and unwinding — therefore clears it.
- `record_success` and `record_failure(kind, msg)` run in `commit_batch_ctl`.
- `set_health` runs where `set_sink_health` is called today.
- pending and backlog lengths are published once per loop pass and per wait slice.

**Supervisor.** `run_drain_supervised` (drain/mod.rs:235-300):
- takes `DrainRuntime`, and uses `initial_backlog` on the first start only;
- returns `DrainExit{reason: Stopped|DeadlineAbandoned|GaveUp, abandoned: Option<(SegmentId, u64)>, pending_left, backlog_left}`;
- never respawns after `stop`;
- logs the panic payload via `wab::panic_message_str`;
- publishes `exit`.

### 5.5 WAB startup split, timed hand-off, and the shared cap evaluator

**Split `wab::spawn`** (wab/mod.rs:262-283):
- `wab::recover(dir, shard_count, &metrics) -> io::Result<RecoverySummary>`: `create_dir_private` per shard, then `recover_open_segments`.
- `wab::scan_replay(dir, shard_count, &metrics) -> io::Result<Vec<PathBuf>>`: the same `scan_unconfirmed_sealed` and the same footer accounting as `replay_unconfirmed` (wab/mod.rs:459-487), with **no sends**. `replay_unconfirmed` and `replay_tx` (main.rs:519-525, 877-886) are deleted.
- `wab::spawn_flushers(dir, cfg, drain_tx, metrics, coalesce_hint, registry, drain_depth, shutting_down: Arc<AtomicBool>) -> io::Result<WabHandle>`.

**`hand_off`.** All three flusher send sites go through one helper:
- the timed seal (wab/mod.rs:595);
- the final seal (wab/mod.rs:843);
- the rotation send (wab/mod.rs:978).

```rust
fn hand_off<C: BlockingClock>(clock: &C, tx: &Sender<PathBuf>, sealed: PathBuf, sh: &HandoffState) -> HandOff
// loop {
//   match clock.send_timeout(tx, p, 100ms) {
//     Ok        => { clear handoff_blocked_since; drain_depth.store(tx.len()); return Delivered }
//     Timeout(p2) => { p = p2; set handoff_blocked_since if unset;
//                      if sh.shutting_down.load(Relaxed) {
//                        let t0 = *sh.shutdown_blocked_at.get_or_insert(now);   // per-flusher budget
//                        if now - t0 >= FINAL_HANDOFF_TIMEOUT (1 s) { log "durable, delivered on restart";
//                          count weir_wab_segments_not_handed_off_total; return NotHandedOff } } }
//     Disconnected(p2) => { log existing "durable … delivered on restart" text; return NotHandedOff }
//   } }
```

**Why a loop of short sends.** A flusher that entered a send *before* SIGTERM re-reads the flag at most 100 ms later. A plain `send` would wait until the drain took an item, and the drain may be inside a commit that runs to `commit_timeout` (≥ 60 s, main.rs:594), or inside a sink call that never yields.
- **Outside shutdown** the loop behaves exactly like a blocking send, so backpressure is unchanged. The cost is one Relaxed load per 100 ms *of blocking*; nothing is added per record or per non-blocking rotation.
- **Budget scope.** `FINAL_HANDOFF_TIMEOUT` is a budget per flusher thread, not per call. After it is spent, each later hand-off makes one 100 ms attempt and gives up, so a flusher's total shutdown hand-off time is ≤ 1 s plus 100 ms per remaining rotation.
- **Why it is safe.** Every call site sends only a path returned by a successful `seal_current`, and the seal includes the fsync. Acks are appended after the send (wab/mod.rs:981) and resolved after it (wab/mod.rs:1050). A segment that is not handed off is sealed, unconfirmed and replayed on the next start, which is today's `SendError` semantics.

**DST seam.** `BlockingClock` (wab/clock.rs:19-25) gains `send_timeout<T>(&self, tx, v, dur) -> Result<(), SendTimeoutError<T>>`. `RealClock` delegates to crossbeam. `SimClock` does `try_send` and advances virtual time. The `shutting_down` flag is plumbed through `flusher_thread`'s signature, which also updates the DST call sites (dst.rs:924, 1160, 1474, 1637). With that in place, a bounded-channel DST seed family is deterministic.

**`wab::cap::evaluate(cap, bytes, &flag) -> bool`** is NEW in `wab/cap.rs`. It is the exact hysteresis from `over_wab_cap` (connection.rs:834-855), which now delegates to it.
- The 5 s poll task calls it after every **successful** scan, so the flag clears without traffic.
- On a scan failure the value is left unchanged (fail-closed).
- At startup, the first evaluation runs synchronously in the handoff phase (§6 step 9).
- The existing hysteresis tests move with it.

### 5.6 `lifecycle.rs` (signals plus shutdown level; not an actor)

```rust
pub(crate) enum ShutdownLevel { Running, Graceful, Forced }
pub(crate) struct Lifecycle { level: watch::Sender<ShutdownLevel>, board: Arc<StatusBoard>,
                              drain: DrainControl, shutting_down: Arc<AtomicBool> /*flushers*/ }
fn subscribe(&self) -> watch::Receiver<ShutdownLevel>
fn request_shutdown(&self)  // 1st: Graceful + board.begin_shutdown + drain.begin_shutdown + flag;
                            // 2nd: Forced + drain.force_stop; 3rd+: log only
fn spawn_signal_task(self: Arc<Self>, reloader: Arc<Reloader>, rt: &Handle)
     // loops on TERM/INT/HUP; replaces main.rs:1172-1196 and spawn_tls_reload_task (main.rs:356-394)
```

- `socket::run`, `tcp::run` and `admin::run` take a `watch::Receiver<ShutdownLevel>` in place of `oneshot::Receiver<()>`.
- **They wait with `rx.wait_for(|l| *l != Running)`, never with `changed()`.** `wait_for` checks the current value first, so a SIGTERM that lands between main's "skip binding?" check and the accept loop's first poll is never missed.

### 5.7 Socket bind/run split, lame duck, live-shard routing

**Bind and run**
- `socket::bind(&SocketConfig) -> io::Result<BoundUnix{listener, path, dev, ino, parent_dev_ino}>`. This is `validate_socket_path` plus `bind_hardened`, moved out of `run` (socket/mod.rs:114-115).
- `run(cfg, bound, queue_tx, level_rx, metrics, sem, registry, board)`. tcp.rs gets the same shape.
- **Live-socket refusal for the admin socket** is part of Foundations. Before `cleanup_existing_socket` (socket/mod.rs:555-600), `socket::refuse_if_live(path)` attempts a non-blocking connect, and a successful connect fails the bind with "another daemon is serving `<path>`". SP-A applies the same probe to the producer socket. Whichever of SP-A and Foundations lands first owns the helper.

**Bind everything, then serve.** In §6 step 11 every producer listener is bound before **any** accept loop is spawned. Today `tcp::run` is spawned before the Unix bind (main.rs:1145-1159 vs 1215).

Why: a bind failure then fails startup before any producer is acked, and no producer traffic overlaps the `umask(0o177)` window in `bind_hardened` (socket/mod.rs:425-445). (Correction to that comment: `0o600 & !0o177 = 0o600`, so segment and `.confirmed` files are unaffected. Directories are made via `create_dir_private`, which chmods after `mkdir` (wab/mod.rs:243-254). The window is benign for today's call sites. The arithmetic in the comment at socket/mod.rs:431 is fixed in PR 3.)

**Shard assignment.** `cfg.shard_id = registry.pick_live(n)` replaces `n % shard_count` (socket/mod.rs:216, tcp.rs:192). On `None`:
- drop the stream;
- bump `weir_connections_refused_no_live_shard_total`;
- log a rate-limited WARN.

**Lame duck.** When the accept loop breaks (socket/mod.rs:159-161, tcp.rs:153):
- immediately `drop(listener)` and mark it `Closed`;
- on Unix, unlink the path with an inode check against the recorded (dev, ino). `remove_socket_hardened` moves up from socket/mod.rs:286, and SP-A's version replaces it when it lands.

**Connection drain.** The wait at socket/mod.rs:261 (and tcp.rs) becomes `select!{drain, sleep(shutdown_timeout), wait_for(Forced)}`. On Forced it calls `abort_all` and counts `connections_aborted_at_shutdown`.

**`queue_tx` ownership.** Main holds `queue_tx` in an `Option`.
- It is `take()`n into `socket::run`, and cloned for `tcp::run` and the 500 ms task.
- On the pre-serving shutdown path it is explicitly dropped.
- Before the worker join, a debug assertion checks that the queue's sender count is 0 (main.rs:1238-1247 documents the deadlock this prevents).
- A listener that was bound but never served is unlinked (inode-checked) on the skip path.

### 5.8 HTTP router (metrics listener)

**Spawn.** `metrics::server::spawn(listener, HttpState{registry, board, thresholds, livez_checks, pools}, max_connections)`. The test-only `HttpState::metrics_only(registry)` keeps the existing server tests on the any-path contract. `fn route(head: &[u8]) -> Route` is pure and table-tested.

**Connection handling** (replaces the single permit taken at accept, metrics/server.rs:66-75):
1. **Head phase.** Take a permit from a head pool of `HEAD_READ_SLOTS = 64`. Read until `\r\n\r\n`, bounded by 4096 bytes and a **1 s** head deadline (`HEAD_READ_TIMEOUT`).
   - A peer that sends nothing holds a cheap slot for at most 1 s. It can no longer hold scrape or probe permits for the 5 s `SCRAPE_IO_TIMEOUT`.
   - Reading the whole head also drains the request headers, so closing after `Connection: close` sends FIN, not RST. If the 4096-byte bound is hit, respond, then `shutdown(Write)` and drain for up to 100 ms before closing.
2. **Route phase.** Route on the request line.
   - The metrics path takes a permit from the existing `metrics_max_connections` semaphore with `try_acquire`. On failure it closes, as today.
   - The four probe and status routes take from a dedicated `PROBE_SLOTS = 4` pool. A scrape storm therefore cannot starve kubelet, and a probe flood cannot starve Prometheus.
3. **Respond.** Writes are bounded by `SCRAPE_IO_TIMEOUT` (5 s), as today.

The contract is in §10.

### 5.9 Admin server

**Placement:** NEW `admin/{mod.rs,verbs.rs,identity.rs}`, `#[cfg(unix)]`.

**Interface:** `admin::spawn(BoundUnix, AdminState{board, drain_ctl, reloader, lifecycle, thresholds, instance}, level_rx) -> JoinHandle<()>`.

**Reuse:** `socket::bind_hardened` (socket/mod.rs:400), `socket::peer::peer_uid_of` (peer.rs:53), `refuse_if_live`, and the inode-checked unlink.

**Default path (changed in revision 2).** The default is `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`, e.g. `/var/lib/weir/wab.admin.sock`. It is not in the producer socket's directory. Reasons:
- **The producer directory is shared by design.** The image declares `/run/weir` a VOLUME (Dockerfile:111), compose shares it with producer containers (docker-compose.yml:12-13), and the k8s exploration shares it as an emptyDir (phase4-k8s-deployment.md:28,81). container.md:72-86 tells producers to run as uid 10001, so the uid check alone would admit every producer. Producer authority is exactly "same uid" (socket/mod.rs:149-171).
- **The `wab_dir` parent is private.** It is never a producer mount: systemd `StateDirectory=weir` is 0700 (weir.service:40-41). In the image, `/var/lib/weir` exists, is chowned to weir and is not a VOLUME (Dockerfile:98-100,111), so compose needs no override and no new directory.
- **One daemon per name.** The name is unique per `wab_dir`, which SP-A's lock already makes one-daemon-per-name. Two instances whose producer sockets share a directory no longer collide.
- **weir-ctl derives the same default** from the `--wab-dir` / `WEIR_WAB_DIR` it already uses (weir-ctl main.rs:77).

Protocol in §11.

### 5.10 Reloader

```rust
pub(crate) struct Reloader {
  #[cfg(feature = "tls")] tls: OnceLock<ReloadableServerConfig>,   // set at startup step 6
  guard: tokio::sync::Mutex<()>, board: Arc<StatusBoard>, metrics: Arc<Metrics> }
async fn reload(&self, trigger: Trigger /*Sighup|Admin*/) -> ReloadReport
ReloadReport { trigger, at_unix_ms, outcome: Applied|Noop|Failed,
               items: [{item:"tls_certificates", outcome: reloaded|failed|not_configured, detail /*admin only*/}],
               not_reloaded: [{item:"config_file", reason:"not re-read in 4.1; edits to weir.toml/env need a restart"}] }
```

- `ReloadableServerConfig::reload` (socket/tls.rs:158-162) runs in `spawn_blocking`.
- A failure keeps the old certificates (the existing guarantee) and increments the existing `weir_tls_config_reloads_total{outcome}`.
- Every reload increments `weir_config_reloads_total{trigger,outcome}`, which is shared with SP-A.
- While the OnceLock is unset, or in a non-tls build, the outcome is `noop` with item outcome `not_configured`.
- The guard is taken only by reloaders.

### 5.11 Poll tasks (moved out of main.rs:927-1071 into `status/poll.rs`)

**500 ms task**
- Publishes `weir_queue_depth` from `QueueSender::len` (queue.rs:56).
- Publishes `weir_drain_channel_depth` from `board.drain_depth`.
- Its `queue_tx` clone is released by `abort()` plus `.await` at T1, which replaces `drop(rt)` (main.rs:1248).

**5 s task**, in order:
1. The unchanged fail-closed `compute_wab_bytes_on_disk` / `apply_wab_scan` (main.rs:83-122, 247-280), which also stamps `first_scan_ok_ms` and the last-scan time.
2. `wab::cap::evaluate`.
3. A separate, read-only `scan_wab_lag(dir, &mut FooterCache)`.
   - It never feeds `wab_bytes_now` and never calls `check_confirmed`.
   - It reports undelivered segments and records, the oldest header `created_at`, per-shard active-segment age, and dead_letter/ and quarantine/ bytes and files.
   - FooterCache is keyed by sealed path, removes entries on NotFound, and is capped at 65,536 entries.
4. Heartbeat age.
5. Counter-delta windows.
6. `evaluate`, latches, `weir_condition_active`, `weir_ready`, `weir_live`.
7. A WARN on each condition transition.
8. The existing growth and stall warnings. `drain_stall_verdict` gains the commit-in-flight exception.

**Start time.** Both tasks start after recovery and after the synchronous first scan (§6 step 9).

### 5.12 Sink selection seam

The per-sink match (main.rs:622-875) moves into `sink/select.rs::build_drain_spawner(&cfg, &metrics) -> Result<DrainSpawner, _>`. This is a mechanical move. The sink is still constructed after recovery. It is the SP-M seam.

### 5.13 Logging (SP-D, no new dependencies)

- One startup line: `info!(version, wire_version, config = ?config_view)`, using the ConfigView from §5.14.
- `.with_ansi(stderr().is_terminal())`.
- A WARN on an invalid `log_level` instead of the silent fallback (main.rs:456-457).
- The drain panic payload is logged.
- A WARN at the start of a dead-letter-blocked episode.
- The segment identity appears in every `commit_batch` log line.
- `weir_records_nack_internal_total{cause}` is incremented at the existing nack sites in connection.rs.

### 5.14 ConfigView (NEW in revision 2)

`status/config_view.rs::ConfigView::from(&Config)` builds the view by **exhaustive destructure of `Config` with no `..`**. A field added to `Config` therefore fails to compile until it has been classified as `Public | Admin | AdminRedacted | Never`.

- `sink_url` is rendered as `scheme://host[:port]` plus `path_redacted: true`. Webhook-style URLs carry the secret in the path, and today's redactors (sink/mod.rs:50-120) cover only userinfo and a fixed list of query keys.
- Bearer token, S3 secret and URL passwords are `Never`.
- The public config subset is a fixed `Public` list (§9.1).
- This is not SP-L's `config::classify` (reloadable vs restart-only). It is a disclosure classification, and SP-L reuses the destructure.

## 6. Startup sequence

**Today:** config → channel (main.rs:518) → `wab::spawn` with recovery (main.rs:559) → workers → sink and drain → **blocking** replay (main.rs:885) → runtime (main.rs:890) → metrics bind (main.rs:898) → TLS load and TCP run (main.rs:1111-1159) → Unix bind inside `socket::run` (main.rs:1215).

**Execution model.** Main builds the multi-thread runtime at step 4 and holds `let _rt_guard = rt.enter();` for the rest of its life, so the sync `tokio::net::*::bind` calls have a reactor. Each async bind runs in a short `rt.block_on(..)`. Recovery (step 8), the scans (step 9) and the joins (§7) run on the main thread **outside** any `block_on`.

**New sequence.** Each step publishes `board.life.enter(phase)` and `weir_startup_phase{phase}`. Phases: `initializing, recovering, handoff, binding, serving, shutting_down`.

0. `umask(0o077)` (main.rs:447-450). **[SP-A slot]** SIGHUP is set to `SIG_IGN`.
1. `Config::load`, then tracing (§5.13), then the version and ConfigView line, then replay the config warnings. Phase `initializing`.
2. **[SP-A slot]** `let wab_lock = WabLock::acquire(&cfg.wab_dir)?`. This is the first I/O on wab_dir, and the guard outlives every join.
3. In-memory setup:
   - `Metrics::new`, which pre-creates every new label set, plus `weir_build_info` and `weir_config_*`;
   - `wab_cap_rejecting` and `wab_bytes_now`, moved up from main.rs:946/952;
   - `drain_depth`;
   - `bounded(256)` drain channel (no clone of either end);
   - `StatusBoard::new`, `ShardRegistry::new` with every shard `Starting`, and `DrainControl::attached()`.
4. Build the runtime and enter it. Create the `Reloader`, then `Lifecycle::spawn_signal_task`. SIGTERM is handled from here.
5. Bind the metrics/health listener (same error text as main.rs:898-912) and spawn the router. From here:
   - `/livez` returns 200;
   - `/startupz` and `/readyz` return 503 `starting (phase=initializing)`;
   - `/status` and `/metrics` are served.
6. **[tls]** `ReloadableServerConfig::load` (moved up from main.rs:1111) and `reloader.tls.set(..)`. Bad certificates fail before a long recovery.
7. If `admin_socket_enabled`, resolve the admin path (§11), then `refuse_if_live`, then `bind_hardened`, then spawn the admin server.
   - **Explicitly configured path:** any failure is fatal.
   - **Derived default:** a failure (parent group/world-writable, too long, bind error) disables admin with an ERROR log and sets `AdminCell::Unavailable{reason}`, which raises the `admin_unavailable` condition.
   - `drain.pause` is accepted from here on.
8. Phase `recovering`: `wab::recover` on the main thread, while the runtime keeps serving HTTP and admin `status`.
9. Phase `handoff`:
   - `let replay = wab::scan_replay(...)?`. A scan error aborts startup, as `?` does today (main.rs:885). Publish `replay_total`.
   - **Then, synchronously:** `compute_wab_bytes_on_disk`, `apply_wab_scan` and `wab::cap::evaluate`, before any producer listener exists. Recovery has just walked the same directories, so this is cheap.
   - If this scan fails and `wab_max_bytes > 0`, set `wab_cap_rejecting = true` (fail-closed): pushes nack and `/readyz` reports `wab_cap_rejecting` until the poll task's first successful scan.
   - Without this step, a daemon restarted over its cap would read `wab_bytes_now = 0` (main.rs:946), report ready and admit pushes until the first async scan (main.rs:983-993).
10. Spawn the pipeline:
    - `wab::spawn_flushers(..)` (after recovery; the counter-safety rule at wab/mod.rs:671-702 is unchanged);
    - `worker::spawn_workers(..)` with drop guards;
    - `build_drain_spawner` and `drain::spawn_with(.., DrainRuntime{attached, board, initial_backlog: replay})` (non-blocking);
    - the two poll tasks.

    Then wait at most 10 s for `registry.all_settled()`. On timeout, WARN; unsettled shards stay `Starting` and are not ingest-capable.
11. Phase `binding`:
    - bind **all** configured producer listeners: `tcp::bind`, then `socket::bind`;
    - mark each one `Bound`;
    - only then spawn `tcp::run` and `socket::run`.
12. Store phase `serving` with Release ordering, after the listener stores, and set `ever_served`. Log "weir serving" with the elapsed time, the recovered segment count and the replay size.

**A signal before `serving`**
- The first SIGTERM sets Graceful. Main finishes the step it is in and checks `level` before step 11.
- If shutting down, main skips steps 11-12: it drops `queue_tx`, unlinks any listener that was bound but not served, and runs the shutdown path over whatever exists.
- Recovery is never interrupted. A second signal during recovery is logged and sets Forced, but recovery still completes, because a forced exit is allowed only after the joins. SIGKILL remains the escape, and it is crash-safe.

**Observable effects**
- Health, metrics and admin answer within milliseconds of process start.
- Startup time is lock + recovery + one directory scan + one size scan + flusher settle.
- testkit's `wait_ready` (harness.rs:255-262) is unaffected.

## 7. Shutdown sequence

**T0: first SIGTERM or SIGINT** (`Lifecycle::request_shutdown`, Graceful):
- (a) `board.life.shutting_down = true`, so `/readyz` returns 503 `not ready: shutting_down` on its next request.
- (b) Phase becomes `shutting_down`.
- (c) `drain_ctl.begin_shutdown()` turns on absorb mode: try_recv-drain into `pending` on every slice, and skip probes and rescans (§5.4).
- (d) The flushers' `shutting_down` flag is set. Their hand-off loops see it within 100 ms, including a flusher already blocked in a hand-off.
- (e) The watch broadcasts Graceful.
- (f) Mutating admin verbs answer `shutting_down`. `status` and `reload` keep working.

**Lame duck (immediate).**
- Each accept loop breaks, drops its listener, marks it Closed and unlinks the Unix path (inode-checked). New connects fail fast.
- Accepted connections are untouched. The JoinSet drains for at most `shutdown_timeout_secs`.

**T1: ingest closed.** Main:
1. aborts and awaits the 500 ms task;
2. keeps the runtime alive, so `/metrics`, the probes, `/status` and admin `status` stay up;
3. joins the workers;
4. joins the flushers. Each flusher seals and fsyncs its active segment and hands it off. Its hand-off time is bounded by the **per-flusher** 1 s budget (§5.5), whatever the drain is doing — absorbing, inside a commit, or wedged in a non-yielding sink call.

The joins are never skipped and have no timeout. Their bound is: queue contents + one fsync per shard + ≤ 1 s + 100 ms per remaining rotation. Flushers run concurrently, so the 1 s does not multiply by shard count.

**T2: drain stop.**
- `drain_ctl.request_stop(drain_shutdown_budget_secs)` (default 10 s) closes input and lowers the deadline (`fetch_min`). The budget starts once everything ingested is sealed.
- The drain delivers whole segments (pending, alternating with backlog) until the deadline or natural completion. At the deadline it abandons in-flight work with no `.confirmed`.
- Main polls `drain_handle.is_finished()` every 50 ms for up to deadline + `DRAIN_EXIT_GRACE` (2 s).

**T3: exit.** The summary is built **without new I/O**:
- from `DrainExit` (`pending_left`, `backlog_left`, `abandoned`);
- from `weir_wab_segments_not_handed_off_total`;
- from the last `WabCell` snapshot.

It never calls `scan_unconfirmed_sealed`, which has quarantine side effects through `check_confirmed` (wab/mod.rs:430-449, recovery.rs:864-880).

- **Drain joined:**
  1. log `shutdown: N segment(s) (~B bytes, as of <t>) left sealed+unconfirmed for replay; abandoned <seg> after R records; D dead-lettered records in its prefix will be dead-lettered again on replay`;
  2. `rt.shutdown_timeout(1 s)`;
  3. unlink the admin socket (inode-checked);
  4. drop `wab_lock`;
  5. exit 0.
- **Drain NOT joined** (a sink call that never yields):
  1. log ERROR `drain did not stop within budget; exiting with <seg> unconfirmed`, plus the summary;
  2. `std::process::exit(0)` **without dropping `wab_lock`**. The kernel releases the flock only once the process, including the orphaned drain thread, is gone.

  The absorb-mode probe skip (§5.4) and the drain runtime's `shutdown_timeout(100 ms)` keep a merely slow sink off this path.

**Second signal, at any point after T0** (Forced):
- WARN "forced shutdown".
- `drain_ctl.force_stop()` sets `stop`, `absorb` and `deadline.fetch_min(now)`. A later `request_stop` cannot raise it.
- The socket layers `abort_all`.
- The worker and flusher joins still run, still bounded as above.
- The drain wait shrinks to `DRAIN_EXIT_GRACE`.
- A third or later signal only logs.

**Bound.** `shutdown_timeout_secs` + join time (queue flush + fsync + ≤ 1 s + 100 ms per rotation) + `drain_shutdown_budget_secs` + 2 s. At defaults that is about 30 + 1 + 10 + 2 s. The only unbounded term is a flusher stuck inside kernel fsync, where only SIGKILL helps, and that is crash-safe.
- **Documented grace:** ≥ `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` = **45 s**.
- weir.service `TimeoutStopSec` 35 → 45 (weir.service:69).
- compose `stop_grace_period: 45s`.
- k8s `terminationGracePeriodSeconds: 45`.

**Duplicate and duplicate-dead-letter window (stated).** For each abandoned in-flight segment, at most one per shutdown, forced or not:
- its committed prefix (records ≤ `processed`) is re-delivered on replay (the existing H2 window);
- a commit cancelled at the deadline may already have been applied by the sink (one more batch);
- records that the prefix permanently rejected are **dead-lettered again** on replay, and the duplicates count against `dead_letter_max_bytes`.

A segment that a flusher did not hand off was never started, so it adds no duplicates. This is the same window as a crash today. It is documented in configuration.md, the release notes and the exit log line.

## 8. Drain control state machine

```
                 request_pause (any time, incl. before spawn)
  +-----------+  head check, before next segment       +-----------------------+
  | Draining  |--------------------------------------->| Draining[paused]      |-- wait_slice(50ms floor):
  | (pending  |<---------------------------------------| (gauge: draining)     |   chan->pending, depth,
  |  ⇄backlog)|            request_resume              +-----------------------+   heartbeat, health poll
  +-----+-----+
        | process_segment_ctl: pause seen before a commit_batch -> Paused{processed}
        +--------------------------------------------->+-----------------------+
        |                                               | Held{seg, processed,  |-- same hold loop; resume ->
        |                                               |  resume: Fresh|Retry} |   process_segment(processed)
        |                                               +-----------------------+   (zero added duplicates)
        | -> Transient                                  +-------------------------------+
        +---------------------------------------------->| RetryingTransient             |
        |                                               |  wait ends iff remaining=0    |
        |  <-- Confirmed / Quarantined / Stranded ------|        && !pause_requested    |
        |                                               |  requeue ticket -> cut backoff|
        |                                               |  once/episode, >= base delay  |
        | DL full                                       +-------------------------------+
        +---------------------------------------------->| BlockedDeadLetterFull (unchanged)
  any arm: absorb (T0) -> try_recv all into pending; skip probes + rescans
  any arm: stop && deadline_passed -> exit (in-flight -> Abandoned, never confirmed)
  any arm: stop && input_closed && all queues + held + channel empty -> exit (natural completion)
  Held / paused at stop -> exit at once (pause honoured)
  Attached: Disconnected = non-event (log once, sleep slices).  Detached (tests): Disconnected = today's exit.
```

**Verb latency**
- **Pause:** at most one in-flight commit (≤ `commit_timeout`). A commit already in flight always finishes.
- **Resume:** within 50 ms.
- **Retry-stranded:** at the next head, slice or held iteration, and at most once per 5 s; extra requests coalesce.

**Idempotent, last writer wins.** The admin server's mutation guard serializes pause and resume. The guard is released after the atomic store and **before** the observation wait. Every verb is logged with the peer uid.

**Generations.** `drain.generation` increments on every `drain_thread` start; SP-G and SP-M use it. Requeue requests use their own monotonic ticket.

**Refusals**
- After T0, mutating verbs are refused with `shutting_down`.
- With `exit=gave_up`, drain verbs return `unavailable`.

## 9. Status model and Conditions catalogue

### 9.1 Document `weir.status/v1`

**Conventions.** Times are unix-ms integers, ages are f64 seconds, and an unknown value is `null`. The document is additive-only. There are two serde types: `AdminStatusDoc`, and `PublicStatusDoc`, which is derived from it.

**Fields** (fields marked "admin only" exist only on the admin type)
- **Top level:** `schema`, `view: public|admin`, `version`, `wire_version`, `generated_at_ms`, `started_at_ms`, `uptime_secs`.
  - Admin only: `pid`, `instance:{wab_dir, socket_path, wab_dir_dev, wab_dir_ino}`.
- **`verdict`:** `{ready, live, startup_complete, livez_checks:[..], readiness_failing:[code], liveness_failing:[code]}`.
- **`conditions`** and **`unevaluated`**.
  - Public conditions carry `code, severity, affects, since_ms, summary, evidence (non-Text values only), runbook, gates`.
  - Admin adds `detail` and `Text` evidence.
- **`startup`:** `{phase, phase_since_ms, replay:{total, remaining}}`.
- **`lifecycle`:** `{shutting_down, shutting_down_since_ms, forced, drain_deadline_ms}`.
- **`ingest`:** `{listeners:[{kind, state}], queue_depth, wab_cap:{max_bytes, low_water_bytes, bytes_now, rejecting, last_scan_ok_ms}}`.
- **`shards`:** `[{id, state, reason, since_ms, respawns, worker_running, handoff_blocked_since_ms, active_segment_age_secs}]`.
- **`drain`**
  - `state` (draining|retrying_transient|blocked_dead_letter_full|stopped), `state_since_ms`, `paused`, `pause_requested`, `held`, `paused_since_ms`;
  - admin only: `paused_by_uid`;
  - `generation`, `observed_requeue`;
  - `in_flight:{segment, attempt, retries_left, processed_records, next_retry_at_ms, commit_in_flight_since_ms}|null`;
  - `pending_segments`, `backlog_segments`, `channel_depth`, `channel_capacity: 256`, `heartbeat_age_secs`, `blocked_since_ms`;
  - `last_requeue:{ticket, requeued, already_queued, backoff_cut, coalesced, scan_failed: bool, at_ms}|null` (admin adds `scan_error`);
  - `exit: running|stopped|gave_up` (admin adds `last_panic`).
- **`sink`:** `{type, health, health_since_ms, last_success_ms, last_failure:{at_ms, kind}|null, consecutive_failures}`.
  - Admin only: `last_failure.message`, `probe_reason`.
- **`backlog`:** `{undelivered_segments, undelivered_records, oldest_undelivered_age_secs /*upper bound*/, dead_letter_bytes, dead_letter_max_bytes, quarantine_bytes, quarantine_files, scanned_at_ms}`.
- **`reload`:** `{last:{trigger, at_ms, outcome, items:[{item, outcome}], not_reloaded:[{item, reason_code}]}|null}`. Admin adds `items[].detail`.
- **`admin`:** `{protocol: 1, verbs: [...], state: disabled|bound|unavailable, reason_code}`. Admin adds `path`.
- **`config`:**
  - public: the fixed numeric subset — shard_count, worker_count, wab_max_bytes, dead_letter_max_bytes, wab_segment_max_bytes, the segment age and lifetime timers, sink_max_retries, sink_timeout_secs, health_poll_interval_secs, shutdown_timeout_secs, drain_shutdown_budget_secs, max_connections, peer_uid_check, tls_enabled;
  - admin: the full ConfigView (§5.14).

**Redaction rules**
- The public type contains no free text: no panic message, no io-error string, no sink error, no probe reason, no reload detail, no uid, no pid, no absolute path, and no sink URL, host or bucket.
- Public `summary` strings come from per-code templates whose parameters are typed as numbers, enums, codes, shard ids and segment ids only. A template cannot accept a `&str`.
- `/livez?verbose` and `/readyz` details render `code` plus the public `summary` only.
- Bearer token, S3 secret and URL passwords never enter the board.

### 9.2 Wait-freedom argument

1. **Flushers** gain:
   - one `drain_depth` store per successful hand-off;
   - one Relaxed `shutting_down` load per 100 ms *of blocking*;
   - registry stores at supervisor events, at counter-scan completion and at hand-off block/unblock edges.

   Nothing is added per record or per non-blocking rotation, and `flush_batch` ack resolution is unchanged.
2. **Workers** gain only a drop guard.
3. **The drain** does atomic stores and ArcSwap stores. ArcSwap writers never wait for readers (the same primitive as socket/tls.rs:126-158).
4. **Readers** do loads and `load_full`, then serialize from their own copies.
5. **No Mutex or RwLock in `src/status/`.** A source-scan test enforces this and is verified by mutation. The only locks are `Reloader.guard` and the admin mutation guard, and neither is on the data path.
6. **Metric families** pre-create every label set in `Metrics::new`, so after startup `Family::get_or_create` only ever takes the read lock.
7. **Drain channel.** No handle outside the pipeline (§4). Depth comes from `len()` snapshots. A dead drain drops the only Receiver, and the flushers' hand-off gets `Disconnected` and continues.
8. **Disk I/O.** HTTP and admin handlers do none. Disk facts are at most 5 s old.

### 9.3 Conditions catalogue

**Format.** Each entry gives `code | severity | affects | trigger | public summary template ({} = typed evidence) | gates`. Admin `detail` may add free text where noted. Runbook anchors are `docs/monitoring.md#cond-<code>`.

**Gate letters**
- **R** forces `/readyz` to 503.
- **L\*** marks a check selectable in `livez_checks`. It is evaluated only while `phase == serving`.

**Ingest and lifecycle**
1. `starting` | info | ingest | trigger: phase < serving | summary: "startup phase {phase} for {s}s; the producer socket binds after recovery seals open segments" | gates: R
2. `shutting_down` | info | ingest | trigger: shutting_down | summary: "graceful shutdown; new connections refused; drain has {left}s of its {budget}s budget" | gates: R
3. `listener_down` | critical | ingest | trigger: a configured producer listener is Failed, or Closed outside shutdown | summary: "{kind} listener is not accepting; restart re-binds it" | gates: R, L\*
4. `no_live_shard` | critical | ingest | trigger: serving and ingest_capable_count == 0 | summary: "every shard's flusher or owning worker is offline; new connections are refused; restart re-runs recovery" | gates: R, L\*
5. `shard_offline` | critical | ingest | trigger: 0 < offline < shard_count, outside shutdown | summary: "shard(s) {ids} offline ({reason}); new connections avoid them; connections already pinned to them nack until they reconnect; restart to recover" | gates: none
6. `worker_exited` | critical | ingest | trigger: a worker is not running, outside shutdown | summary: "worker {w} exited; shards {ids} cannot accept records" | gates: L\*
7. `wab_cap_rejecting` | critical | ingest | trigger: cap flag set | summary: "WAB holds {bytes} of the {cap} cap; pushes nack until below {low_water}; the root cause is delivery (see drain/sink conditions)" | gates: R
8. `drain_backpressure` | warning | ingest | trigger: any shard has `handoff_blocked_since` older than 1 s, OR channel_depth ≥ 230 for 10 s | summary: "drain channel {depth}/256; shard(s) {ids} blocked handing off for {s}s; acks wait behind the hand-off, so producers see ACK_TIMEOUT nacks of durable records (delivery, not storage)" | gates: none
9. `ack_timeouts_recent` | warning | ingest | trigger: weir_ack_timeout increased within 5 min | gates: none | cause inferred:
   - drain_backpressure active → "delivery is stalling ingest";
   - else fsync failures increased or coalesce EWMA > 1 s → "storage is slow or failing";
   - else → "a flusher is not completing groups; check shards".
10. `wab_scan_stale` | warning | ingest | trigger: no successful scan for 30 s, or none yet since start | summary: "wab_max_bytes is enforced against a stale size (fail-closed); lag figures are stale" | gates: none
11. `wab_growing_unbounded` | warning | durability | trigger: the existing `should_warn_wab_growth` (main.rs:302-310); `drain_is_healthy` (main.rs:327-346) also returns false while paused or held | gates: none

**Delivery**

12. `drain_stopped` | critical | delivery | trigger: exit=gave_up, outside shutdown | summary: "drain supervisor gave up after {n} panics; ingest continues, nothing is delivered, the WAB grows; sink_health is stale; restart resumes" | detail (admin): the last panic message ≤ 256 bytes | gates: L\*
13. `drain_wedged` | critical | delivery | gates: L\* | trigger, either:
    - commit in flight AND commit_age > commit_timeout + 15 s; or
    - no commit in flight AND last_progress_age ≥ health_poll_interval + 15 s, where last_progress = max(heartbeat change, CommitGuard stamps).

    summary: "drain thread is blocked inside a sink call that does not yield; a restart re-delivers the in-flight segment from record 0"
14. `drain_paused` | warning | delivery | trigger: pause requested or in effect | summary: "delivery paused since {t} ({held: mid-segment at record {n}}); ingest keeps buffering; WAB grows{ without bound: wab_max_bytes=0}; restart also resumes" | detail (admin): the uid | gates: none
15. `drain_blocked_dead_letter_full` | critical | delivery | trigger: state=blocked | summary: "dead_letter/ at {dl}/{cap}; free space (weir-ctl dl requeue/drop) or raise the cap" | gates: none
16. `delivery_stalled` | warning | delivery | trigger: undelivered > 0 AND no successful commit for 300 s AND not paused | gates: none
17. `delivery_lagging` | warning | delivery | trigger: oldest_undelivered_age > 900 s (an upper bound) | gates: none
18. `segments_stranded` | warning | delivery | trigger: stranded_total increased since the last successful commit | summary: "segments exhausted retries; auto-resumed on a healthy probe or via `weir-ctl drain retry-stranded`; each resume re-delivers from record 0" | gates: none
19. `sink_down` | warning | delivery | trigger: probe health = down | summary: "sink unreachable since {t}; weir keeps buffering durably (not a readiness failure)" | detail (admin): the probe reason | gates: none
20. `sink_degraded` | info | delivery | trigger: probe health = degraded | gates: none
21. `sink_failing` | warning | delivery | trigger: consecutive_failures ≥ 3 while health ≠ down | summary: "commits fail while the probe passes; last kind {kind}" | detail (admin): the last error | gates: none
22. `dead_letter_near_cap` | warning | delivery | trigger: ≥ 80% of the cap | gates: none
23. `noop_sink` | warning | delivery | trigger: sink_type=noop | summary: "records are acked and DISCARDED" | gates: none
24. `active_segment_aging` | info | delivery | trigger: an active segment is older than 3600 s and both seal timers are 0 | gates: none

**Durability**

25. `fsync_failures_recent` | critical | durability | trigger: increased within 15 min; windowed, never sticky (replaces weir-readiness.sh:113-116) | gates: none
26. `flusher_respawned_recent` | warning | ingest | trigger: panics increased within 15 min and every shard is up (replaces weir-readiness.sh:118-122) | gates: none
27. `quarantine_nonempty` | warning | durability | trigger: quarantine bytes > 0 | gates: none
28. `recovery_incomplete` | critical | durability | trigger: weir_recovery_segments_failed or weir_recovery_quarantine_copy_failed > 0 in this process | gates: none

**Config and control surface**

29. `tls_reload_failed` | warning | ingest | trigger: the last TLS reload failed; sticky until a success | summary: "previous certificates still served" | detail (admin): the error | gates: none
30. `admin_unavailable` | warning | ingest | trigger: admin enabled but `AdminCell::Unavailable` | summary: "admin socket not bound ({reason_code}: parent_insecure|path_too_long|in_use|bind_failed); weir-ctl drain/reload unavailable, kill -HUP still reloads" | gates: none

**Emitted only by weir-ctl offline:** `daemon_unreachable` | critical | ingest, and `recovery_pending` | info.

**Reserved, not emitted in 4.1:** `config_pending_restart` (SP-L), `tls_cert_expiring` (SP-L), `confirm_retrying` (SP-B).

**Thresholds** are constants in `weir_status::Thresholds` (owner decision).

**Contract**
- Every code in `ConditionCode::ALL`, plus the offline-only codes, has a `#### cond-<code>` heading.
- docs_drift extends `every_alert_runbook_anchor_resolves_to_a_heading` (tests/docs_drift.rs:430-488).
- Every `weir_condition_active{code=…}` reference in an alert names a real code.

## 10. HTTP routes contract

**Listener.** The existing `metrics_bind:metrics_port` (default 127.0.0.1:9185).

**Connection phases** (§5.8):
- head pool 64 with a 1 s head deadline;
- metrics route on `metrics_max_connections`;
- probe and status routes on a dedicated pool of 4;
- writes bounded by 5 s.

**Parsing**
- Read until `\r\n\r\n`, 4096 bytes or the head deadline.
- Split `METHOD SP target SP version` and strip `?query`.
- An unparseable request line gets **metrics, as today**.
- Matching is exact and case-sensitive against `/livez`, `/readyz`, `/startupz` and `/status`, with no trailing-slash folding.
- **Every other path** gets today's response with a byte-identical head and body format: `200`, `Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8`, Content-Length, then the body, with no new headers. weir-ctl's `GET /metrics HTTP/1.0` (weir-ctl main.rs:2189-2205) and every scrape config keep working.
- **Behaviour change for metrics paths, release-noted:** a client that sends no complete request head within 1 s is closed. Prometheus and curl send the head immediately.
- The four routes accept GET and HEAD; any other method gets `405` with `Allow: GET, HEAD`.
- Route responses carry `Cache-Control: no-store` and `Connection: close`. No route reads a body, and no route mutates state.

**`GET /livez`** (text/plain)
- With `livez_checks = []`: `200 ok\n` whenever the runtime answers.
- Otherwise: `503 not live: <code>[,<code>]\n` when any selected check is active.
- `?verbose` prints `[+]<code> ok` or `[-]<code> failed: <public summary>`.
- It never fails on startup, shutdown, sink, retry, blocked or pause.

**`GET /readyz`** (text/plain): `200 ok\n` iff all of:
- `phase == serving`;
- not shutting down;
- `!wab_cap_rejecting`;
- `ingest_capable_count ≥ 1`;
- no `listener_down`.

Otherwise `503 not ready: <code>[, <code>]\n`. It is evaluated live from atomics on every request. `?verbose` gives per-check lines with the public summary.

**`GET /startupz`**
- `200 ok\n` once `ever_served` is set, and sticky after that.
- Before that: `503 starting: phase=<p> since=<s>s replay=<n>\n`.

**`GET /status`**
- Always `200 application/json; charset=utf-8` with the `PublicStatusDoc`.
- `500` with an empty body on a serialization error.
- Bounded at 256 shards and 40 conditions.

**Compatibility and security**
- The four paths become reserved.
- New weir-ctl detects an old daemon from `/status`'s openmetrics content type.
- The listener stays unauthenticated and bound to 127.0.0.1 by default; k8s binds 0.0.0.0.
- threat-model.md gets the rows and actors listed in §17.
- The PR includes a security-review step.

## 11. Admin socket protocol

**Config**
- `admin_socket_enabled` (bool, default `true`).
- `admin_socket_path`:
  - default `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` (§5.9);
  - must be absolute, with no `..`, at most 103 bytes, and not under `wab_dir`;
  - must differ from `socket_path` after lexical normalization (`.` and `//` removed), and, after both binds, by parent (dev, ino) plus basename. This catches symlinked parents, e.g. `/var/run` → `/run`.
- One env var for both binaries: `WEIR_ADMIN_SOCKET_PATH`.
- Unix only.

**Bind**
- The parent directory must not be group- or world-writable. The admin bind **refuses** (it does not just warn, as the producer socket does at socket/mod.rs:620-646), because a squatter in such a directory could impersonate the control plane.
- Then `refuse_if_live`, then `bind_hardened` (umask 0o177 → 0600, inode re-check), then record (dev, ino).
- On failure: fatal for an explicit path; `admin_unavailable` for the derived default (§6 step 7).

**Server-side authorization**
- On **every** accept, `peer_uid_of` must be the daemon euid or 0 (owner decision, §20).
- The check is always on and independent of `peer_uid_check`. It fails closed on a platform without peer credentials.
- A rejected peer gets one `forbidden` line, then close, plus `weir_admin_requests_total{verb="_connect",outcome="refused"}` and a WARN.

**Client-side server authentication (weir-ctl).** Before sending:
- `lstat` the path: it must be S_IFSOCK, mode 0600, and its parent not group- or world-writable;
- after connect, `peer_uid_of(stream)` must equal weir-ctl's euid, or the owner of `--wab-dir`, or 0.

After the first response:
- if `--wab-dir` was given (or derived), `instance.wab_dir_dev/ino` must match `stat(--wab-dir)`.

Any mismatch: exit 2 with `identity mismatch`, and never trust the payload.

**Framing**
- UTF-8 JSON lines, one object per `\n`, in both directions; requests processed in order, one response each.
- The 64 KiB line limit is enforced while reading (bounded reader); over it, `request_too_large`, then close.
- Per-request deadline: 5 s from first byte to newline. Idle timeout between requests: 10 s. Write timeout: 5 s. At most 64 requests per connection.
- At most 4 concurrent connections; an excess connection gets one `busy` line, then close.
- Unknown request fields are ignored.

**Envelope**
- Request: `{"v":1,"id":<any JSON, echoed>,"verb":"…","args":{…}}`.
- Success: `{"v":1,"id":…,"instance":{"pid","started_at_ms","wab_dir","socket_path","wab_dir_dev","wab_dir_ino"},"ok":true,"result":{…}}`.
- Failure: the same envelope with `"ok":false,"error":{"code","message","retryable"}`.
- `instance` is on every response, so weir-ctl can always print which daemon it acted on.

**Verbs**
- **`status`** (`{}`): the `AdminStatusDoc` (`view:"admin"`). Same model and same evaluator as `/status`.
- **`reload`** (`{}`): calls the same `Reloader::reload` as SIGHUP and returns the `ReloadReport`.
  - A failed TLS reload is `ok:true` with `outcome:"failed"`.
  - Non-TLS build, or no `tcp_bind`: `noop`.
- **`drain.pause`** (`{"wait_ms":0..30000, default 5000}`) returns:
  `{"generation","pause_requested":true,"paused":bool,"held":bool,"paused_since_ms","held_segment":{segment,processed_records}|null,"warnings":[…],"note":"takes effect before the next commit batch; in-memory only — a restart resumes delivery"}`.
  The `warnings` include "wab_max_bytes is 0: the WAB grows without bound while paused" when that applies.
- **`drain.resume`** (`{"wait_ms"}`) returns `{"generation","pause_requested":false,"was_paused":bool,"paused_for_secs":…|null}`.
- **`drain.retry_stranded`** (`{"wait_ms", default 10000}`) returns `{"ticket","completed":bool,"coalesced":bool,"requeued":n|null,"already_queued":n|null,"backoff_cut":bool,"scan_error":…|null,"paused":bool,"note":"requeued segments restart from record 0; a running retry backoff is cut at most once, never below base_retry_delay"}`.

**Errors:** `bad_request`, `unsupported_version` (with `"supported":[1]`), `unknown_verb`, `request_too_large`, `forbidden`, `busy`, `shutting_down`, `unavailable`, `internal`.

**Concurrency**
- `status` reads the board directly.
- Mutating verbs take the admin mutation guard only for the atomic store, release it, then poll the board every 20 ms until the target generation or ticket is observed or `wait_ms` expires.
- Neither the drain nor the Lifecycle ever awaits a client.
- Every mutating verb is logged at INFO with `{verb, peer_uid, outcome}`.
- Every request increments `weir_admin_requests_total{verb,outcome}`. The verb label is bounded to the 5 verbs plus `_connect` and `_invalid`.

**Versioning.** `v` is the protocol version. New verbs and keys are additive within v1. The daemon answers `unknown_verb` for verbs it does not know, and the document lists `admin.verbs`.

## 12. Readiness and liveness

**Readiness.** See §10 and owner decision 4. Sink, drain, pause, lag, dead-letter, quarantine and fsync/panic history never gate it.
- Live-shard routing makes "at least one shard up" an honest bar.
- Pinned connections to a dead shard keep nacking until they reconnect (SP-G closes them).
- The cap flag is fail-closed from startup and refreshed by the poll task.
- There is no write probe.

**Liveness** is opt-in through `livez_checks` (default `[]`). Selectable checks: `no_live_shard`, `listener_down`, `worker_exited`, `drain_stopped`, `drain_wedged`.
- The docs recommend the ingest-dead set.
- `drain_stopped` and `drain_wedged` are deliberate opt-ins; `drain_wedged` only after SP-B has soaked.
- The wedge verdict is truthful without SP-B, because the RAII CommitGuard stamps commit start and end.

**Probe recipe** (docs only; manifests are SP-O)
- **k8s:**
  - startupProbe `/startupz`: `periodSeconds: 5`, `timeoutSeconds: 2`, failureThreshold sized for recovery only.
  - readinessProbe `/readyz`: `periodSeconds: 5`, `timeoutSeconds: 2`, `failureThreshold: 2`.
  - livenessProbe `/livez`: `periodSeconds: 10`, `timeoutSeconds: 2`, `failureThreshold: 6`. This absorbs a minute of probe-pool pressure before a restart.
  - A NetworkPolicy that restricts 9185 to kubelet and Prometheus is strongly recommended.
- **Docker HEALTHCHECK** (Dockerfile:127-128): `bash -c 'exec 3<>/dev/tcp/127.0.0.1/9185 && printf "GET /livez HTTP/1.0\r\n\r\n" >&3 && read -r l <&3 && [[ $l == *" 200 "* ]]'` with `--start-period=10s`.

**`deploy/systemd/weir-readiness.sh` rewrite** (a bare-metal and cron tool; the k8s claim is removed)
- **Source.** If weir-ctl is present, run `weir-ctl status --json` and map its exit code. Otherwise use `GET /readyz` plus `/status`. An openmetrics body means a pre-4.1 daemon: reachable = ready.
- **Exit codes**, aligned with weir-ctl (§14):
  - **0** READY;
  - **1** NOT READY (prints the codes) — **if and only if** `/readyz` would return 503;
  - **2** DEAD, unreachable, or identity mismatch (`fail_dead`, line 57);
  - **3** READY but a critical condition that does not gate readiness is active (e.g. `shard_offline`, `drain_stopped`). Returned only with `--fail-on-critical`; otherwise exit 0 with WARN lines.
- The sink-down, blocked and lifetime-counter gates (lines 98-122) become advisory WARN lines.
- `--allow-noop` suppresses the `noop_sink` WARN.

## 13. Metrics and config additions

**Rules.** All metrics are additive: no renames, and no labels added to existing families. Every new family pre-creates its full label set.

**Gauges**
- **Build and lifecycle:** `weir_build_info{version,wire_version}`, `weir_start_time_seconds`, `weir_startup_phase{phase}` (one-hot), `weir_ready`, `weir_live`, `weir_condition_active{code,severity}` (30 pre-created series).
- **Shards and cap:** `weir_wab_shard_up{shard}`, `weir_wab_handoff_blocked_seconds{shard}`, `weir_worker_up{worker}`, `weir_wab_cap_rejecting`.
- **WAB backlog:** `weir_wab_undelivered_segments`, `weir_wab_undelivered_records`, `weir_wab_oldest_undelivered_age_seconds`, `weir_wab_active_segment_age_seconds{shard}`.
- **Drain:** `weir_drain_channel_depth`, `weir_drain_channel_capacity`, `weir_drain_pending_segments{queue=live|backlog}`, `weir_drain_replay_remaining_segments`, `weir_drain_heartbeat_age_seconds`, `weir_drain_commit_in_flight_seconds`, `weir_drain_paused`, `weir_drain_generation`, `weir_drain_shutdown_deadline_seconds`.
- **Sink:** `weir_sink_last_success_timestamp_seconds`, `weir_sink_last_failure_timestamp_seconds{kind}`, `weir_sink_consecutive_failures`.
- **Admin:** `weir_admin_socket_up`.
- **Effective config:** `weir_config_{wab_max_bytes, dead_letter_max_bytes, wab_segment_max_bytes, segment_max_age_seconds, segment_max_lifetime_seconds, sink_max_retries, shutdown_timeout_seconds, drain_shutdown_budget_seconds}`.

**Counters**
- `weir_records_nack_internal_total{cause=queue_full|write_failed|sender_dropped|ack_timeout|wab_cap|coordinate}`
- `weir_config_reloads_total{trigger,outcome}`
- `weir_admin_requests_total{verb,outcome}`
- `weir_connections_refused_no_live_shard_total`
- `weir_drain_segments_abandoned_total`
- `weir_wab_segments_not_handed_off_total`
- `weir_http_requests_dropped_total{reason=head_timeout|head_pool_full|metrics_pool_full|probe_pool_full}`

**Cadence change only.** `weir_dead_letter_bytes_on_disk` and `weir_quarantine_bytes_on_disk` are now also set by the 5 s task. The drain's own writes remain, except that they are skipped in absorb mode.

**Config keys** (CLI > env > TOML; `check_range` and KNOWN keys updated)

| Key | Default | Range / rule | CLI | Env |
|---|---|---|---|---|
| `drain_shutdown_budget_secs` | 10 | 0..=3600 | `--drain-shutdown-budget-secs` | `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS` |
| `admin_socket_enabled` | true | bool | none | `WEIR_ADMIN_SOCKET_ENABLED` |
| `admin_socket_path` | `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` | absolute, no `..`, ≤ 103 B, not under wab_dir, ≠ socket_path (normalized + (dev,ino)) | `--admin-socket-path` | `WEIR_ADMIN_SOCKET_PATH` |
| `livez_checks` | `[]` | subset of {no_live_shard, listener_down, worker_exited, drain_stopped, drain_wedged} | `--livez-checks a,b` | `WEIR_LIVEZ_CHECKS` |

**Constants, not knobs:**
- `DRAIN_EXIT_GRACE` 2 s, `FINAL_HANDOFF_TIMEOUT` 1 s, the hand-off slice 100 ms, the settle wait 10 s, `REQUEUE_MIN_INTERVAL` 5 s;
- `HEAD_READ_SLOTS` 64, `HEAD_READ_TIMEOUT` 1 s, `PROBE_SLOTS` 4;
- the admin limits and the condition thresholds.

**Downgrade to 4.0.x is safe.** Unknown TOML keys warn and are ignored (config/file.rs:303-337).

**Dependencies**
- `arc-swap` becomes unconditional. Today it is optional behind `tls` (Cargo.toml:28,151), and it is already in the lock.
- `serde_json` becomes a normal dependency of weir-server.
- No new third-party crates.
- MSRV 1.88 is unaffected (`JoinHandle::is_finished`, `IsTerminal`, `OnceLock`, `AtomicU64::fetch_min`, tokio 1.52's `watch::Receiver::wait_for`).

## 14. weir-ctl changes

**`weir-ctl status [--admin-socket P] [--addr H:P] [--wab-dir D] [--source auto|admin|http|metrics|disk] [--json]`**

The admin socket defaults to `WEIR_ADMIN_SOCKET_PATH`, then to the path derived from `--wab-dir` / `WEIR_WAB_DIR`, then to `/var/lib/weir/wab.admin.sock`.

In auto mode the first success wins:
1. **Admin socket**, after the client-side identity checks (§11).
2. **`GET /status`**, accepted only for a JSON content type with `schema == weir.status/v1`.
3. **`GET /metrics` plus disk.** Reads `weir_condition_active` if present. Otherwise `evidence_from_metrics` is merged with disk Evidence and the same `evaluate` runs locally.
4. **Disk only.**

Each failed source is recorded in `sources_tried` with its reason.

**Disk evidence** (`offline.rs`, read-only). It never takes or probes the SP-A lock and never calls `check_confirmed`. It collects:
- per shard: undelivered sealed segments, records from `parse_segment_footer`, age from `parse_segment_header`;
- active `.wab` age;
- last delivery, from the newest `.confirmed`;
- dead_letter/ and quarantine/ bytes.

It also excludes those two directories from shard scans, which fixes the miscount at weir-ctl main.rs:506.

**Human output.**
- A verdict line: `READY` / `NOT READY: <codes>` / `UNREACHABLE`.
- Conditions sorted by severity.
- Drain, sink, backlog and shard blocks.
- An `instance:` line (admin source).
- A `source:` footer and any `unevaluated:` codes. These are never shown as healthy.

**`--json`.** The status document as rendered, plus the additive top-level keys `source` and `sources_tried`. A key-shape test pins it.

**Exit codes** (one table, shared with weir-readiness.sh):
- **0** ready, and no critical condition;
- **1** not ready (exactly when `verdict.ready == false`, i.e. `/readyz` would be 503);
- **2** unreachable with no offline facts, or identity mismatch;
- **3** ready, but a critical condition that does not gate readiness is active (any `affects`).

**Parity test:** for identical Evidence, exit ∈ {0,3} ⇔ `readiness().ok` ⇔ `/readyz` 200.

**`weir-ctl reload [--admin-socket P] [--json]`.** Exits 0 for applied or noop, 1 for failed. Without a socket it exits 2 with the hint "`kill -HUP <pid>` uses the same code path".

**`weir-ctl drain pause|resume|retry-stranded [--wait[=SECS]] [--admin-socket P] [--wab-dir D] [--json]`.**
- Exits 0 when the action was observed, 4 when it was requested but not yet effective, 1 on an error response, and 2 when unreachable or on identity mismatch.
- Pause always prints "in-memory only; a daemon restart resumes delivery".
- `--json` prints the `result`; errors keep `error_json` (weir-ctl main.rs:336-341).
- Mutating verbs have no offline fallback.

**Changed (SP-D).**
- `metrics` gains summary keys only.
- Its warnings list STOPPED and BLOCKED first, plus "sink_health is stale while the drain is stopped" and an offline-shard list from `weir_wab_shard_up`.
- `segments` skips `quarantine/` and gains `--files`.
- The header comment (weir-ctl main.rs:1-5) is updated.

**Unchanged:** `health`, `push`, `dl *`, `quarantine *`, `attest *` (writ's `attest --json` contract).

## 15. Invariant analysis

**Status board, metrics, poll tasks**
- **False ack:** none. No ack decision reads them except `wab_cap_rejecting` (below).
- **Loss / duplicates / ordering:** none.
- **Deadlock:** no locks added, and no channel handle outside the pipeline.
- **Ack path:** nothing added per record.

**Cap refresh and startup scan**
- The hysteresis is identical. Failed scans leave the value unchanged.
- The startup fail-closed setting (`rejecting = true` if the first scan fails and the cap is > 0) can only cause NACKs, never a false ack.
- Push-path and poll-task writes compute the same idempotent function.

**Lag scan.** Read-only, never calls `check_confirmed`, tolerates NotFound.

**Timed hand-off loop (the only new branch near the ack path)**
- Every hand-off sends a path from a successful `seal_current` (seal fsyncs), and acks are appended after it (wab/mod.rs:981) and resolved after it (wab/mod.rs:1050). Whether it is handed off or not, the acks are true.
- A segment that is not handed off is sealed, unconfirmed and replayed.
- Outside shutdown, the loop never returns `NotHandedOff` except on Disconnected, so backpressure is identical to a blocking send.

Pinned by:
- the invariant comment;
- unit test A: a pre-filled bounded(1) channel with no consumer, flag already set — the flusher returns within ~1.1 s, the segment is `.wab.sealed` with a valid footer, and every ack resolves true;
- unit test B: the same channel, flag flipped 500 ms **after** the flusher blocked — the join finishes within ~1.7 s;
- a deterministic DST seed family with a bounded channel through `SimClock::send_timeout`.

**Startup restructure**
- **Counter safety:** recovery completes before any flusher exists, and the producer bind waits for the flushers to settle.
- **Readiness** cannot report before recovery.
- **Replay:**
  - the identical `scan_unconfirmed_sealed` list moves in memory into `backlog`;
  - a drain panic before consuming it triggers a rescan (`initial_health_ok=false`, drain/mod.rs:249-256);
  - the B3 deadlock cannot occur.
- **Ordering:** replay segments share no connection with live traffic. The main.rs:883-884 comment is rewritten as consumption policy.

**Lifetime decoupling**
- The `shard_count=1` zombie is gone.
- Every path after the drain spawns calls `request_stop`. An early `?` after the spawn means process exit (crash-equivalent).
- The PR 1+ system test asserts that SIGTERM exits within the bound on every PR.

**Pause**
- No false ack, because pause acts on the drain side only.
- No loss: segments stay sealed and unconfirmed.
- **Zero added duplicates:**
  - Draining-head pause starts no segment;
  - Held is entered *before* a commit with `processed` equal to the committed prefix;
  - the RetryingTransient hold keeps the F05 cursor.
- A respawn while held re-delivers from record 0 (the existing H2 window).
- No deadlock: every hold consumes the channel on 50 ms slices.
- No CPU spin: the 50 ms floor.
- Disk usage is bounded only by `wab_max_bytes` (stated trade-off).

**Retry stranded and auto-resume**
- Both drain the channel first and dedup against `pending ∪ backlog ∪ held ∪ in_flight`.
- Any residual double-queue is absorbed by the NotFound no-op (drain/mod.rs:1116-1122) and by `confirm_and_delete` skipping missing files (confirmed.rs:28-35).
- Coalescing plus the 5 s floor bound scan cost. Cutting the backoff at most once per episode, and never below `base_retry_delay`, preserves W2.

**Bounded shutdown**
- `confirm_and_delete` is reachable only from `Confirmed` (drain/mod.rs:1015, 1270-1316).
- A forced exit happens only after the worker and flusher joins, which are now bounded in every drain state.
- No loss. Duplicates as stated in §7.
- The T3 summary does no I/O, so it cannot quarantine anything.
- A torn `.confirmed` is possible only on the orphan path (`write_confirmed_durably` writes in place, confirmed.rs:65-80). Recovery then quarantines an already-delivered segment (recovery.rs:864-880): noise, not loss.
- The lock is held until the kernel reaps the process.

**Second signal.** `fetch_min` makes the forced deadline final. It shortens only the connection drain and the drain wait.

**Lame duck.** Only new connects are affected, and the unlink is inode-checked.

**Live-shard routing.** Pinning at accept is unchanged, so per-connection ordering is unchanged.

**HTTP and admin**
- HTTP is read-only, and the public type carries no free text.
- Admin mutates only DrainControl intent and the Reloader. It is reachable only through a non-producer directory and a uid check, and weir-ctl authenticates the server.

## 16. Verification plan and merge gates

DST covers only the WAB flusher (wab/dst.rs:1-4; unbounded drain channel at :904). Drain and lifecycle changes are therefore gated by targeted unit tests, system tests in CI, and a manual chaos campaign before release.

**Unit tests (weir-server bin tests, `--test-threads=1`)**

- **Drain**
  - **Regression gate:** the `mod tests` block of drain/mod.rs is diff-checked as additions-only. Every existing `spawn` / `drain_thread` / `process_segment` / `commit_batch` / `DrainConfig{}` test compiles and passes unchanged through the detached wrappers.
  - New tests:
    1. zombie: an attached drain with every Sender dropped stays alive and delivers;
    2. budget 0 against a 2 s sleeping sink: exit ≤ 100 ms, no `.confirmed`, `segments_abandoned_total` = 1;
    3. 500 ms budget with 5 fast segments: all confirmed;
    4. stop in RetryingTransient with `wait_end > deadline`: immediate exit;
    5. stop while Blocked;
    6. `force_stop` during a yielding commit: Abandoned, no `retried` bump;
    7. **`force_stop` then `request_stop(10 s)`: exit within the grace period** (fetch_min);
    8. pause in Draining: 300 timed sends never time out, 0 commits, resume delivers all in order;
    9. **pause mid-segment** at batch k: the sink never re-sees records ≤ k after resume; `held` is published;
    10. pause during RetryingTransient after the backoff has elapsed: the iteration count over 1 s is ≤ 25 (no spin), and sink health refreshes on cadence;
    11. retry-stranded while the probe says Down: requeues and dedups, returns the ticket;
    12. **1,000 back-to-back requeue requests produce ≤ 1 scan per 5 s window and ≤ 1 backoff cut per episode**;
    13. auto-resume during replay does not double-queue channel items;
    14. alternation: with backlog 300, a sender never blocks for more than 2× segment time;
    15. `initial_backlog` plus a panic on the first segment: everything is delivered;
    16. **CommitGuard: a sink exceeding `commit_timeout`, and one breaking the contract, both leave `commit_in_flight_since == 0`**;
    17. `last_error` is redacted and capped;
    18. a respawn while paused stays paused;
    19. absorb mode skips `probe_health` (a sink whose `health()` sleeps 5 s does not delay exit).
- **WAB**
  - Hand-off tests A and B (§15).
  - `FlusherExit` → registry: the counter-scan give-up ends `Offline(CounterScanFailed)` (DST store-fault hook); panic exhaustion ends `Offline(RespawnExhausted)`.
  - `handoff_blocked_since` set and cleared.
  - Cap hysteresis tests, moved verbatim.
  - Zero-push poll refresh; startup fail-closed.
- **Status**
  - Concurrent writer/reader stress test.
  - The no-Mutex/RwLock source scan, verified by mutation.
  - `pick_live` fairness and None.
  - `ConfigView` compile-guard (a mutation adds a Config field and the test crate fails to build).
- **weir-status**
  - Per-code fire / no-fire / unevaluated tables.
  - Readiness and liveness truth tables.
  - Golden `status_v1_{public,admin}.json`.
  - **A 4.1.0 parser reading a document with an unknown code.**
  - `evidence_from_metrics` against captured 4.0.1 and 4.1 expositions.
  - The exit-code parity table.
- **Router**
  - Existing tests pass.
  - `/metrics`, `/`, `/foo`: head and body format byte-identical to today's `handle()`.
  - Routing table: query strip, `/READYZ` → metrics, HEAD, 405.
  - **Request line and headers in separate writes: the client reads a full response (no RST).**
  - A 1 s head timeout.
  - **Pool isolation: 8 idle connections do not prevent a `/readyz` 200; 64 idle connections are shed within 1 s.**
  - **Sentinel redaction test:**
    - plant a unique sentinel in every String-typed board field (panic payload, `scan_error`, reload detail, `last_error`, `probe_reason`, `paused_by_uid`, the wab_dir path) and in planted bearer/S3 secrets;
    - assert it is absent from `/status`, `/livez?verbose` and `/readyz?verbose`;
    - verified by mutation (route one field into public and watch the test fail).
- **Admin** (in-process UnixStream)
  - Malformed JSON, v=2, unknown verb, a 70 KiB line (rejected without buffering it), a 5th connection.
  - Per-request deadline; guard released before the wait (a `pause wait_ms=30000` does not delay `resume`).
  - The peer predicate; mutating verbs after `begin_shutdown`.
  - **Refusal on a group-writable parent; refusal when a live socket already listens at the path**; `instance` present on every response.
  - In tls_listener.rs: reload with good certificates → applied; corrupt key → failed, and the old certificates still handshake.
- **weir-ctl:** identity checks (wrong uid via the predicate, (dev, ino) mismatch → exit 2); the derived-default path.
- **Config:** defaults and ranges for each new key; the normalized-path collision; under-wab_dir rejection; `livez_checks` parsing.

**System tests** (tests/system.rs via weir-testkit; CI gate for PR 4 and later)

Test hooks:
- A `test-hooks` feature provides `WEIR_TEST_STARTUP_DELAY_MS` and `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN`.
- The hooks are active only when `WEIR_TEST_HOOKS=1` is also set, and log a loud startup WARN when compiled in.
- Hook-dependent tests are `#[cfg(feature = "test-hooks")]`, so the default-features CI line (ci.yml:198) skips them.
- A drift guard asserts the `--all-features` line runs them.

Tests:
1. 300 pre-seeded segments with a 200 ms/POST mock: the producer socket accepts in under 3 s while undelivered is above 250; everything is delivered eventually.
2. Health during recovery (delay hook).
3. SIGTERM with an idle open connection: `/readyz` 503 and the socket gone within 200 ms; the open connection still gets acks; `/metrics` stays up until exit.
4. Bounded shutdown under a blackholed sink **with the drain channel full at SIGTERM**: exit within `shutdown_timeout + budget + 6 s`; a restart delivers every acked record; no stray `.confirmed`.
5. A second SIGTERM.
6. Pause, then 1,000 pushes, then resume.
7. Retry-stranded.
8. Routing avoids an offline shard (hook), and refusal when none is live.
9. `weir-ctl status --json` via admin, http and offline, plus the parity check.
10. Readiness latch-up recovery with zero pushes; a restart over the cap is not-ready from the first `/readyz`.
11. Existing SIGTERM tests with a tight budget.
12. SIGHUP survives (shared with SP-A).
13. **From PR 1 onward: SIGTERM exits within N s** (guards the detached-era sequencing).

**Chaos** (chaos/, beast, real kernel). This is a **manual pre-release gate**, not a per-PR CI gate: it needs root, device-mapper and beast (run.py:822-823), and CI only compiles it (ci.yml:145-180).

In scope as PR 9 (re-sized M-L):
1. **Oracle fix.** The final stop today relies on SIGTERM running a full drain (run.py:418-436). It must set `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS=3600`, or restart and wait for `weir_wab_undelivered_segments == 0` before the `frontier_slack=0` verify.
2. **New fault kinds** `sigterm_at`, `double_sigterm`, `sigterm_in_recovery` and `admin_pause_resume`, with orchestrator unit tests (test_run.py).
3. **Prerequisite:** fix `stack.teardown()` destroying the WAB post-mortem on the crash path (open with the user).

Schedules: `shutdown-bounded`, `shutdown-forced`, `startup-signal`, `pause-resume-cut`.

Gate: ≥ 1,000 cuts in total, ≥ 200 per schedule, 0 oracle violations, the canary firing every episode, and the duplicate ratio reported against the phase-2 baseline.

**Load.** tests/load.rs Durable herd and Buffered on beast, before and after, within ±3%.

**Docs and alerts**
- New rules with promtool tests (docs_drift.rs:598):
  - `WeirConditionCritical`;
  - `WeirNotReady`: `weir_ready == 0 unless on(instance, job) (weir_startup_phase{phase=~"initializing|recovering|handoff|binding|shutting_down"} == 1)` for 5m. Its promtool test includes the inactive phase series present at 0, plus a serving case;
  - `WeirDrainPausedLong` (1h), `WeirOldestUndeliveredAge`, `WeirDrainBackpressure` (on `weir_wab_handoff_blocked_seconds`), `WeirAdminUnavailable`;
  - `WeirSegmentQuarantined` switched to bytes.
- The WeirAckTimeout and WeirQueueSaturation runbooks are rewritten to check hand-off blocking and channel depth first.
- docs_drift guards, each verified by mutation, for:
  - the conditions table and its anchors;
  - the config keys;
  - the `/status` top-level keys;
  - the grace formula in weir.service, compose, configuration.md and the systemd README;
  - **the presence of the HTTP-listener and admin-socket rows and actors in threat-model.md**.
- Run CI locally with `act -W .github/workflows/ci.yml -j <job>`, including the container jobs.
- Security review of redaction and the admin socket before merge.

**PR sequence** (each PR leaves main green and releasable; about 5 engineer-weeks plus the chaos campaign)

| PR | Scope | Size | Behaviour change |
|---|---|---|---|
| 1 | weir-status types and evaluator; StatusBoard (no channel handle), ShardRegistry, WorkerCell, `FlusherExit`; ConfigView; additive metrics (channel depth registered in PR 3); poll tasks extracted; `wab::cap::evaluate`, poll refresh, synchronous first scan before `socket::run`; SP-D logging; SIGTERM-exit system test | M | Cap refresh, startup cap fail-closed, gauge cadence |
| 2 | DrainControl / DrainRuntime and the detached wrappers; backlog and alternation; pause incl. Held; requeue with coalescing; Abandoned and deadline; CommitGuard; absorb skips; publication; `DrainExit`. Wired **detached**, board `Some`, no channel handle | M | None (tests are the gate; additions-only diff) |
| 3 | `wab::recover` / `scan_replay` / `spawn_flushers` split; `hand_off` loop + `BlockingClock::send_timeout` + bounded DST seeds; depth publication; socket bind/run split; bind-all-then-serve; routing; DrainSpawner; `refuse_if_live`; socket/mod.rs:431 comment fix | M | Routing, bind order |
| 4 | Lifecycle, startup restructure, bounded shutdown, lame duck, second signal; flip to **attached**; `drain_shutdown_budget_secs`; grace docs | M-L | **The** behaviour PR; system tests 1-5, 11, 13 gate it in CI |
| 5 | HTTP router (pools, head parsing), readiness, liveness, public type, threat model | S-M | New routes |
| 6 | Admin socket (path, refusal, identity), Reloader (SIGHUP unified with SP-A), verbs | M | New socket |
| 7 | weir-ctl status (identity checks), reload, drain, offline; metrics and segments fixes | M | Additive CLI |
| 8 | Deploy (readiness script, Docker HEALTHCHECK, compose grace, systemd), alerts and promtool, runbooks, docs_drift guards | M | Deploy defaults |
| 9 | Chaos harness: oracle fix, new fault kinds, teardown prerequisite; campaign and write-up | M-L | — (pre-release gate) |

**Dependencies**
- PRs 2 and 3 run in parallel after PR 1.
- PR 4 after 2 and 3.
- **PRs 5 and 6 after 4.** /readyz and /startupz read lifecycle and listener state that only PR 4 writes.
- PR 7 after 5 and 6. PR 8 after all.
- PR 9's harness work can start after PR 4. Its campaign must pass before the 4.1.0 tag.

## 17. Compatibility, docs and release notes

**Release: weir 4.1.0**, a minor release in lockstep across all crates. Mind the seven `version` sites, the three lockfiles and `--locked`.
- Wire protocol, on-disk format, `HealthCheck`, existing metrics and existing `--json` shapes (including `attest`) are unchanged.
- If `weir-status` is approved as a crate, publish it right after weir-core and update RELEASING.md.
- **Downgrade to 4.0.x is safe.** A leftover `wab.admin.sock` is ignored by 4.0.x and is replaced by 4.1's stale-socket handling (not live, so cleaned).
- **Mixed versions.** New weir-ctl against an old daemon falls back to metrics plus disk. Old weir-ctl is unaffected. A 4.1.0 weir-ctl reads newer condition codes as `unknown`.

**Behaviour changes (release-noted)**
1. **Shutdown is bounded.** After the flushers join, the drain gets `drain_shutdown_budget_secs` (default 10 s). The rest replays at the next start. A flusher blocked on a full drain channel gives up its hand-off after 1 s, and the segment replays. Duplicate windows as in §7. To decommission, wait for backlog 0 in `weir-ctl status` first, or raise the budget.
2. **Startup no longer waits on the sink.** The backlog interleaves with new traffic. A restart over `wab_max_bytes` is not-ready from the first probe.
3. **Lame duck.** The socket file is unlinked on SIGTERM, and `/readyz` returns 503 at once.
4. **New admin socket by default** at `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` (e.g. `/var/lib/weir/wab.admin.sock`), mode 0600, with a mandatory uid check. It refuses an insecure parent directory. Disable it with `admin_socket_enabled=false`.
5. **Four reserved HTTP paths.** In addition, a metrics client must send its request head within 1 s, and probes and scrapes have separate connection pools.
6. **weir-readiness.sh** no longer gates on sink health or lifetime counters. Exit 1 now equals `/readyz` 503, and exit 3 needs `--fail-on-critical`.
7. **Deploy defaults.** HEALTHCHECK probes `/livez`; compose gets `stop_grace_period: 45s`; systemd `TimeoutStopSec` goes from 35 to 45; `ExecReload` is coordinated with SP-A.
8. **No more zombie drain.**
9. **Routing.** New connections go only to live shards.
10. **Build.** arc-swap is always compiled. The flusher "drain channel closed" WARN becomes INFO while shutting down.

**Docs**
- architecture.md: startup and shutdown order, and the stale CancellationToken line 76.
- configuration.md: the new keys, the grace formula (replacing ~871-909), the duplicate window, and pause's disk warning.
- monitoring.md: the Conditions section, metrics, runbooks.
- tcp-mtls.md: reload.
- deploy/systemd/README.md, the Dockerfile comments (lines 122-126), compose, and phase4-k8s-deployment.md:125.
- **threat-model.md** (explicit edits):
  1. a new row for the metrics/health/status HTTP listener: unauthenticated, read-only, typed redaction, head and probe pools;
  2. a new row for the admin socket;
  3. new actors: "network peer reaching the metrics/health port" (disclosure and the probe-starvation attempt) and "producer or sidecar sharing the socket volume" (no admin access: the admin socket is not on that volume);
  4. lines 24-30: reconcile "Root is not implicitly trusted" for producers with the admin socket's deliberate root allowance (if the owner keeps it);
  5. the "compromised process running as the daemon uid" out-of-scope note: qualify it for cross-container same-uid producers;
  6. the "Stuck pipeline on shutdown" row: the runtime now outlives the joins, and the drain is bounded;
  7. the "Stale socket files" row: refuse-if-live (with SP-A);
  8. line 132, with SP-A.
- **socket-bind.md and container.md:**
  - an admin-socket section (private parent required, not on shared volumes, the step-7 umask window with a live runtime);
  - container.md:120-125 and the compose comment rewritten for the `/livez` HEALTHCHECK.

## 18. Interfaces reserved for later sub-projects

- **SP-A lock slot**
  - Step 0: `SIG_IGN` for SIGHUP. Step 2: `WabLock::acquire`. The guard outlives every join, and the orphan path exits holding it.
  - `refuse_if_live` and the (dev, ino) record: Foundations owns them for the admin socket; SP-A extends them to the producer socket. Whichever lands first owns the helper.
  - The inode-checked unlink at lame duck and at T3.
  - The Lifecycle SIGHUP arm calls `Reloader::reload(Sighup)`.
  - The shared `weir_config_reloads_total{outcome=noop}`.
- **SP-B**
  - The per-N-records heartbeat inside `process_segment` (CommitGuard already gives truthful commit markers).
  - The "delivered, confirm pending" set publishes into DrainCell.
  - `confirm_retrying` is reserved.
- **SP-G (escalation)**
  - `FlusherExit` / registry reasons, `DrainExit.reason`, `drain.generation`.
  - `Condition.affects` / `severity` as the policy key.
  - A hook to close pinned connections on an offline shard.
  - Counter-scan retry-forever as `Starting → Up` re-entry.
- **SP-H (breaker)**
  - The `backlog` queue, the Held / RetryingTransient holds, and `REQUEUE_MIN_INTERVAL` are the breaker's shape.
  - Invariant: the bounded(256) channel stays the disk bound, and nothing sends without blocking outside shutdown.
- **SP-L (live reload)**
  - `ReloadReport.items` and `not_reloaded` grow within protocol v1.
  - `config_pending_restart` and `tls_cert_expiring` are reserved.
  - The ConfigView exhaustive destructure (§5.14) gains the reloadable / restart-only classification.
- **SP-M (sink hot-swap)**
  - `DrainSpawner`, `DrainExit{pending_left, backlog_left}`, `drain.generation`.
  - **Invariant:** the drain Receiver is never cloned or held elsewhere; a swap stops the old drain and passes its Receiver. Any extra holder would also defeat the `Disconnected` path that Attached mode relies on.
- **SP-N:** `socket::bind -> BoundUnix` is the inherited-fd seam. Phase transitions are the `READY=1` and `STOPPING=1` points.
- **SP-O (k8s)**
  - The probe recipe and timeouts (§12) and NetworkPolicy for 9185.
  - **An explicit `admin_socket_path` on a private emptyDir not mounted into producer containers.** With `readOnlyRootFilesystem`, the derived default under `/var/lib/weir` is not writable and would raise `admin_unavailable`.
  - `log_format=json` if deferred.
  - The optional separate health listener (§20).

## 19. Risks

1. **The drain refactor runs on a thread DST does not cover.** Mitigations: the additions-only test diff, detached wiring until PR 4, the CI system tests, and the pre-release chaos gate.
2. **The hand-off loop is a new branch next to the ack path.** It is correct only because of the seal → send → append → resolve order. Mitigations: one helper with an invariant comment, tests A and B, and deterministic bounded-channel DST seeds.
3. **Operators expect shutdown to deliver everything.** Mitigations: release notes, a runbook entry, the exit log line, and the decommission recipe.
4. **Pause with `wab_max_bytes=0` grows the disk without bound.** Mitigations: the warning, the `drain_paused` condition, `WeirDrainPausedLong`, and in-memory-only pause.
5. **Cancelling an in-flight `sink.commit` at the deadline.** Same mechanism as `commit_timeout`; at-least-once still holds.
6. **Probe starvation on an unauthenticated listener.** This is reduced (1 s head phase, a dedicated probe pool, liveness `failureThreshold` 6) but not eliminated against a determined attacker on the network path. NetworkPolicy or loopback binding remains the boundary.
7. **The admin socket is a privileged entry point.** Defences: a non-producer path, refusal on an insecure parent, a mandatory uid check, client-side server authentication, bounded framing, and no secrets. Residual risk: anything running as the daemon uid with access to the wab_dir parent, which can already destroy the WAB.
8. **Metadata exposure on 0.0.0.0.** Free text is excluded by type, and the sentinel test is a merge gate.
9. **Daemon vs offline evaluation drift.** Mitigated by the single evaluator, the parity test and the exit-code parity table.
10. **Alternation changes post-restart consumption order.** It stays within the per-connection contract.
11. **The settle wait, early TLS load, bind-all ordering and the 1 s head deadline** change when some errors appear. Test fixtures may need updating.
12. **Release overhead** if `weir-status` is a published crate.
13. **The derived admin path depends on the `wab_dir` parent being private.** On custom installs with a group-writable parent, admin is disabled with a condition instead of being exposed.

## 20. Open questions

Owner decisions (with recommendations; see the structured list):
- crate placement;
- admin peer policy (root);
- budget start point and default;
- replay ordering;
- the default liveness set;
- forced-exit status;
- pause through shutdown;
- retry-stranded backoff cut;
- `log_format=json`;
- config-diff before SP-L;
- the probe connection model;
- thresholds;
- ship order;
- TCP failure as not-ready;
- pause granularity;
- whether chaos is a per-PR or a pre-release gate.

Engineering questions, to settle during implementation:
- **(a)** `Respawning` counts as ingest-capable for routing as well as readiness. Proposed yes.
- **(b)** The publish cadence of `pending_len` in holds. Proposed: per slice.
- **(c)** FooterCache bound. Proposed: remove on NotFound, cap at 65,536.
- **(d)** Whether `HEAD_READ_SLOTS = 64` needs to scale with `metrics_max_connections`. Proposed: a constant until measured.
- **(e)** Whether the drain's current-thread runtime `shutdown_timeout(100 ms)` interacts badly with any built-in sink's `spawn_blocking` use (e.g. S3 credential loading). To be checked per sink in PR 2.
