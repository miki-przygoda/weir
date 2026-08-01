# Chaos Phase 1 — Final Review Findings (must land before merge)

**Date:** 2026-07-26
**Branch:** `feat/chaos-fault-injection`
**Reviewed range:** `0b8151b..7bec2b8` (41 commits, ~3000 lines under `chaos/`)
**Verdict:** needs work

Phase 1's nine implementable tasks are complete and all gates are green — `cargo
fmt --check` and `cargo clippy --all-targets -- -D warnings` clean, 30 Rust
tests, 44 Python tests (1 root-only skip), and `cargo metadata` from the repo
root confirms `weir-chaos` is not a workspace member. The Rust↔Python ledger
contract was checked exhaustively and agrees on every case tried.

**But the harness cannot yet distinguish "weir is durable" from "the harness did
not look."** That is precisely the failure Phase 1's exit criterion — *zero false
violations* — exists to prevent, so these land before merge.

---

## C1 — Quiescence returns `True` unconditionally ~1.5 s after restart

`chaos/orchestrator/quiescence.py:109-118`, defaults at `:69`

**The premise of the design is invalid as implemented.** `weir_wab_bytes_on_disk`
is refreshed by a background task **every 5 seconds**
(`crates/weir-server/src/main.rs:563-576`). The stability window is
`stable_polls=3 × poll_interval_s=0.5 = 1.5 s` — **3.3× shorter than the refresh
period**. So "stable across consecutive polls" is satisfied by the gauge being
*stale*, never by drain progress.

The other two signals are trivially satisfied at the same instant:

- `weir_queue_depth` is socket→worker channel occupancy (`main.rs:545-555`),
  empty right after a restart before loadgen reconnects.
- `weir_drain_state{state="draining"}` is the daemon's *initial* state, and its
  own registered HELP text warns: *"state=\"draining\" does NOT imply delivery
  progress — a segment stranded waiting on a fully-down sink still reads
  draining"* (`metrics/mod.rs:543-546`).

Demonstrated with 40 MiB of sealed-awaiting-drain segments held constant:
`quiesced=True after 1.51s and 4 polls`.

Two consequences: verification runs while the pre-crash backlog is still
draining → **I1 false violations**; and the 120 s timeout is unreachable, so *"a
drain that never quiesces is itself a finding"* can never fire.

**Fix:** stability window strictly longer than the gauge's refresh period
(`poll_interval_s=2.0, stable_polls=4`, or `stable_polls>=12` at 0.5 s);
**require at least one observed change** before counting stability, so a
never-refreshed gauge cannot satisfy it; and add `weir_sink_health{state="up"}==1`
plus a stable `weir_drain_segments_stranded`, exactly as weir's own metric docs
instruct.

## C2 — A run where weir refuses or delivers nothing passes 20/20

`chaos/orchestrator/verify.py:213-251`, `chaos/orchestrator/run.py:256-277`

I1 is `acked − delivered_set` and I2 is `nacked ∩ delivered_set`; **both are
vacuously satisfied when `acked` is empty.** `Nack(NackReason::InternalError)` is
*recoverable* (`crates/weir-client/src/unix.rs:108`), so with every shard flusher
offline loadgen keeps its connection and keeps pushing at full rate while every
record is nacked.

Demonstrated with 100 000 nacked records and 0 delivered: `ok=True acked=0
i1=0 i2=0`.

`VerifyResult` has no `nacked_count`, and neither `episodes.jsonl` nor
`report.md` records a *pushed* total, so nothing distinguishes this from a
healthy run. Task 8's guards cover "the observer process died", not "the observer
is alive and the run is meaningless."

**Fix:** add `nacked_count` and `pushed` to `VerifyResult` and the episode
record; fail the episode when the per-episode **delta** in acked or delivered
falls below a schedule-configured floor.

---

## Important

**I1 — `report.py` sums cumulative totals; headline numbers inflated ~N/2×.**
`report.py:50-53, 69-70`. `verify.Accumulator` is cumulative and `run.py:298-313`
writes running totals into `episodes.jsonl`. 20 episodes ending at 20 000 acked
render as `210000` (10.5×). `avg_dup` averages a monotone cumulative series. Use
the last episode's values, or record per-episode deltas.

**I2 — `orphaned_delivered` will be large on every healthy episode, with a wrong
diagnosis.** `verify.py:231`, `report.py:74-79`, `loadgen.rs:181`. The recorder
fsyncs a delivery *before* its 200, ~ms after the ack; the ledger entry sits in
loadgen's per-thread buffer until `pending.len() >= 256` — **there is no
time-based flush**, though the spec says "checkpointed to disk every N seconds"
(design §3.1). With 8 threads that is ~2000 delivered-but-unprovenanced records
at every check, and the report announces them as *"the likeliest cause is a stale
delivery log from an earlier run of this seed"*. Deterministic, not racy: the
harness crying wolf on a clean run, with a diagnosis that sends the operator
hunting a bug that does not exist.

**I3 — I1 has no watermark; whether it fires is a race.** `run.py:287-289`. Load
runs continuously through the kill, the quiescence wait and the 1 s sleep, so
there is always a set of acked-but-undelivered records at check time. The only
thing preventing a false I1 today is incidental ordering. Fixing I2 makes the
ledger more current and a false I1 **more** likely. `LedgerEntry.t_micros` exists
for exactly this and is discarded by `parse_ledger_line`. Fix: carry `t_micros`
and exempt records acked after `quiescence_start − margin` from both I1 and the
orphan set; or SIGSTOP loadgen before waiting.

**I4 — `Nack(InternalError)` is held to I2 but means indeterminate, not
refused.** `loadgen.rs:40`. weir emits it when the flusher returns a non-durable
outcome or its ack sender was dropped (`socket/connection.rs:651-675`) — the
bytes may already be in the segment, so recovery legitimately replays and
delivers them. `weir_wab_fsync_failures`' HELP text states producers in a failed
fsync receive it. Holding those to I2 is stricter than weir's own contract, and
design §6.1 calls any I2 violation "a P0 finding". Barely reachable under
SIGKILL-only; near-certain once Phase 3 adds ENOSPC and read-only remount.
`Unknown`/I3 is the correct classification.

**I5 — "violation" means two different things and the README gate uses both.**
`run.py:270, 291-292` counts durability failure **or** quiescence timeout **or**
observer death; `report.py:19` counts only `not e["ok"]`. Five clean-but-unquiesced
episodes → run.py prints `5 violation(s)` and exits 1 while `report.md` says "0
violations". README:42 gates on both.

**I6 — Recorder never sends `Connection: close`, but weir's sink pools
connections.** `recorder.rs:133, 156` vs `sink/http.rs:158-159`
(`pool_idle_timeout(60s)`, `pool_max_idle_per_host(8)`). Verified live: the second
POST on a reused socket hit EOF and its record never reached the log. Inflates the
duplicate rate (a headline deliverable), flaps `weir_sink_health`, and after
`sink_max_retries=3` strands segments — acked records not delivered, which C1's
broken quiescence then reads as an I1 violation. Three-line fix.

**I7 — Both observers' stderr goes to `/dev/null`.** `run.py:188, 236`. The
recorder's stderr is the only place `refused a request with {code}` appears — and
a recorder 4xx is *permanent* to weir's drain, so a refused batch is dead-lettered,
producing acked-never-delivered records indistinguishable from a weir defect.
loadgen's stderr carries the `FATAL — could not durably record N ledger entries`
message. Send both to files in `run_dir`, as `weir-server.log` already is.

**I8 — "Exactly one privileged component" is documented but not implemented.**
`chaos/README.md:14-16` and design §3 vs `run.py:186, 227`: run.py is root and
spawns both observers as root children with no privilege drop. This is the
branch's central architectural claim.

---

## Minor (triaged)

Must fix (escalated from the deferred roll-up):
- Recorder keep-alive → this is I6, not out of scope.
- `report.py:61` renders "Duplicate rate (mean)" with no unit. Rename to
  "Deliveries per distinct record (1.000 = no redelivery)" — the prose was fixed
  in `7bec2b8`, the table cell was not.

Can stand: recorder sockopt failures swallowed (unreachable, and the log line
survives once I7 lands); recorder temp-dir cleanup on success only; loadgen's
coincident-failure message (mooted by I7); `dm_stack` absolute-path enforcement
(both call sites build absolute paths); `dm_stack.name == ""` at `/` (unreachable,
and `name` is dead); loadgen `--tier Sx` → `'S'` (visible misconfiguration, not a
silent false verdict).

Also worth doing:
- **`--wab-dir` is the mount root, which contains `lost+found`** after
  `mkfs.ext4` (`run.py:154, 209`). Benign as root; becomes a startup failure
  (EACCES) the moment I8 is fixed. Point it at `<mount>/wab`.
- `[faults] kill_random = true` is never read; `run.py:10` refers to branches in
  an `inject` function that does not exist.
- `report.py` is never invoked by `run.py` — one line would remove a manual step
  from the gate.
- No final verification after the last episode, and `finally` SIGTERMs loadgen
  (no handler), so up to 256×threads ledger entries are never written.
- `progress.md` has two "Minor findings roll-up" sections; the empty one shadows
  the real list.
- `chaos/Cargo.toml`'s "No `[[bin]]` sections yet" comment now sits above two
  `[[bin]]` blocks.
- Dead surface: `dm_stack.StorageStack.name`; `verify.check()` (used only by
  tests); `t_micros`/`rtt_micros` and the whole `escape_reason`/`unescape_reason`
  machinery are write-only — nothing reads a NACK reason and the report has no
  nacked figure at all.
- `parse_ledger_line`'s docstring overclaims strict parity with the Rust decoder.
- `test_report.py:46` uses `loadgen_exit_code` while `run.py:274` writes
  `exit_code` — the fixture was not derived from real output.
- `run.py` has ~25 of 358 lines under test. `Daemon.start()`'s argv is a pure
  function of `cfg`; a test asserting each flag appears in
  `crates/weir-server/src/config/cli.rs` would pin the Rust↔Python CLI contract
  without needing root.

---

## What the review confirmed sound

Worth recording so it is not re-litigated: the ledger round trip agrees between
Rust and Python on every case tried (empty-reason NACK including its trailing
space, reasons with spaces, escaped `\n`/`\r`/`\\`, a reason that is literally
`"ACK"`, truncated NACK, ACK/UNK with trailing garbage, `u64::MAX`); the delivery
log contract holds; the observers' logs are genuinely outside the fault zone;
payload encoding is newline-free at every boundary so the NDJSON dead-letter trap
is unrepresentable; all 11 weir-server flags exist and `--sink-http-batch ndjson`
is accepted; the health probe is `HEAD` and the recorder answers it; the socket
unlink before restart is correct; teardown ordering is right; and Phases 2–4
features are genuinely absent.

## Predicted first-Linux-run behaviour

1. **Every episode reports thousands of orphans** (I2), deterministically, with
   the report blaming a stale log. Crying wolf on a clean run.
2. **Quiescence returns True ~2 s after every restart** (C1). On a 512 MiB loop
   with `batch_deadline_ms=2` the backlog is usually small, so this will *mostly
   pass* — the dangerous outcome, because it passes for reasons nobody chose and
   starts failing the moment Phase 2's `dm-delay` slows the drain.
3. **Sporadic sink errors** from the pooled-connection mismatch (I6), first
   visible as an inflated duplicate rate, with the recorder's stderr discarded (I7).
4. **Nothing breaks outright** — `run.py` should get through a run.

What a green first run would **not** have exercised: the quiescence timeout
(unreachable, C1), the observer-death guards, and `dm_stack.setup/teardown` on
real hardware. And it *cannot* fail for the reason it most needs to be able to
fail (C2).

---

# Fix design (decided 2026-07-27)

Verified against the weir source before deciding, since C1's diagnosis depends
on them:

- `weir_wab_bytes_on_disk` is set by a background task on
  `tokio::time::interval(Duration::from_secs(5))` — `main.rs:563-576`. The 5 s
  refresh is real.
- `weir_sink_health{state="healthy"|"degraded"|"down"}` exists
  (`metrics/mod.rs:471`, `SinkHealthState` at `:95`).
- `weir_drain_segments_stranded` and `weir_drain_segments_resumed` exist
  (`:523`, `:533`) as **counters**, so the exposition names carry a `_total`
  suffix.

## C1 — quiescence

The root error was a stability window **shorter than the gauge's refresh
period**, which makes "unchanged" mean "not yet recomputed".

1. **`poll_interval_s = 2.0`, `stable_polls = 4`.** Four stable comparisons at
   2 s span 8 s of wall clock, and a 5 s timer necessarily ticks inside any 8 s
   window — so an unchanged value has survived at least one genuine recompute.
2. **A runtime guard makes the invariant un-tunable-into-brokenness:**
   `wait_for_quiescence` refuses to run when
   `poll_interval_s * stable_polls <= GAUGE_REFRESH_SECS` (5.0). Someone
   "optimising" the poll interval later cannot silently reintroduce this bug.
3. **Two signals added, both named by weir's own HELP text:**
   `weir_sink_health{state="down"} != 1` (a down sink means nothing is draining,
   yet `drain_state` still reads `draining`), and
   `weir_drain_segments_stranded_total` stable across the window.

`drain_state{state="draining"}` is **kept but demoted** — its registered HELP
text says outright that it does not imply delivery progress, so it is a
necessary-not-sufficient signal.

## C2 — vacuous pass

1. `VerifyResult` gains **`nacked_count`** and **`pushed`** (every ledger entry,
   whatever its outcome).
2. The episode record carries **per-episode deltas**, not just cumulative totals.
3. The schedule gains **`min_acked_per_episode`** and
   **`min_delivered_per_episode`**; an episode whose delta falls below either
   fails with `abort_reason = "no_progress"`. A run in which weir refuses or
   delivers nothing can no longer read as twenty clean passes.

## I2 + I3 — the frontier

Two symptoms, one cause: continuous load means there is always in-flight work at
check time. Records acked but not yet ledger-flushed look like **orphans**;
records ledger-flushed but not yet delivered look like **I1 violations**.

1. **Time-based ledger flush** in loadgen (every `LEDGER_FLUSH_INTERVAL` = 200 ms,
   in addition to the 256-record threshold), so ledger staleness is bounded.
   The spec asked for this in §3.1 and it was never implemented.
2. **Frontier exemption, derived rather than magic.** At verification the
   frontier is the ledger's high-water seq; `frontier_slack = threads *
   ledger_flush_threshold` bounds how far a still-buffered thread can lag. Acked
   seqs above `frontier - slack` are exempt from I1, and delivered seqs above it
   are "pending provenance" rather than orphans.
3. **The exempted count is reported.** Hiding it would replace one silent
   distortion with another.

## I4 — `Nack(InternalError)` is indeterminate

Reclassified from `Nacked` to `Unknown`. weir emits it when a flusher returns a
non-durable outcome or its ack sender was dropped, so the bytes may already be in
the segment and recovery may legitimately replay them. Holding those to I2 is
stricter than weir's own contract, and design §6.1 calls an I2 violation a P0
finding. `Unknown`/I3 is exactly the category for "no answer arrived".

## I5 — one word, two meanings

`run.py` counts durability failure, quiescence timeout and observer death all as
"violations"; `report.py` counts only `not ok`. Split into **`violations`**
(durability only) and **`anomalies`** (everything else), reported separately and
both surfaced in the exit line. The README gate becomes "exit 0 and 0 violations
and 0 anomalies".

## I6, I7, I8 and the minors

- **I6:** the recorder sends `Connection: close` on every response. weir's sink
  pools up to 8 idle connections for 60 s and would otherwise reuse a socket the
  recorder has already closed.
- **I7:** both observers' stderr goes to files in `run_dir`, as the daemon's
  already does.
- **I8:** deferred, and the README/spec claim corrected to match reality — a real
  privilege drop needs `setuid` plumbing that is its own piece of work. The
  claim, not the code, was wrong.
- `--wab-dir` becomes `<mount>/wab` so `lost+found` is not inside it.
- The duplicate-rate table cell becomes "Deliveries per distinct record
  (1.000 = no redelivery)".
- `report.py` totals come from the last episode, not a sum of a cumulative
  series.
- `run.py` invokes the report renderer at the end of a run.
- Dead surface removed: `dm_stack.StorageStack.name`, the stale `[[bin]]` comment
  in `chaos/Cargo.toml`, the unread `[faults] kill_random` key and the `inject`
  reference in run.py's docstring.

---

# C1 — second correction (2026-07-27)

The fix above landed (`poll_interval_s=2.0`, `stable_polls=4`, a
`GAUGE_REFRESH_SECS` guard) and made the immediate symptom — quiescing ~1.5s
after every restart — go away. A second review round found it broke in the
**opposite** direction instead of actually working: run.py never pauses the
load generator, and `weir_wab_bytes_on_disk` (`compute_wab_bytes_on_disk`,
`main.rs:58-91`) counts the open, still-growing active segment plus sealed
segments awaiting drain. Under continuous load that total changes on every
genuine 5s recompute, so byte-exact stability across consecutive polls never
occurs at all. Simulated across four load rates and the full poll/refresh
phase space: **0 of 800 episodes quiesced.** Every episode would time out at
120s, so the gate was unreachable by construction — the inverse of the
original bug, but equally fatal to Phase 1's exit criterion.

**The bytes gauge was abandoned entirely, not re-tuned a second time.** It
conflates two things that need to be judged separately: how much is
*buffered* (workload-dependent, and irrelevant to "has the drain caught up")
and whether *sealed work* has reached a terminal state (exactly what
matters). No stability window, however chosen, can separate those two once
they're combined into one number.

**Replacement: `weir_wab_segments_total`**, a Counter family with states
`open`/`sealed`/`confirmed`/`quarantined`, incremented at the actual
transition sites (`wab/mod.rs:603,666,769` on seal, `drain/confirmed.rs:55`
on confirm, `wab/recovery.rs` on quarantine) — transition-driven, not
timer-refreshed, so it carries no staleness trap in either direction.
Quiescence now requires, for `stable_polls` consecutive polls:

1. `sealed_total == confirmed_total + quarantined_total` — every sealed
   segment has reached a terminal state (the real "drain caught up" test).
2. `stranded_total == resumed_total` — no segment is still stranded.
   **Equality, not stability**: the previous fix's stranded check tested for
   an *unchanging* counter, which catches a counter that is rising but not
   one that has *already* risen — an already-stranded segment satisfied it
   forever while acked-undelivered records sat on disk. weir's own HELP text
   for `weir_drain_segments_resumed` states the correct test directly:
   convergence with `stranded` means the backlog was picked back up.
3. `weir_queue_depth == 0`.
4. `weir_drain_state{state="draining"} == 1` — kept, still
   necessary-not-sufficient.
5. `weir_sink_health{state="down"} != 1`.

Because every signal here is snapshot-based (no delta against the previous
poll, unlike the bytes gauge), `stable_polls` consecutive passes is now a
guard against a single flicker, not a stability comparison — and because
none of the five is timer-refreshed, `GAUGE_REFRESH_SECS` and its
`ValueError` guard were deleted rather than re-tuned; a comment in
`quiescence.py` explains why no such guard is needed and what to do if a
future timer-refreshed signal is ever added here. Defaults dropped to
`poll_interval_s=0.5, stable_polls=3` (a 1.5s window) now that there is no
refresh period to outrun. Missing-key defaults differ by metric type: the
four counters default to `0.0` (prometheus-client only emits a `Family`
member once incremented, so absent genuinely means zero), while the gauges
keep their conservative blocking defaults (`queue_depth` absent → `1.0`,
`draining` absent → `0.0`, `sink_down` absent → `1.0`).

Proven both ways against the real new defaults (see
`fix-c1-round2-report.md` for full output): a scrape simulating continuous
load with `sealed_total` rising and `confirmed_total` permanently behind by a
fixed backlog does not quiesce within a 5s budget; the same shape of load
with `confirmed + quarantined` caught up to `sealed` (while `stranded`/
`resumed` keep changing together, as they would under real load) quiesces in
1.01s — three polls at the new 0.5s interval — nowhere near a realistic 120s
timeout.

Also fixed in the same pass: a failed scrape now resets the stability
counter (previously a window could straddle an observability gap and stitch
polls from either side of it into false consecutivity).

---

# First real run (2026-08-01, privileged Linux container) + two investigations

Phase 1 needs only `losetup`/`mkfs.ext4`/`mount` — **no device-mapper at all**
— so the gate does not need the dedicated box. It ran in a privileged container
(LinuxKit 6.12.54). `chaos/Dockerfile.gate` reproduces it.

## What the run found immediately

- **`load_schedule` resolved relative paths against the orchestrator directory**,
  so the invocation the README documents died before doing any work. The unit
  test missed it by passing a path that resolves correctly under *both* rules —
  it pinned the bug rather than catching it. Fixed, with a test that runs from
  the documented cwd and genuinely fails when reverted.
- **Teardown killed the recorder before the daemon**, so weir spent its whole
  shutdown draining into a dead sink: 32 transport errors, `sink health: down`,
  4 stranded segments — all in 24.6 ms, all pure noise that reads like a real
  outage. Order is now loadgen → daemon → recorder → stack.

## My diagnosis of the quiescence timeouts was WRONG

I concluded "the drain can never catch up under sustained load" and applied
`SIGSTOP` to the producer. The investigation disproved the premise: **the drain
was never behind.** `weir-server.log` shows exactly `shard_count` segments
queued for replay per restart and **zero** pre-existing unconfirmed sealed
segments.

The 78–87 k gap is the contents of the four **open active segments**. Modal
delivery run is 31,775 records (8 MiB / 264 B); × 4 shards = up to **127,100
acked records** that weir has no reason to seal, because idle-seal is off. The
density profile across the gap is a textbook four-shard staircase
(0.505 → 0.750 → 0.873 → 1.000), not drain lag.

### And SIGSTOP made things worse in a way the run concealed

**On a freshly restarted daemon every quiescence condition is trivially
satisfied because nothing has happened yet** — the counter families are absent
(correctly read as 0), `drain_state{draining}` is pre-initialised to 1.0 and
`sink_health{down}` to 0.0. The only condition that blocked a false pass was
`queue_depth > 0` under load — **which is exactly what SIGSTOP removes.**
Demonstrated against the real module: `quiesced=True after 1.02s` with four
replay segments queued and undelivered.

The corollary is the uncomfortable part: **the SIGSTOP run only produced
"0 violations" because quiescence kept timing out.** The broken predicate was
accidentally doing the job the predicate was meant to do, and correctness hinged
on losing a race. A green result there was luck, not evidence.

## Root cause 2 — a genuine weir finding, the harness's first

`recover_segment` (`crates/weir-server/src/wab/recovery.rs:497-503`) seals a
recovered segment — rename + fsync + `info!("recovery sealed segment")` — and
**never increments `weir_wab_segments{state="sealed"}`**. The drain's confirm
*does* increment `confirmed` (`drain/confirmed.rs:51-58`). So after every
restart `confirmed = sealed + shard_count` permanently, and any predicate of the
form `sealed == confirmed + quarantined` is unsatisfiable at rest.

That is a real defect in weir's metric contract — the counter family is
non-conserving across a restart — not merely inconvenient for the harness.
Observed live as `sealed(0) != confirmed(4)` on every episode.

## Root cause 3 — idle-seal was disabled

`wab_segment_max_age_secs` defaults to `0` (disabled). A frozen producer
produces no seal, so no drain, so no convergence. Note the branch *requires* a
pause — it only fires on an empty batch cycle — so pause and idle-seal are
complementary and neither alone does anything. `run.py` now passes
`--wab-segment-max-age-secs 2` (`deploy/systemd/weir.toml` ships `5`, so this is
a supported posture, not a test hack).

## The fix that follows: stop inferring, measure

`run.py` is root and owns the mount. Ground truth is a `glob`, not a derived
identity over cross-process counters: **zero `*.wab.sealed` lacking a
`.confirmed` sidecar, and zero non-empty active `*.wab`**, stable across polls.
That is immune to counter resets, to the recovery-seal asymmetry, and to Phase
2/3 conditions where counters may lie — and it kills the ~1 s false positive for
free, because at T+50 ms the replay segments are physically on disk. The metric
conditions stay as necessary-but-not-sufficient companions.

## Also established, worth not re-deriving

- **Recorder capacity is not the problem.** 1,782,145 delivered in 408.9 s =
  4,358 rec/s = 43.6 POST/s at 23 ms/POST *including* TCP connect and fsync, and
  that figure is supply-limited so it is a floor. Draining 127 k records takes
  ~29 s, comfortably inside a 120 s budget.
- **weir's SIGTERM does a FULL drain**, not a seal-and-exit, and
  `shutdown_timeout_secs` does not bound it (that covers only the socket
  layer). The old 30 s budget was never binding because the recorder died first
  and every sink call failed instantly. Raised to 300 s and a kill is now
  reported.
- **The teardown fix produces no verified evidence on its own.** weir will now
  deliver the final ~82 k records, and nothing reads them before
  `stack.teardown()` deletes the filesystem. A final verification pass after a
  clean shutdown is the single highest-value addition available — it is the only
  moment in a run where `frontier_slack=0` is a *true* statement.
- **SIGTERM on a handler-less loadgen** discards up to `threads × 256` buffered
  ledger entries. Safe direction (under-checking, not false accusation) but
  concealing; `loadgen.log` was 0 bytes because it died before printing.
- **RTT contamination**: a push straddling a freeze yields a ~120 s sample.
  Measured against the real 1.86 M-sample ledger: p50/p99/p99.9 survive, but the
  mean nearly doubles and max goes 1.98 s → 120 s. The *kill* already
  contaminates the stream (24 samples > 1 s). Fix is not SIGSTOP-specific —
  record each episode's excluded wall-clock windows and drop straddling samples
  in analysis.
- **Slowloris**: `connection_read_timeout_secs` defaults to 30 s, so a 120 s
  freeze drops all 8 connections. loadgen's poison/reconnect handles it
  correctly; cost is 8 spurious `Unknown` entries per episode.
- weir shutdown **skips the retry backoff entirely** (`drain/mod.rs:511-524`):
  `Disconnected` breaks the wait, so the 100/200/400 ms ladder collapses to
  zero. All four segments burned all four attempts in 0.4 ms. A merely-slow sink
  gets no retry at shutdown.
