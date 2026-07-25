# Real-Kernel Fault Injection & Chaos Soak — Design

**Date:** 2026-07-25
**Status:** design approved, not implemented
**Target platform:** Linux (x86_64), dedicated bare-metal host

---

## 1. Goal

weir's README makes a durability claim in the imperative:

> **An ack is never a false ack:** an acked record is on disk and replays after a crash.

and a tier claim:

> `Buffered` acks *before* fsync, so it survives a process crash but **not** power loss.

Today both are supported by *simulated* faults. The DST harness
(`crates/weir-server/src/wab/dst.rs`) injects `fdatasync`-`EIO`, torn writes,
failed renames, `ENOSPC`-at-seal and mid-flush panics by **swapping out the
segment backend** — it never touches a real block device or a real syscall
boundary. That is a genuine gap: DST proves the daemon's logic is correct given
a model of the kernel, not that the model matches the kernel.

This suite closes that gap. It runs weir against a **real filesystem on a real
device-mapper stack**, injects faults the kernel actually implements, and
verifies the durability claim from **outside** the daemon across crash and
restart.

**Success criterion:** a skeptical reader can read the numbers and re-run them.
The deliverable is a committed harness plus a generated report, not a one-off
study.

### 1.1 Explicit non-goals

- **Not a replacement for DST.** DST is fast, deterministic and runs on every
  push. This runs for days on one machine. They cross-validate; neither
  subsumes the other.
- **Not a benchmark.** Absolute throughput numbers are not the point and are
  not published from this harness — `tests/load.rs` and
  `docs/benchmarks/` own that. This measures *latency stability over time* and
  *behaviour under fault*.
- **Not portable.** Linux-only, root-only, single-host. macOS is explicitly out
  of scope: `F_BARRIERFSYNC` gives no power-loss guarantee, so a durability
  proof there would prove nothing.
- **Not a CI gate** in this iteration. The harness is committed and runnable;
  wiring it to a scheduled self-hosted runner is deferred.

---

## 2. Placement — in-repo, separate cargo project

The harness lives at `chaos/` in the weir repo, as a **standalone cargo project
with its own `[workspace]` and lockfile**, `publish = false`, depending on
`weir-client` and `weir-wab` by path.

This mirrors `fuzz/` exactly, and for the same stated reason: `fuzz/Cargo.toml`
isolates itself so that "the fuzz crate doesn't pull `weir-server` into a
nightly toolchain when stable users invoke `cargo build` from the repo root."
`chaos/` has the identical property with different specifics — Linux-only,
root-requiring, device-mapper-dependent tooling that must not enter the
workspace build, the MSRV matrix, or the publish flow.

**Why not a separate repository.** Two reasons, one of them decisive:

1. **Version drift is fatal to a durability proof.** Path dependencies mean the
   harness always tests the working tree and cannot misreport what it verified.
   A separate repo would depend on weir via git or crates.io, and the failure
   mode is a report asserting "durability verified" against a weir several
   releases stale. That is worse than publishing no report.
2. **Evidence belongs next to the claim.** A reader who follows the README's
   durability assertion should be one link from the run that establishes it.

A separate repo would only win if this became a general-purpose chaos harness
for arbitrary durable-write daemons. That is speculative; extracting it later is
cheap if it happens.

---

## 3. Architecture

Four harness components alongside the daemon, exactly one of them privileged,
and one deliberate rule: **the observers are unprivileged and live outside the
fault zone.**

```
                    ┌──────────────────────────────────────┐
                    │  orchestrator  (root, Python)        │
                    │  · owns dm stack + mounts            │
                    │  · owns fault schedule (seeded)      │
                    │  · owns eBPF probe lifecycle         │
                    │  · drives episodes, calls verifier   │
                    └───────┬──────────────────────┬───────┘
                            │ spawns/kills         │ injects
                            ▼                      ▼
   ┌───────────────┐   ┌──────────┐        ┌──────────────────┐
   │ loadgen       │──▶│ weir     │───────▶│  dm stack        │
   │ (Rust, unpriv)│   │ -server  │  fsync │  loop→delay→     │
   │ · producer    │◀──│          │        │  flakey→ext4     │
   │   pool        │ack└────┬─────┘        └──────────────────┘
   │ · ledger      │        │ HTTP sink (NDJSON batch)
   │ · latency     │        ▼
   └───────┬───────┘   ┌──────────────┐
           │           │ recorder     │  ← writes to host fs,
           │           │ (Rust,unpriv)│    NOT the dm stack
           │           └──────┬───────┘
           │ ledger           │ delivered-id log
           ▼                  ▼
        ┌────────────────────────────┐
        │ verifier + report (Python) │
        └────────────────────────────┘
```

### 3.1 `loadgen` — Rust binary, unprivileged

Sustained producer pool over `weir-client`. Per record it assigns a unique
identity and records the outcome in an in-memory **ledger**.

- **Record identity.** The payload carries a 16-byte prefix:
  `[run_id: u64 LE][seq: u64 LE]`, followed by filler to the configured record
  size. `run_id` is derived from the schedule seed, so records from a previous
  run can never be mistaken for the current one.
- **Ledger outcomes** — exactly three, and the third is not a failure:
  - `Acked` — weir returned `Ack`. **This is a promise and the suite holds weir
    to it.**
  - `Nacked(reason)` — weir returned `Nack`. weir explicitly refused.
  - `Unknown` — pushed, but the connection died or timed out before any
    response. The outcome is *legitimately* indeterminate.
- **Why in-memory is sound.** The suite kills the *daemon*, never the harness.
  The ledger therefore models exactly what the producer *was told*, which is
  precisely the thing the durability claim is about. It is checkpointed to disk
  (outside the fault zone) every N seconds so a multi-day run survives an
  operator mistake or an OOM.
- **Latency stream.** Every push's round-trip is sampled and appended to a
  time-series log with a timestamp and tier. This is the soak half of the
  deliverable — p50/p99/p99.9 over hours.

### 3.2 `recorder` — Rust binary, unprivileged

The recording sink. weir is configured with `--sink-type http` pointed at it,
using **NDJSON batch mode** (`sink_http_batch = "ndjson"`) so one POST carries a
whole batch.

It extracts `(run_id, seq)` from each record and appends to a durable log.

**It must fsync before returning 200.** This is not incidental — it is the
`Sink` contract. weir treats a successful response as "committed downstream" and
becomes free to reclaim the segment. A recorder that buffered in memory and lost
records on its own crash would manufacture false durability violations. The
recorder is the oracle; it must be at least as durable as the thing it is
judging.

Its log lives on the **host filesystem, deliberately outside the dm stack**, so
no injected fault can corrupt the observer.

### 3.3 `orchestrator` — Python, root

Owns everything privileged, and is the only component that is. Builds and tears
down the device-mapper stack, applies faults, manages the eBPF probe, drives the
episode loop, and invokes the verifier.

Python matches existing repo precedent for tooling: `deploy/avg_benchmarks.py`,
`deploy/grafana/gen-dashboards.py`, `docs/conformance/gen_vectors.py`.

### 3.4 `verifier` + `report` — Python

Verifier runs the invariants after each episode. Report renders the accumulated
episode log into markdown for `docs/`.

---

## 4. The storage stack

The WAB directory sits on a nested device-mapper stack over a sparse file:

```
sparse file (host fs)
 └─ losetup ────────→ /dev/loopN
     └─ dm-delay ──────→ slow disk        (configurable read/write latency)
         └─ dm-flakey ───→ power loss     (drop_writes)
                          torn writes     (corrupt_bio_byte)
             └─ ext4, mounted at <wab_dir>
```

A **second, smaller stack** is mounted at `<wab_dir>/dead_letter` so dead-letter
exhaustion can be driven independently of WAB exhaustion.

> **Verified:** `create_dir_private` (`wab/mod.rs:207-220`) uses
> `DirBuilder::recursive(true)`, which tolerates a pre-existing directory — so a
> mount point at that path is safe. Caveat: `.mode(0o700)` applies only to
> directories the builder actually creates, so the orchestrator must `chmod
> 0700` the mounted volume root itself.

### 4.1 Sizing

To provoke real `ENOSPC` *and* real segment rotation inside a small volume,
`wab_segment_max_bytes` is lowered to **8–16 MiB** against a **~128 MiB**
volume.

> **Verified:** the config range is `[4096, 4 GiB]`
> (`config/mod.rs:509`), default 256 MiB. 8–16 MiB is comfortably valid.

### 4.2 Fault catalogue

Seven classes. Each has a **defined expected behaviour** — the suite asserts
weir does the right thing, not merely that it survives.

| # | Fault | Mechanism | Expected weir behaviour |
|---|-------|-----------|-------------------------|
| F1 | `kill -9` mid-fsync | eBPF probe (§5) + random killer | Process dies. On restart, every `Acked` record replays and reaches the sink. |
| F2 | Power loss | `dm-flakey drop_writes` | Non-fsync'd writes vanish. Sync/Batched lose nothing; Buffered **may** lose — quantified. |
| F3 | Disk full (WAB) | small volume + sustained load | Nack, not death, not silent loss. Nacked records must never appear downstream. |
| F4 | Slow disk | `dm-delay` (escalating) | Stays up; latency degrades gracefully; backpressure via queue depth, no loss. |
| F5 | Torn / partial writes | `dm-flakey corrupt_bio_byte` | Recovery quarantines or truncates at the corrupt record; no `Acked` record lost. |
| F6 | Read-only remount | `mount -o remount,ro` | Fails **closed** — nacks rather than accepting records it cannot persist. |
| F7 | Dead-letter exhaustion | second volume filled | Drain enters `BlockedDeadLetterFull`; no loss; recovers when space is freed. |

F5 is the cross-validation case: DST already simulates torn writes at the
segment-backend seam. F5 does it at the **block layer**, which tests whether
DST's model of a torn write matches what the kernel actually delivers.

---

## 5. The kill probe

`bpftrace` attaches a kprobe to `vfs_fsync_range`, filters to the daemon PID,
and calls `signal(9)` **on entry** — killing the process *inside* the fsync
call, before it returns.

```
kprobe:vfs_fsync_range /pid == $target/ { signal(9); }
```

Requires kernel ≥ 5.3 for the `send_signal` BPF helper. The probe script is
itself part of the published evidence: it is short enough to read and verify.

A **random killer** runs as a separate mode — `SIGKILL` at seeded random offsets
under sustained Sync-tier load — to accumulate crash-restart cycles in volume.

**The two are tagged distinctly in the report.** A targeted mid-fsync kill and a
lucky random one are different evidence and must never be conflated.

---

## 6. The oracle

The part that determines whether any of this is credible.

After each episode, the verifier reads the loadgen ledger and the recorder log
and checks three invariants.

### I1 — Every `Acked` record was delivered

```
{ id : ledger[id] == Acked }  ⊆  { id : id ∈ recorder_log }
```

Set **containment**, not equality. At-least-once delivery means duplicates are
conformant.

The duplicate rate is **measured and reported** — `|recorder_log| /
|distinct(recorder_log)|` per episode. The README tells integrators their sink
must dedupe but never says how much redelivery a crash actually costs. This
answers that for free.

### I2 — No `Nacked` record was ever delivered

```
{ id : ledger[id] == Nacked }  ∩  { id : id ∈ recorder_log }  =  ∅
```

The inverse invariant, and a real bug class: a record weir *refused* must not
silently appear downstream.

### I3 — `Unknown` records are unconstrained but counted

Either outcome conforms. They are counted and reported rather than quietly
reclassified — an oracle that hides its awkward cases is not an oracle.

### 6.1 I1 is tier- and fault-aware

This asymmetry is the point of the whole suite. It converts the docs' durability
table from an assertion into a measurement.

| | `kill -9` (process crash) | `dm-flakey drop_writes` (power loss) |
|---|---|---|
| **Sync / Batched** | zero loss required | zero loss required |
| **Buffered** | zero loss required | **loss permitted — and quantified** |

The bottom-right cell is the headline result. Two outcomes are interesting:

- Buffered loses records under `drop_writes` → the documented tier contract is
  confirmed as a measured number.
- Buffered loses **nothing** → either the fault injection is not biting (a
  harness bug, must be ruled out before publishing) or the implementation is
  stronger than the docs claim. Both are worth knowing.

**Any violation of I1 in the top row, or of I2 anywhere, is a P0 finding.**

### 6.2 Drain quiescence

Verification must not run before weir has finished draining, or it reports false
violations. Quiescence is three signals from the existing `/metrics` endpoint —
no new instrumentation required:

1. `weir_wab_bytes_on_disk` stable at active-segment-only. Its registered HELP
   text states it counts the active segment plus sealed-awaiting-drain and
   *excludes* `.confirmed`, `dead_letter/` and `quarantine/`
   (`metrics/mod.rs:426-427`) — so it falls to the active segment precisely when
   drain has caught up.
2. `weir_queue_depth` at zero.
3. `weir_drain_state{state="draining"}` = 1 (not blocked, not retrying).

A bounded timeout applies. **Timeout reports "stuck" as a finding** rather than
hanging the run — a drain that never quiesces is itself a defect.

---

## 7. Seeds and reproducibility

Every schedule derives from a single seed: fault ordering, timing jitter, random
kill offsets, record sizes, tier mix. Any invariant violation prints a one-line
reproducer naming the seed and episode index.

This mirrors the discipline DST already uses (`WEIR_DST_SEED=0x… cargo test …
--features dst`) for the same reason: **a violation found at 3am on day two is
worthless if you cannot return to it.** Reproducers that fire are pinned into
`chaos/schedules/pinned/` and re-run at the start of every subsequent run, so a
fixed bug stays fixed — the same contract as `tests/dst_seeds/*.json`.

Full determinism is **not** claimed. Real kernels, real timing, real I/O. The
seed reproduces the *schedule*, not the exact interleaving. This is stated in
the report rather than papered over.

---

## 8. What is sampled

Continuously, throughout the multi-day run, to a time-series log:

| Signal | Source | Answers |
|--------|--------|---------|
| Push latency by tier | loadgen | p50/p99/p99.9 drift over hours |
| RSS | `/proc/<pid>/status` | Memory growth |
| Open fd count | `/proc/<pid>/fd` | fd leaks |
| WAB bytes / segment counts | `/metrics` | Segment accumulation |
| Dead-letter bytes | `/metrics` | Dead-letter growth |
| Queue depth | `/metrics` | Backpressure behaviour |
| Drain state | `/metrics` | Time spent blocked/retrying |

This is where the second deliverable comes from. The pre-publication sweep rated
the connection-handler `JoinSet` a **medium-severity unbounded leak**, noting it
requires "sustained high churn over a long uptime" to manifest — conditions no
existing test creates. A multi-day run with connection churn either shows the
RSS curve bending or it does not. Either result is worth having.

---

## 9. The report

Generated into `docs/` and rendered by the existing mdbook site. Contents:

1. **Run metadata** — weir commit, kernel version, hardware, filesystem, schedule
   seed, wall-clock duration, total records.
2. **The tier × fault matrix** (§6.1) — the headline.
3. **Per-fault-class tables** — episodes, violations, acked / delivered /
   duplicate counts.
4. **Latency over time** — p50/p99/p99.9, annotated with fault episodes so a
   spike can be attributed.
5. **Resource curves** — RSS, fds, segments, dead-letter bytes.
6. **Every violation, with its reproducer.**
7. **Honest limitations** — what was not tested, what the seed does and does not
   reproduce, tenancy and hardware caveats.

Section 7 is not optional. A report that only lists successes is marketing, and
a reader who spots an unstated limitation discounts everything else in it.

---

## 10. Implementation phases

Spine first. The genuine unknowns are the oracle and quiescence detection, and
they are cheaper to get wrong with one fault implemented than with seven.

**Phase 1 — spine.** `chaos/` scaffolding; loadgen with ledger and latency
stream; recorder with durable append; dm stack setup/teardown (no faults yet,
just the plumbing); episode loop; quiescence detection; I1/I2/I3 verifier; F1
random-kill only; minimal report.
*Exit criterion: a 30-minute run with random kills produces a clean report and
zero false violations.* The false-violation check is the real gate — a harness
that cries wolf is useless for a multi-day run.

**Phase 2 — the headline.** eBPF probe for targeted mid-fsync kills (F1 full);
`dm-flakey drop_writes` (F2); tier-aware I1; the tier × fault matrix.
*Exit criterion: the matrix is populated with measured numbers.*

**Phase 3 — the rest of the taxonomy.** F3 disk full, F4 slow disk, F5 torn
writes, F6 read-only remount, F7 dead-letter exhaustion.

**Phase 4 — the long run.** Multi-day schedule, resource sampling, latency
plots, pinned-reproducer replay, full report.

---

## 11. Risks

| Risk | Handling |
|------|----------|
| Harness produces false violations | Phase 1 exit criterion is explicitly a clean run. Recorder fsyncs before 200. Ledger checkpoints outside the fault zone. |
| `dm-flakey drop_writes` semantics differ from expectation | Validate against a deliberately-non-durable control program before trusting any weir result. |
| eBPF `signal()` unavailable or blocked | Falls back to random killer; targeted results are simply absent rather than fabricated. Report states which mode produced each number. |
| Multi-day run dies mid-way | Ledger and episode log are append-only and checkpointed; a partial run yields a partial report. |
| Nested dm targets interact unexpectedly | Each target is validated in isolation before stacking. |
| Harness rots as weir changes | Path deps mean it breaks loudly at compile time rather than silently testing the wrong thing. |

---

## 12. Open questions

1. **Tier mix.** Should a single run mix all three durability tiers
   concurrently (realistic, and tests that tiers do not interfere), or run them
   in separate phases (cleaner attribution)? *Leaning: mixed, with tier recorded
   per record so attribution is preserved anyway.*
2. **Connection churn rate.** The `JoinSet` question needs sustained churn, but
   churn also changes the latency profile. May warrant a dedicated
   high-churn phase rather than being folded into the general load.
3. **Recorder throughput ceiling.** An fsync-per-batch recorder may become the
   bottleneck before weir does. Needs measuring in Phase 1; if so, batch its
   fsyncs while preserving the durable-before-200 contract.
