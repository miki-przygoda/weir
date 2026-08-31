# 2.0.1 Silent-Failure Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every path found by the 2026-08-31 audit where weir loses acknowledged data or stops delivering **without saying so**, and make the polyglot conformance clients unable to rot again.

**Architecture:** Seven independent fixes across recovery, the drain supervisor, the segment writer, and CI. No API, wire-format, or config-default changes — 2.0.1 is a patch. Each fix is small and local; they share no code, so tasks can be reviewed and reverted independently.

**Tech Stack:** Rust (edition 2024, MSRV 1.88), `tokio`, `crossbeam-channel`, `prometheus-client`; GitHub Actions; Python/Go/C/Java/TypeScript for the conformance demos.

**Spec:** `docs/superpowers/specs/2026-08-31-silent-failure-fixes.md`

## Global Constraints

- **MSRV is 1.88.** `rustup run 1.88 cargo check --workspace --all-features --locked` must pass.
- **No public API change, no wire change, no config-default change.** This is a patch release.
- **The failing test IS the verification.** Every finding came from an agent. If a task's test does not fail *before* the fix, the finding is refuted — **stop, delete the task, and record the refutation in the spec's "Refuted or downgraded" section.** Do not implement a fix for a bug you could not reproduce.
- **`weir-server`'s bin unit tests must run serially:** `cargo test -p weir-server --bins -- --test-threads=1`. `socket::bind_hardened` mutates the process-global umask around `bind(2)`; parallel runs produce ~66 spurious `PermissionDenied` failures.
- **The full gate before any PR** is in `CONTRIBUTING.md` — fmt, three clippy matrices, six test commands, `cargo deny`, MSRV, dst, load-compile, `mdbook build`, and the demo-version drift check.
- Commit messages: no `Co-Authored-By` or `Claude-Session` trailers.

---

### Task 1: Recovery must quarantine a mid-file sentinel, not truncate past it

**Files:**
- Modify: `crates/weir-server/src/wab/recovery.rs:321-323`
- Test: `crates/weir-server/src/wab/recovery.rs` (in-file `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SEGMENT_FOOTER_LEN` (32) from `weir_wab::format`; the existing `quarantine_reason: Option<String>` local declared at `recovery.rs:~299`.
- Produces: nothing other tasks depend on.

The `payload_len == 0` branch breaks with `quarantine_reason` left `None`, so `file.set_len(valid_end_offset)` (`:512`) destroys the tail and `:515` writes a footer matching the truncated prefix — making the loss invisible to the drain's cross-check. The oversized-`payload_len` branch (`:355-372`) already does the right check; this is the missing third case.

A genuine partial seal leaves **at most** `4 + SEGMENT_FOOTER_LEN` bytes after the sentinel position. Anything longer is corruption.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn mid_file_sentinel_with_a_live_tail_is_quarantined_not_truncated() {
    // A zero length-prefix mid-file is ALWAYS corruption, never a partial
    // seal: `seal()` consumes the handle (segment.rs:383) and write_record
    // rejects empty payloads at two layers (segment.rs:117, :546). So a
    // sentinel with real records behind it means lost writeback or bit-rot,
    // and truncating it silently destroys acked data.
    let dir = tempdir_for("sentinel_live_tail");
    let path = make_segment(&dir, 0, &[b"A-acked", b"B-acked", b"C-acked"]);

    // Zero B's 4-byte length prefix in place, leaving C intact behind it.
    zero_length_prefix_of_record(&path, 1);

    let metrics = test_metrics();
    let before = metrics.recovery_segments_quarantined.get();
    let len_before = std::fs::metadata(&path).unwrap().len();

    let _ = recover_segment(&path, &dir, &metrics);

    assert_eq!(
        metrics.recovery_segments_quarantined.get(),
        before + 1,
        "a mid-file sentinel with a live tail must quarantine, not truncate"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        len_before,
        "the original bytes must be preserved for forensics"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins mid_file_sentinel_with_a_live_tail -- --test-threads=1`
Expected: FAIL — quarantine counter unchanged (0 vs 1) and the file shorter than before.
**If it passes, the finding is refuted. Stop and record that.**

- [ ] **Step 3: Add the trailing-bytes guard**

Replace `recovery.rs:321-323` with:

```rust
        if payload_len == 0 {
            // A genuine partial seal leaves at most the sentinel plus a footer.
            // Anything longer means this zero is corruption — lost writeback or
            // bit-rot — sitting in front of records that were acked. Truncating
            // there destroys them AND rewrites the footer to match, so the
            // drain's record-count cross-check cannot see the loss. Both
            // neighbouring branches (oversized length :355, CRC mismatch :405)
            // already quarantine; this was the missing third case.
            let field_start = valid_end_offset;
            let file_len = match std::fs::metadata(path) {
                Ok(m) => m.len(),
                // Take the conservative branch when we cannot tell.
                Err(_) => u64::MAX,
            };
            if file_len > field_start + 4 + SEGMENT_FOOTER_LEN as u64 {
                quarantine_reason = Some(format!(
                    "zero length prefix at offset {field_start} with {} bytes of \
                     tail behind it — a partial seal leaves at most {}",
                    file_len.saturating_sub(field_start),
                    4 + SEGMENT_FOOTER_LEN
                ));
            } else {
                info!(path = %path.display(), records = record_count,
                      "found sentinel during recovery — file was partially sealed");
            }
            break;
        }
```

Add `SEGMENT_FOOTER_LEN` to the `weir_wab::format` import at the top of the file if not already present.

- [ ] **Step 4: Run the new test and the existing sentinel test**

Run: `cargo test -p weir-server --bins recovery -- --test-threads=1`
Expected: PASS, including the pre-existing `recovery_stops_at_partial_seal_sentinel` — a real partial seal has only sentinel + footer behind it and stays on the clean path.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/wab/recovery.rs
git commit -m "fix(wab): a mid-file sentinel destroyed the tail and rewrote the footer to match"
```

---

### Task 2: Recovery's payload cap must match the writer's when zstd is on

**Files:**
- Modify: `crates/weir-server/src/wab/recovery.rs:326`
- Test: `crates/weir-server/src/wab/recovery.rs` tests module

**Interfaces:**
- Consumes: `max_stored_record_bytes()` from `weir_wab::format` (already imported by `wab/segment.rs:17`); `parse_segment_header` from `crate::wab::format` (used at `recovery.rs:2039`).
- Produces: nothing.

The writer bounds a record by `max_stored_record_bytes()` = `compress_bound(16 MiB)` ≈ 16,843,025 when compression is `Zstd` (`segment.rs:139-142`), but recovery uses the flat `MAX_PAYLOAD_HARD_CAP` = 16,777,216. `zstd -1` over 16 MiB of random data produces 16,777,614 bytes — 398 over. Such a record is written, fsynced and acked `true`, then recovery calls it corruption and truncates it and everything behind it.

Recovery already parses the segment header, which carries the compression flag; use it to pick the same cap the writer used.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn recovery_accepts_a_record_the_writer_was_allowed_to_write_under_zstd() {
    // The writer's cap under Zstd is compress_bound(16 MiB) ~= 16,843,025;
    // recovery's was a flat 16,777,216. Incompressible input (pre-compressed
    // or encrypted blobs) lands in that 65,809-byte window: acked durable,
    // then truncated away on the next crash recovery.
    let dir = tempdir_for("zstd_cap_window");
    // A stored size above MAX_PAYLOAD_HARD_CAP but within the writer's bound.
    let stored_len = weir_core::MAX_PAYLOAD_HARD_CAP + 400;
    let path = make_zstd_segment_with_stored_record_len(&dir, 0, stored_len);

    let metrics = test_metrics();
    let before = metrics.recovery_segments_quarantined.get();

    let _ = recover_segment(&path, &dir, &metrics);

    assert_eq!(
        metrics.recovery_segments_quarantined.get(),
        before,
        "a record the writer legitimately accepted must not be read as corruption"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins recovery_accepts_a_record_the_writer -- --test-threads=1`
Expected: FAIL — the record is quarantined as "oversized payload_len".
**If it passes, the finding is refuted. Stop and record that.**

- [ ] **Step 3: Use the compression-aware cap**

At the top of `recover_segment`, after the header is parsed, derive the cap:

```rust
    // Recovery must accept exactly what the writer was allowed to write.
    // Under Zstd a stored record may be up to compress_bound(16 MiB), which
    // exceeds MAX_PAYLOAD_HARD_CAP by ~65 KiB — incompressible payloads land
    // in that window (zstd -1 over 16 MiB of random data = 16,777,614 bytes).
    // Using the flat cap here turns an acked record into "corruption" and
    // truncates every acked record behind it. Mirrors segment.rs:139-142.
    let max_stored = match header_meta.compression {
        Compression::None => MAX_PAYLOAD_HARD_CAP,
        Compression::Zstd => max_stored_record_bytes(),
    };
```

Then change `:326` from `if payload_len > MAX_PAYLOAD_HARD_CAP {` to `if payload_len > max_stored {`, leaving the branch body unchanged.

- [ ] **Step 4: Run the recovery suite**

Run: `cargo test -p weir-server --bins recovery -- --test-threads=1`
Expected: PASS. The oversized-payload test still passes — it uses a length far above both caps.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/wab/recovery.rs
git commit -m "fix(wab): recovery rejected records the writer was allowed to write under zstd"
```

---

### Task 3: A failed fsync must poison the active segment

**Files:**
- Modify: `crates/weir-server/src/wab/segment.rs:600` (`ShardWriter::fsync_current`)
- Modify: `crates/weir-server/src/wab/mod.rs:~832` (the call site, if the borrow changes)
- Test: `crates/weir-server/src/wab/segment.rs` tests module

**Interfaces:**
- Consumes: the existing `ShardWriter { active: Option<Box<WabSegment>>, .. }`.
- Produces: `ShardWriter::fsync_current(&mut self) -> io::Result<()>` — note the receiver changes from `&self` to `&mut self`. `wab/mod.rs`'s caller must hold the writer mutably; it already does for `write_record`.

`WabSegment` poisons on a failed *write* because the file offset has advanced past stray bytes. A failed *fsync* leaves `self.active` untouched, so the same file keeps being written; on Linux the writeback error is reported once per fd and the failed pages are dropped clean, so the next `sync_data()` returns `Ok` and the following batch is acked `true` over a hole.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_failed_fsync_retires_the_active_segment() {
    // Mirrors the write path (segment.rs:571-577): once the file's durability
    // is in doubt, stop using it. Without this the next batch's fsync succeeds
    // (Linux reports a writeback error once per fd, then drops the pages
    // clean) and gets acked TRUE over the hole the failed batch left.
    let dir = tempdir_for("fsync_poison");
    let mut w = shard_writer_with_failing_nth_fsync(&dir, 0, 1);

    w.write_record(b"batch-N").unwrap();
    assert!(w.has_active_segment());

    let first = w.fsync_current();
    assert!(first.is_err(), "the injected fsync must fail");
    assert!(
        !w.has_active_segment(),
        "a failed fsync must retire the segment; reusing it acks later records over a hole"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins a_failed_fsync_retires -- --test-threads=1`
Expected: FAIL — `has_active_segment()` is still `true`.
**If it passes, the finding is refuted. Stop and record that.**

- [ ] **Step 3: Retire the segment on fsync failure**

Change the signature to `pub(crate) fn fsync_current(&mut self) -> io::Result<()>` and on the error path:

```rust
        if let Some(seg) = self.active.as_mut() {
            if let Err(e) = seg.fsync() {
                // Same reasoning as the write path (:571-577): the file's
                // durability is now in doubt, so stop writing to it. Linux
                // reports a writeback error once per fd and then drops the
                // failed pages clean, so the NEXT fsync on this handle returns
                // Ok — and the batch after the failure would be acked `true`
                // over the hole the failed batch left behind.
                self.active = None;
                return Err(e);
            }
        }
        Ok(())
```

Update the `wab/mod.rs` call site to take the writer mutably. The `false` it forwards to `ack_tx` is unchanged, so the nack behaviour for the failing batch is untouched.

- [ ] **Step 4: Run the WAB suites**

Run: `cargo test -p weir-server --bins wab -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/wab/
git commit -m "fix(wab): a failed fsync left the segment active, so later records acked over a hole"
```

---

### Task 4: A respawned drain must rescan the disk

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs:463` (`drain_thread` signature), `:498` (`prev_health_ok` init), `:221-244` (`run_drain_supervised`)
- Test: `crates/weir-server/src/drain/mod.rs` tests module

**Interfaces:**
- Consumes: `probe_and_resume_stranded(.., prev_health_ok: bool, ..)` at `:372`.
- Produces: `drain_thread<S: Sink>(drain_rx, sink, config, metrics, initial_health_ok: bool)` — a fifth parameter. `run_drain_supervised` passes `true` on first start and **`false` on every respawn**.

After a panic, `pending` is rebuilt empty and `prev_health_ok = true`. The only in-run requeue is the `now_ok && !prev_health_ok` edge, which with a healthy sink never fires again — so the stranded backlog is never rescanned before a restart. Starting a respawn at `false` makes the first healthy probe an edge, which re-queues everything on disk.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_respawned_drain_rescans_the_disk_for_stranded_segments() {
    // After a panic, `pending` is gone and the only rescan is gated on a
    // down->up health edge. Starting a respawn at prev_health_ok = true means
    // that edge never fires again with a healthy sink, so the backlog waits
    // for a process restart while drain_state reads `draining`.
    let dir = tempdir_for("respawn_rescan");
    let _seg = make_sealed_unconfirmed_segment(&dir, 0, &[b"stranded"]);
    let metrics = test_metrics();

    // A respawn starts here: no in-memory pending, healthy sink.
    let resumed = probe_and_resume_stranded(
        &healthy_sink(),
        &fast_config(dir.clone()),
        &metrics,
        &mut VecDeque::new(),
        /* prev_health_ok */ false,
    );

    assert!(resumed, "a respawn must treat the first healthy probe as an edge");
    assert_eq!(
        metrics.drain_segments_resumed.get(),
        1,
        "the sealed-unconfirmed segment on disk must be re-queued"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins a_respawned_drain_rescans -- --test-threads=1`
Expected: FAIL to **compile** — `drain_thread` has no `initial_health_ok` parameter yet, and the test asserts a behaviour no caller can currently produce.
**If the equivalent already holds, the finding is refuted. Stop and record that.**

- [ ] **Step 3: Thread the initial health state through**

Add the parameter and use it:

```rust
fn drain_thread<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    // False on every respawn. A panic takes `pending` with it, and the only
    // in-run rescan is the now_ok && !prev_health_ok edge — starting at `true`
    // with a healthy sink means that edge never fires and the backlog waits
    // for a process restart. Starting at `false` makes the first healthy probe
    // an edge, which re-queues everything still on disk.
    initial_health_ok: bool,
) {
```

and at `:498` replace `let mut prev_health_ok = true;` with `let mut prev_health_ok = initial_health_ok;`.

In `run_drain_supervised`, pass `attempts == 0` so the first start is `true` and every respawn is `false`:

```rust
        let first_start = attempts == 0;
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            drain_thread(rx, sink, cfg, m, first_start)
        }));
```

- [ ] **Step 4: Run the drain suite**

Run: `cargo test -p weir-server --bins drain -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/drain/mod.rs
git commit -m "fix(drain): a respawned drain never rescanned disk, stranding the backlog until restart"
```

---

### Task 5: A dead-letter open failure must not look like a clean exit

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs:474-479`
- Test: `crates/weir-server/src/drain/mod.rs` tests module

**Interfaces:**
- Consumes: `DeadLetterWriter::open(&Path) -> io::Result<DeadLetterWriter>`.
- Produces: nothing; the fix is a `panic!` so the existing supervisor's `catch_unwind` handles it.

`DeadLetterWriter::open` failure logs and `return`s. The supervisor reads that as `Ok(())` — "the channel closed, we are done" — so it does not respawn and does not bump `drain_panics`. `open` → `scan_dir` propagates `entry.metadata()?`, so an ENOENT from a file vanishing between `read_dir` and `stat` is enough. The daemon then boots, binds, and acks producers forever while nothing is ever delivered.

Panicking is the right shape here: it is exactly what the supervisor exists to catch, it bumps `drain_panics`, and a genuinely persistent failure exhausts `max_respawns` and produces the loud "delivery is stopped" error the operator needs.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_dead_letter_open_failure_is_not_mistaken_for_a_clean_shutdown() {
    // The supervisor's contract is "Ok(()) means the channel closed". Returning
    // Ok on an open failure tells it delivery finished normally, so it does not
    // respawn and does not count a panic — and the daemon acks producers
    // forever while nothing is delivered. A transient ENOENT during scan_dir
    // is enough to trigger it.
    let dir = tempdir_for("dl_open_failure");
    // Make <wab_dir>/dead_letter un-openable.
    let dl = dir.join("dead_letter");
    std::fs::write(&dl, b"not a directory").unwrap();

    let metrics = test_metrics();
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
    drop(tx);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drain_thread(rx, healthy_sink_arc(), fast_config(dir.clone()), metrics.clone(), true)
    }));

    assert!(
        result.is_err(),
        "an unusable dead-letter store must surface as a panic the supervisor can see, \
         not as a clean channel-closed exit"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins a_dead_letter_open_failure -- --test-threads=1`
Expected: FAIL — `catch_unwind` returns `Ok`, because the current code returns normally.
**If it passes, the finding is refuted. Stop and record that.**

- [ ] **Step 3: Make the failure visible to the supervisor**

```rust
    let mut dead_letter = match DeadLetterWriter::open(&config.wab_dir) {
        Ok(dl) => dl,
        Err(e) => {
            // NOT a bare `return`. run_drain_supervised reads a normal return
            // as "the channel closed, delivery is finished" — so it neither
            // respawns nor counts a panic, and the daemon goes on acking
            // producers while nothing is ever delivered. Panicking routes this
            // into the supervisor, which retries and, if it is persistent,
            // exhausts max_respawns and logs the loud "delivery is stopped"
            // error an operator can act on. A transient ENOENT inside scan_dir
            // (a file vanishing between read_dir and stat) is enough to reach
            // this path.
            error!(error = %e, "drain: failed to open dead-letter writer");
            panic!("drain: dead-letter writer unavailable: {e}");
        }
    };
```

- [ ] **Step 4: Run the drain suite**

Run: `cargo test -p weir-server --bins drain -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/drain/mod.rs
git commit -m "fix(drain): a dead-letter open failure read as a clean exit, so the daemon acked forever"
```

---

### Task 6: Detect a wedged drain

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` (heartbeat bump in the loop), `crates/weir-server/src/main.rs:718-757` (the existing 5 s WAB poll)
- Test: `crates/weir-server/src/drain/mod.rs` tests module

**Interfaces:**
- Produces: `pub(crate) struct DrainHeartbeat(Arc<AtomicU64>)` with `fn bump(&self)` and `fn get(&self) -> u64`, constructed in `main.rs` and cloned into both `run_drain_supervised` and the poll task.

The drain is a current-thread runtime, so `tokio::time::timeout` on `commit`/`health` cannot fire against a sink that blocks the thread. Measured: a 50 ms `commit_timeout` against a 1.2 s blocking commit elapsed 1.214 s and confirmed the batch. No watchdog exists, and the one warning that might catch it — `should_warn_wab_growth` — is suppressed when `drain_healthy`, which is exactly the wedged state, with both gauges pre-initialised to it.

A counter the drain bumps each loop iteration, checked by a task on a *different* runtime, is the smallest thing that can observe this.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_stalled_drain_is_visible_to_an_outside_observer() {
    // tokio::time::timeout cannot fire against a sink that blocks the drain's
    // current-thread runtime, so liveness has to be observed from outside.
    let hb = DrainHeartbeat::new();
    let first = hb.get();
    hb.bump();
    assert!(hb.get() > first, "a live drain advances the heartbeat");

    let stalled = hb.get();
    // Simulate a wedged drain: no bumps at all.
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(hb.get(), stalled, "a wedged drain leaves the heartbeat frozen");
    assert!(
        drain_appears_stalled(stalled, hb.get()),
        "an unchanged heartbeat across a poll interval must read as stalled"
    );
}
```

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `cargo test -p weir-server --bins a_stalled_drain_is_visible -- --test-threads=1`
Expected: FAIL to compile — `DrainHeartbeat` and `drain_appears_stalled` do not exist.

- [ ] **Step 3: Add the heartbeat**

```rust
/// A monotonic counter the drain advances once per loop iteration.
///
/// The drain runs on a current-thread runtime, so a sink that BLOCKS the thread
/// (a blocking DB driver, std::fs, a spin loop) defeats both
/// `tokio::time::timeout` backstops — they can only fire if the awaited future
/// yields. Measured: commit_timeout=50ms against a 1.2s blocking commit elapsed
/// 1.214s and confirmed the batch. Liveness therefore cannot be observed from
/// inside the drain; this counter lets a task on another runtime see it.
#[derive(Clone, Default)]
pub(crate) struct DrainHeartbeat(std::sync::Arc<std::sync::atomic::AtomicU64>);

impl DrainHeartbeat {
    pub(crate) fn new() -> Self { Self::default() }
    pub(crate) fn bump(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn get(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// True when the heartbeat has not moved between two observations.
pub(crate) fn drain_appears_stalled(previous: u64, current: u64) -> bool {
    previous == current
}
```

Bump it at the top of the drain's main loop body. In `main.rs`'s existing 5 s poll, hold the previous value and, when `drain_appears_stalled` is true for **three** consecutive polls (15 s), log a `warn!` naming the likely cause. Do not change `drain_state` — a wedged drain is not `stopped`, and mislabelling it would break the readiness script's meaning.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p weir-server --bins drain -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/weir-server/src/drain/mod.rs crates/weir-server/src/main.rs
git commit -m "feat(drain): a heartbeat, because a blocking sink defeats every timeout we have"
```

---

### Task 7: Fix the polyglot clients and stop them rotting again

**Files:**
- Modify: `demos/py-wire-client/`, `demos/go-wire-client/`, `demos/c-wire-client/`, and the Java and TypeScript clients — every occurrence of the tier name `Sync`
- Create: a `conformance` job in `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `docs/conformance/wire_v1_vectors.json`, which now spells the durable tier `Durable`.
- Produces: a CI job later work can rely on.

Three of five demo clients fail conformance **today**: python 15/30, go FAIL, c FAIL — all `durability='Sync' != 'Durable'`. Commit `2116377` renamed the tier in the vectors; `demos/` was last touched before it, and no workflow runs them, so the breakage shipped in 2.0.0.

- [ ] **Step 1: Reproduce the failures**

```bash
python3 docs/conformance/run_vectors.py          # expect 30/30 — the Rust side is fine
python3 demos/py-wire-client/tests/test_conformance.py   # expect 15/30, exit 1
( cd demos/go-wire-client && go test ./... )     # expect FAIL
( cd demos/c-wire-client && make check )         # expect RESULT: FAIL
```

- [ ] **Step 2: Rename the tier in all five clients**

```bash
grep -rln "Sync" demos/ | grep -vE '\.(o|a|out)$'
```

For each hit, rename the durable tier constant/label from `Sync` to `Durable`. **Keep the wire byte `0x01` unchanged** — this is a name change only. Check the Java and TypeScript clients too even though their suites currently pass; they may not assert on the label.

- [ ] **Step 3: Confirm all five pass**

```bash
python3 demos/py-wire-client/tests/test_conformance.py   # expect 30/30
( cd demos/go-wire-client && go test ./... )             # expect ok
( cd demos/c-wire-client && make check )                 # expect RESULT: PASS
```

- [ ] **Step 4: Add the CI job**

Add to `.github/workflows/ci.yml`, modelled on the existing `lint` job's shape:

```yaml
  conformance:
    name: conformance (wire vectors + polyglot clients)
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: stable }
      # The Rust decoder is already covered by crates/weir-core/tests/conformance.rs
      # in the `test` job. What had NO coverage were the five from-spec clients —
      # which is how commit 2116377's Sync -> Durable rename broke three of them
      # and shipped in 2.0.0 unnoticed.
      - run: python3 docs/conformance/run_vectors.py
      - run: python3 demos/py-wire-client/tests/test_conformance.py
      - run: cd demos/go-wire-client && go test ./...
      - run: cd demos/c-wire-client && make check
```

- [ ] **Step 5: Commit**

```bash
git add demos/ .github/workflows/ci.yml
git commit -m "fix(demos): three conformance clients broke on the tier rename and CI could not see it"
```

---

### Task 8: Bump to 2.0.1 and write the release notes

**Files:**
- Modify: `Cargo.toml` (workspace version), `Cargo.lock`, `CHANGELOG.md`, `demo/version.js` (generated)

**Interfaces:** none.

- [ ] **Step 1: Bump the workspace version**

In `Cargo.toml`, `version = "2.0.0"` → `version = "2.0.1"`. Then:

```bash
cargo update -w
./scripts/sync-demo-version.sh
git diff --exit-code demo/version.js || true   # expect a diff; it is regenerated
```

- [ ] **Step 2: Write the CHANGELOG entry**

Add a `## [2.0.1] - <today>` section above `## [2.0.0]`, with a `### Fixed` list covering Tasks 1-7. Lead each entry with the observable symptom, not the code change — a reader is trying to work out whether they were affected. State plainly that these were found by an audit of the 2.0.0 tree and that **no user report prompted them**.

- [ ] **Step 3: Run the full gate**

Every command in `CONTRIBUTING.md`'s "pre-PR gate", plus `mdbook build`.
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md demo/version.js
git commit -m "chore(release): 2.0.1 — silent-failure fixes"
```

- [ ] **Step 5: Open the PR**

Base `main`, head `fix/silent-failures-2.0.1`. The body should lead with the four confirmed silent paths and their test evidence, link the spec, and state which findings were refuted or downgraded.

---

## Self-Review

**Spec coverage:** S1→Task 1, S2→Task 2, S3→Task 3, S4→Task 4, S5→Task 5, S6→Task 6, S7→Task 7. Release mechanics→Task 8. Every "Out of scope" item is absent from the plan, as intended.

**Placeholders:** none — every code step carries real code, every test step a real command and expected result.

**Type consistency:** `DrainHeartbeat`/`drain_appears_stalled` are defined in Task 6 and used only there. `drain_thread`'s new `initial_health_ok` parameter is introduced in Task 4 and consumed by Task 5's test, which passes `true` — consistent. `max_stored_record_bytes()` and `SEGMENT_FOOTER_LEN` both come from `weir_wab::format`, matching `wab/segment.rs:17`.

**Ordering note:** Task 5's test calls `drain_thread` with five arguments, so **Task 4 must land first**. All other tasks are independent.
