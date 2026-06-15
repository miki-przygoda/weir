# WAB subsystem — adversarial verification (sweep 2026-06-14)

Summary: 8 findings reviewed; 7 confirmed real (1 critical counter-reset data-loss bug + its doc facet, 1 recovery parent-dir-fsync gap, 1 swallowed-dirent replay gap, 1 dead-letter-dir mis-recovery, 2 recovery test gaps) and 1 confirmed-as-accurate informational platform-durability note. Zero refuted.

## Confirmed (real)

### 1. Segment counter resets to 1 after crash recovery; seal()'s rename silently overwrites a still-undrained recovered `.wab.sealed` (data loss, crown-invariant violation)
- **File:** `crates/weir-server/src/wab/segment.rs:335` (counter scan) + `:258` (seal rename) + `mod.rs:231` / `:434`
- **Severity:** critical
- **Verdict:** **real**

Argument (every link verified):
1. `wab::spawn` runs `recover_open_segments` in Phase 1 (`mod.rs:231`) before flushers spawn; `recover_segment` renames every unsealed `seg_NNNNNNNN.wab` to `…wab.sealed` (`recovery.rs:322`), so after recovery there are zero active `.wab` files.
2. The flusher's only counter init is `scan_and_advance_counter` (`segment.rs:414`) → `FsSegmentStore::segment_counters` (`segment.rs:335`) → `segment_counter_from_path` → `Path::file_stem()` (`segment.rs:282-284`). I verified with rustc: `seg_00000001.wab` → `Some(1)`, but `seg_00000001.wab.sealed` and `…wab.confirmed` → stem `seg_00000001.wab`, which fails `parse::<u64>()` → `None`. Sealed/confirmed files are invisible to the scan.
3. After recovery has sealed everything, the scan returns an empty set, `max = 0`, and `next_counter` stays 1 (`segment.rs:421-423`).
4. `WabSegment::create` uses `create_new(true)` / `O_EXCL` (`segment.rs:59`), which only collides with an existing active `.wab` at the exact path — not with `.wab.sealed`. So the first post-startup record successfully creates `seg_00000001.wab`.
5. On rotation/shutdown, `WabSegment::seal` → `std::fs::rename(active, sealed)` (`segment.rs:258`). I verified with a runtime test that `std::fs::rename` atomically OVERWRITES an existing destination on Unix (POSIX `rename(2)`), destroying any existing `seg_00000001.wab.sealed`.
6. `replay_unconfirmed` only *queues* recovered sealed segments to the bounded drain channel (`mod.rs:328-347`, called at `main.rs:424`); it does not wait for delivery. `confirm_and_delete` only removes a sealed file *after* a successful drain (`drain/confirmed.rs:42`). When the sink is slow/down the recovered `seg_00000001.wab.sealed` sits undrained — holding durably-acked-but-undelivered records — until the new run's seal overwrites it. That retroactively turns a true ack into a false ack.

Contrast confirming the omission: the dead-letter scanner deliberately counts BOTH `dl_*.wab` and `dl_*.wab.sealed` by splitting on `.` (`dead_letter.rs:103-105`) precisely to avoid counter reuse; the WAB scan lacks that handling.

Fix: derive the counter from the max across `.wab`, `.wab.sealed`, and `.wab.confirmed` so a new segment never reuses a counter with any on-disk artifact (and/or guard seal()'s rename against an existing destination).

**verdict_reason:** rustc-confirmed `file_stem()` hides sealed/confirmed from `segment_counters` (segment.rs:335) → `next_counter` resets to 1, and `std::fs::rename` in seal() (segment.rs:258) overwrites the undrained recovered `.wab.sealed`.

### 2. `segment_counters` doc justifies skipping sealed files as "matches the historical scan" — stale rationale masking the counter-reset bug
- **File:** `crates/weir-server/src/wab/segment.rs:319`
- **Severity:** low
- **Verdict:** **real**

The `SegmentStore::segment_counters` doc (`segment.rs:319-323`) says sealed files "do not parse … and so are skipped — this matches the historical scan, which only advanced past active segments." Because `recover_open_segments` seals all active segments before any flusher scans (finding 1), after a crash the scan can only ever see an empty active set, so the "historical" reasoning guarantees a counter reset rather than "one past the highest existing segment." The comment makes the sealed-skip look intentional and safe, hiding the data-loss bug. Same fix as finding 1, plus updating the comment.

**verdict_reason:** the comment at segment.rs:319-323 frames the sealed-skip as safe-by-design, which is the doc-facing facet of the confirmed bug in finding 1.

### 3. `recover_segment` renames `.wab`→`.wab.sealed` without `fsync_parent_dir`, unlike `WabSegment::seal` — recovered seal not crash-durable
- **File:** `crates/weir-server/src/wab/recovery.rs:322`
- **Severity:** medium
- **Verdict:** **real**

`recover_segment` writes sentinel + footer, calls `file.sync_all()` (`recovery.rs:318`), then `fs::rename(path, &sealed)` (`recovery.rs:322`) and returns at `:329` with no parent-dir fsync. The production seal path is deliberately careful: `WabSegment::seal` calls `fsync_parent_dir(&sealed_path)` after its rename (`segment.rs:263`), and the helper doc (`segment.rs:518-528`) states POSIX only makes a rename's dirent crash-durable after a parent-dir fsync. The recovery path omits that single step. If the daemon crashes during/just-after recovery the `.wab.sealed` dirent can be lost; the file re-appears as `.wab` and is re-recovered + re-drained (dedup makes it idempotent), so not data loss — but it violates the module's own documented durability discipline. Fix: one `fsync_parent_dir(&sealed)?` after `recovery.rs:322`. (Raw finding cited line 424; actual rename is at `recovery.rs:322` — confirmed.)

**verdict_reason:** recovery.rs:318-329 fsyncs the file but never the parent dir after the rename, whereas segment.rs:263 explicitly does, by the same module's documented rule.

### 4. `replay_unconfirmed` silently skips sealed-but-unconfirmed segments on a per-entry DirEntry error (`filter_map(|e| e.ok())`)
- **File:** `crates/weir-server/src/wab/mod.rs:321`
- **Severity:** medium
- **Verdict:** **real**

`replay_unconfirmed` enumerates each shard's sealed segments with `fs::read_dir(&sdir)?.filter_map(|e| e.ok())` (`mod.rs:321-325`). A per-entry `DirEntry` `Err` (transient FS/NFS error, concurrent rename, permission blip) is silently dropped, so that `.wab.sealed` is omitted from the startup replay. Those segments hold records already durably acked under the crown invariant but never delivered — they sit undelivered until a later clean restart re-enumerates them, with no log/metric for the skip. The inconsistency is the tell: the crash-recovery scans were deliberately hardened to PROPAGATE dirent errors via `.collect::<io::Result<Vec<_>>>()?` (`recovery.rs:30-32` and `:99-101`, with explicit "propagating any dirent error, as the streaming `?` did" comments), while this delivery-critical replay path still swallows them. Recoverable on a subsequent clean restart (not permanent loss), but delays at-least-once delivery indefinitely.

**verdict_reason:** mod.rs:321 uses `filter_map(|e| e.ok())` (drops dirent errors) while recovery.rs:30-32/99-101 in the same subsystem deliberately propagate them — confirmed inconsistency on the durable-but-unconfirmed delivery path.

### 5. `recover_open_segments` processes the `dead_letter/` directory as if it were a shard directory
- **File:** `crates/weir-server/src/wab/recovery.rs:40`
- **Severity:** low
- **Verdict:** **real**

`recover_open_segments` iterates every subdirectory of `wab_dir` and only skips one named exactly `quarantine` (`recovery.rs:40`). The dead-letter dir lives at `<wab_dir>/dead_letter/` (`dead_letter.rs:34`) and holds segment-format files (`dl_NNNNNNNN.wab`, sealed to `…wab.sealed` — `dead_letter.rs:69,75`). A crash during `write_records` after `create` but before `seal`'s rename leaves an active `dl_NNNNNNNN.wab`. On next startup, recovery treats `dead_letter/` as a shard dir (`is_dir()` true, name ≠ `quarantine`), and `recover_shard_dir`'s filter (`recovery.rs:103-106`: extension `wab` and ends-with `.wab`) matches the torn `dl_*.wab` and re-seals it — bypassing dead-letter accounting and the `dl_` counter ownership. Mostly benign because the segment format matches (so it is re-sealed, not quarantined — quarantine only fires on bad magic/version/short-header), but unintended. (The finding's "or worse, quarantined" sub-claim is the weaker case: a torn-but-valid-header `dl_*.wab` is truncated+sealed, not quarantined.) Fix: skip `dead_letter` like `quarantine` (ideally skip any non-`shard_*` directory).

**verdict_reason:** recovery.rs:36-44 only denylists `quarantine`, so the `<wab_dir>/dead_letter/` dir (dead_letter.rs:34) is descended into and its torn `dl_*.wab` re-sealed by recover_shard_dir's `.wab` filter.

### 6. Recovery: no test for the oversized-`payload_len` boundary (record at exactly MAX_PAYLOAD_HARD_CAP must survive; one over must truncate)
- **File:** `crates/weir-server/src/wab/recovery.rs:234`
- **Severity:** medium
- **Verdict:** **real**

`recover_segment` treats a record whose length field exceeds `MAX_PAYLOAD_HARD_CAP` as corruption and truncates at the last valid record (`recovery.rs:234`) — a data-bearing decision on the durability path. I read every recovery test: clean, empty, truncate-mid-record, bad-magic, unknown-version, multi-segment deterministic-seal, the `check_confirmed` variants, and `audit_segment_modes`. None exercises the oversized-`payload_len` branch — neither the boundary (a legit record at exactly MAX_PAYLOAD_HARD_CAP must be RECOVERED) nor the just-over case (must truncate). The MAX_PAYLOAD_HARD_CAP tests in `tests/system.rs:526-614` exercise the *ingest/socket* cap (`NackReason::PayloadTooLarge`), not the recovery decode path. A future off-by-one (`>` vs `>=`) would silently drop the largest legal records on recovery with a green suite.

**verdict_reason:** the recovery.rs:234 `> MAX_PAYLOAD_HARD_CAP` branch has no test; the only MAX_PAYLOAD_HARD_CAP tests (system.rs:540/567) hit the socket layer, not recover_segment.

### 7. Recovery: the partial-seal sentinel branch during crash recovery is untested
- **File:** `crates/weir-server/src/wab/recovery.rs:229`
- **Severity:** low
- **Verdict:** **real**

`recover_segment` has an explicit branch (`recovery.rs:229`) for hitting a zero-length sentinel mid-recovery, documented (`recovery.rs:226-228`) as graceful handling of partial seals (sentinel written, footer/rename not). No recovery test constructs a segment with a written sentinel and then runs `recover_segment` to assert it stops at the sentinel and recovers exactly the pre-sentinel records. The DST `RenameFails` scenario (`dst.rs:499-510`) calls `finalize_to_disk` — sentinel AND footer both written + fsynced — not the in-between state where only the sentinel landed. Grep of the whole test tree found no test writing `build_sentinel`/`[0u8;4]` then recovering. Branch has no direct coverage. (Low: the same `payload_len == 0` decode path is exercised indirectly by `SegmentReader` round-trips and the empty-segment recovery, so a regression here is partially fenced.)

**verdict_reason:** no test in recovery.rs or tests/ writes a sentinel-without-footer mid-file then runs recover_segment; the DST RenameFails path uses fully-finalized segments (dst.rs:505).

## Refuted / dismissed

(none — the one remaining finding is informational and accurate, recorded below)

### 8. macOS data fsync uses F_BARRIERFSYNC (barrier) while directory durability uses plain fsync via sync_all — weaker than F_FULLFSYNC; an explicit, undertested tradeoff
- **File:** `crates/weir-server/src/wab/segment.rs:506`
- **Severity:** info
- **Verdict:** **real** (accurate description of a deliberate, documented tradeoff — not a defect)

On macOS, record/seal durability goes through `platform_fsync` = `fcntl(F_BARRIERFSYNC)` (`segment.rs:506`), a write barrier that orders writes but, unlike `F_FULLFSYNC`, does not force the drive to flush its volatile cache to the medium. The parent-dir durability path uses `File::open(dir)?.sync_all()` (`segment.rs:535`), i.e. plain `fsync`. The code documents F_BARRIERFSYNC as "sufficient for WAB durability" (`segment.rs:503-505`). For an ordered-WAB design on APFS this is defensible, but the crown "an ack is never a false ack" guarantee on macOS rests on barrier semantics, differs from the Linux `fdatasync` path, and is not validated against power loss (the DST harness injects logical faults, not cache loss). The finding is an accurate, precise platform-durability note for the 1.0 claim; if strict power-loss durability on macOS is required, the data path needs F_FULLFSYNC (with the documented throughput cost). No code defect.

**verdict_reason:** segment.rs:500-511 and :532-538 match the finding exactly — F_BARRIERFSYNC for data, plain fsync for dir; a real, documented per-platform tradeoff, correctly flagged as info rather than a bug.
