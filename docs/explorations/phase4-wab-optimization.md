# Phase 4 WAB Optimization — Exploration

**Date:** 2026-06-11  
**Branch:** `v1/phase-3-performance` (base for Phase 4 scoping)  
**Status:** Research / proposal — no implementation.

---

## 1. The fsync-bound constraint: what it rules in and out

Phase 3 measured the actual time distribution for a durable write on both
target machines:

| machine | fdatasync p50 | fdatasync share of Sync latency |
|---|---|---|
| Mac NVMe (`F_BARRIERFSYNC`) | ~150 µs | ~89% |
| beast SATA SSD (`fdatasync`) | ~1 394 µs | ~99% |

Everything Phase 3 optimised — the bridge hop (4 → 2 µs), the write syscalls
(`writev` −60%), the Bytes payload (O(1) clone), the adaptive coalesce window
— is invisible against this floor. The entire pre-fsync software pipeline is
now ~7–10 µs on the Mac (queue 2, bridge_wait 2, write 5); on beast's slower
storage that rounding error is even smaller proportionally.

io_uring was validated on beast across 4 runs and **never beat writev +
fdatasync** (−0.2% to −46.5% worse). The ring bookkeeping adds overhead the
storage savings can never recoup because the savings themselves are zero: the
~2 ms cost is the media, not the syscall overhead.

### What this rules out

Any idea whose primary mechanism is "make the write cheaper before fsync" is
already captured or dead:

- Batching more records per write (Phase 3: group-fsync + `writev`)
- Eliminating buffer copies (Phase 3: `bytes::Bytes`)
- Reducing syscall count (Phase 3: writev; io_uring tried and rejected)
- Removing intermediate threads (Phase 3: bridge removal)
- CPU-side concurrency tricks (SCHED_FIFO, core affinity, SIMD warmup: already in)

### What this rules in

The only levers that can materially move the needle are those that reduce the
**number of fsyncs the media sees**, or **avoid fsync for a class of records
entirely**, or **improve the read path** (drain), which is independent of the
write-side fsync floor:

- Fsync amortisation: commit coalescing across shards or callers
- Durability-tier contracts: bounded-staleness writes that genuinely skip fsync
- Alternative sync primitives that flush less data per call
- Read-path efficiency: the drain reads are unrelated to fsync and currently
  use a `BufReader<File>` iteration loop — there is headroom here
- Storage hardware selection: the 9× latency gap between NVMe and SATA SSD
  is itself a "lever" for operators

---

## 2. Optimization candidates

### 2.1 Cross-shard fsync coalescing ("global group-commit")

**What it is.** Today, each shard flusher runs its own independent `flush_batch`
→ `fdatasync` loop (in `wab/mod.rs::flusher_thread`). N shards → N independent
fsyncs, potentially in parallel. On a single drive this is not additive — the
drive serialises them — but the software still issues N syscalls and collects N
completion events. Under high-concurrency, many-shard configs, this can
translate to more fsyncs per unit time than a coordinated coalesce would need.

A "commit coalescing tier" would have all per-shard flushers accumulate their
ready batches and then nominate one of themselves (or a dedicated commit thread)
to issue a single `fdatasync`. Every shard whose records are included in that
round gets its acks fired from the single completion.

**Expected impact.** High only when `shard_count > 1` **and** the drive is a
single spindle or an SSD serialising concurrent fsyncs. For the current default
(`shard_count = 1`) there is no impact at all. With `shard_count = 4` on beast's
SATA SSD (where each fsync is ~1.4 ms), four independent shards could trigger
four sequential media operations per batch cycle; coalescing them to one would
be a ~4× throughput improvement on that dimension. The gain shrinks with NVMe
because the media latency is lower and the marginal cost of extra fsyncs drops.

**Measure-first.** Quantify whether multiple shards actually issue overlapping
or near-sequential fsyncs on the test machine under herd load before building
the coalescing tier. The Phase 3 data used `shard_count = 1`; the benefit is
only provable with multi-shard load data.

**Effort/risk.** High effort, moderate risk. Requires a synchronisation point
between per-shard flusher threads — a barrier or a designated "fsync leader"
— which adds complexity and potential latency jitter if one shard lags.
Deadlock/starvation risk requires careful design. Recommend validating the
multi-shard fsync behaviour first.

---

### 2.2 `sync_file_range` as a write-behind hint (Linux only)

**What it is.** Linux's `sync_file_range(2)` with `SYNC_FILE_RANGE_WRITE` can
kick off write-back of dirty pages asynchronously before the actual blocking
`fdatasync` call. The idea: after writing a batch of records, call
`sync_file_range(..., SYNC_FILE_RANGE_WRITE)` immediately (non-blocking — it
only schedules write-back, does not wait for it), then proceed to accept more
records, and only call `fdatasync` once the next batch is ready to be acked.
The write-back for batch N is partially or fully in flight by the time batch N
is acknowledged.

This is the kernel-visible version of a write-behind cache. It does not reduce
the number of `fdatasync` calls — you still need one per batch boundary — but
it can reduce the *blocking time* of each `fdatasync` by the time the
non-blocking `sync_file_range` already spent pushing data to the drive.

The downside is it's Linux-only (`cfg(target_os = "linux")`), requires `unsafe`
FFI (not exposed in `std`), and its benefit depends entirely on how quickly the
drive services the pre-issued write-back. On beast's SATA SSD, where `fdatasync`
blocks for ~1.4 ms, even partial preflushing could help. On fast NVMe it's
likely within noise.

**Expected impact.** Moderate on SATA SSD (potentially 20–50% reduction in
blocking fdatasync time for a batch that follows a pre-issued sync_file_range),
low on NVMe, zero on macOS (which uses `F_BARRIERFSYNC` and doesn't have this
syscall). The writev path already writes the full record before the fsync, so
the dirty pages exist; the question is whether scheduling write-back earlier
amortises the media latency into the inter-batch gap.

**Measure-first.** Issue `sync_file_range` after every `write_vectored` in
`WabSegment::write_record` (or at batch-flush time in `flush_batch`), then
measure whether the wall-clock time spent in the subsequent `fdatasync`
decreases. `bench-trace`'s `stage_write` and `stage_total` metrics can observe
this directly without new instrumentation.

**Effort/risk.** Low-moderate effort: a few lines of `libc` FFI behind
`#[cfg(target_os = "linux")]` in `segment.rs`'s `platform_fsync` or a new
`platform_presync` call from `flush_batch`. Risk: `sync_file_range` semantics
are subtle (SYNC_FILE_RANGE_WRITE does not guarantee data is on stable media —
it only starts write-back). The subsequent `fdatasync` remains mandatory for
durability; this is purely a scheduling hint. Misuse (replacing `fdatasync`)
would be a durability bug.

---

### 2.3 Bounded-staleness (`Buffered` tier) honest accounting + a new `LazySync` tier

**What it is.** The existing `Buffered` durability tier acks immediately without
fsync (evident in `flush_batch`: `Durability::Buffered` → `ack_tx.send(true)`
without waiting). This is already the right design for callers that don't need
per-record durability. However, there is no tier between "immediate ack, no
durability" and "ack only after fdatasync" — a large class of real workloads
wants something in between: "ack when the record is in-kernel write-back
buffers, deliver it to the drain eventually, but don't block me on the fdatasync
right now."

A `LazySync` tier would ack after the write syscall (record is in the OS page
cache) but before `fdatasync`. It would still ride the same group-fsync as Sync
and Batched records — the fsync still happens, it just happens asynchronously
relative to the ack. The producer gets its ack in ~7 µs (write-stage latency)
instead of ~1.4 ms, while the daemon still ensures the record reaches stable
storage within one batch deadline.

The bounded-staleness window is the `batch_deadline` (currently 1 ms). A crash
inside that window loses the LazySync records — the same risk as `Buffered`,
except LazySync records are included in the next fdatasync rather than being
lost entirely.

**Expected impact.** For latency-sensitive callers that can tolerate a 1-ms
durability window, this turns a 1.4 ms p50 into a ~7 µs p50. Throughput for
LazySync-dominant workloads becomes purely limited by write throughput (~40k+
RPS on beast) rather than the fsync floor (647 RPS Sync). Honest about the
risk: it is NOT durable to power loss during the batch window.

**Measure-first.** Define the semantics precisely before implementing (what does
recovery do with a LazySync record whose batch was in-flight at crash time?
Answer: the same as Buffered — it's lost. Is that acceptable to the caller?
That's a product decision, not a measurement question.)

**Effort/risk.** Low-moderate effort: add a `Durability::LazySync` variant to
`weir_core/src/lib.rs`, handle it in `flush_batch` identically to `Buffered`
for the ack but include it in the group-fsync accounting. Risk: easy for callers
to misuse if the semantics are not clearly documented. Mixing LazySync and Sync
records in the same batch is safe (the Sync records still gate the fsync; the
LazySync records just ack earlier).

---

### 2.4 mmap-based segment reads in the drain (zero-copy drain path)

**What it is.** The drain's `SegmentReader` in `wab/mod.rs` uses `BufReader<File>`
to iterate records: it issues `read_exact` calls for the 4-byte length, 4-byte
CRC, and then a heap-allocated `vec![0u8; payload_len]` for the payload. Each
record in the drain allocates a new `Vec<u8>`, immediately freezes it into a
`bytes::Bytes` via `Payload::from(payload_buf)` (the `freeze` call in
`SegmentReader::next`), and passes it upstream.

An mmap-based reader would `mmap` the sealed segment file read-only and vend
`Bytes::copy_from_slice` (or ideally a zero-copy sub-slice of the mmap) for each
record's payload. The mmap approach avoids the per-record `Vec` allocation and
the `read_exact` syscalls entirely — the OS page-cache backs the mapping and the
kernel pre-faults pages via `MADV_SEQUENTIAL`.

The `payload.rs` comment already notes this possibility: `bytes::Bytes` was
chosen precisely because it makes O(1) clone transfers through the drain and HTTP
sink path cheap, which is the prerequisite for a zero-copy mmap drain.

**Expected impact.** The drain is NOT on the fsync-bound critical path — it runs
asynchronously on a separate thread. Reducing drain latency does not improve Sync
write latency at all. However, it can improve throughput when the daemon is
catching up after a crash/recovery replay of many sealed segments, and reduces
allocator pressure during high-volume drains. On beast at 157k Buffered RPS, the
drain's per-record allocation would be the bottleneck, not the fsync.

Phase 3 confirmed 249:1 IOPS compression (records per sink commit) in the herd
Buffered benchmark. mmap would make the record-reading side cheaper so it keeps
up with that rate more easily under sustained load.

**Measure-first.** Profile the drain's per-segment iteration time under a
sustained Buffered load (where segments are produced fast and the drain must keep
pace). If allocation is not the bottleneck — i.e., the drain spends its time in
network I/O to the sink — mmap buys nothing.

**Effort/risk.** Moderate effort: replace `SegmentReader` in `wab/mod.rs` with
an mmap-backed variant using `memmap2` or `libc::mmap`. Risk: mmap lifetimes
require care (the file cannot be deleted while the mapping exists); the drain
already has the segment lifecycle under control (it confirms then deletes), so
the natural place is to open the mapping, drain all records, then drop the
mapping before the confirm-and-delete step. On Linux `MADV_SEQUENTIAL` is a
strong hint to the kernel to read-ahead. Recovery in `recovery.rs` uses
`BufReader` too and could benefit from the same change.

---

### 2.5 Segment format evolution: inline bloom filter / skip index for recovery

**What it is.** Crash recovery in `recovery.rs::recover_segment` replays every
record in an unsealed segment sequentially: read 4-byte length, read 4-byte CRC,
read N-byte payload, hash payload, compare CRC — for every record, until a
mismatch or EOF. On a large segment (default 256 MiB) this is a full sequential
read with per-record CRC computation. On beast's SATA SSD, reading 256 MiB
sequentially takes several hundred milliseconds, plus the CRC overhead.

Two sub-ideas:

**(a) Trailing record-index.** During normal operation, before the sentinel is
written at seal time, append a compact record-offset table (8 bytes per record:
file offset of the length field). Recovery can then binary-search to the last
valid boundary without reading every payload. Rollout: bump `FORMAT_VERSION` to
2, read the index if present, fall back to sequential scan if absent.

**(b) Segment-level checksum shortcut.** The footer already carries a `file_crc32`
over all file bytes before the sentinel. A recovery implementation could verify
`file_crc32` first — if it passes, the segment is clean and can be sealed without
per-record replay. Only fall back to record-by-record scan on checksum failure.
This turns the 99% case (clean shutdown) into a single-pass file hash instead
of a record-by-record iteration. The hash itself is sequential and potentially
SIMD-accelerated via `crc32fast`.

**Expected impact.** Zero effect on the write path or Sync latency. Recovery
speed only matters at startup (and only after an unclean shutdown). For a daemon
that is restarted frequently, or for large segments, this is worthwhile. For the
current 256 MiB default and a typical crash rate, it is a quality-of-life
improvement rather than a throughput lever.

The footer's `file_crc32` field (`format.rs` line 23) is already written and
verified in the existing code path — the fast-path shortcut for sub-idea (b)
does not require a format change, only a read-path change in `recovery.rs`.

**Measure-first.** Time the current `recover_segment` on a full 256 MiB segment
on beast. If it completes in < 500 ms, the ROI is minimal. If it takes seconds,
sub-idea (b)'s fast-path is a worthwhile one-day change.

**Effort/risk.** Sub-idea (b): low effort (10–20 lines in `recovery.rs`, no
format change, no feature gate). Sub-idea (a): moderate effort (format version
bump, backward-compatible reader, sealing code change, fuzz test update).
Risk for (b): if `file_crc32` passes but a record CRC fails (i.e. the file is
internally corrupt but has a matching whole-file checksum, which is extremely
unlikely with CRC32), recovery would seal a corrupt segment. This is already
the risk of relying on CRC32 for integrity; it is not a new risk introduced by
the shortcut.

---

### 2.6 Power-loss-protected NVMe: the storage hardware lever

**What it is.** This is not a code change — it is an infrastructure
recommendation. The 9× latency gap between beast's SATA SSD (~1.4 ms fdatasync)
and the Mac's NVMe (~150 µs F_BARRIERFSYNC) is almost entirely explained by:

1. The NVMe protocol's lower command overhead vs SATA.
2. The Mac NVMe having onboard capacitor-backed write cache (power-loss
   protection, PLP), which allows the drive firmware to acknowledge the write
   barrier without physically flushing to NAND. `F_BARRIERFSYNC` specifically
   exploits this.
3. Consumer SATA drives (Samsung 850 EVO) must flush their cache to NAND on
   every `fdatasync`, which takes ~1–2 ms.

A PLP-equipped NVMe drive (e.g. enterprise/datacenter U.2 or M.2 with PLP) on
Linux would allow `fdatasync` to complete in ~50–200 µs instead of ~1.4 ms —
matching or beating the Mac baseline. This is a 7–28× improvement in Sync
latency with **zero code changes**.

**Expected impact on beast.** Replacing the 250 GB SATA SSD with a PLP NVMe
would likely bring beast's Sync p50 from 1 394 µs to 100–200 µs. At that point
the software pipeline (currently 7–10 µs) is a larger fraction of total latency
and the higher-effort software optimisations in this document start to matter
more.

**Measure-first.** Check beast's NVMe drive (Samsung 970 EVO). Consumer 970 EVO
does NOT have hardware PLP, so `fdatasync` on Linux would still block ~300–500 µs
— better than SATA, but not as fast as a PLP drive. The ext4 filesystem is
currently on the SATA SSD only; mounting the NVMe and re-running the Phase 3
load suite would give a real data point.

**Effort/risk.** Zero code effort. Risk: the drive mount / partition setup on
beast is potentially disruptive (current NVMe is NTFS/Windows). Defer to a
separate environment setup task.

---

## 3. Ranked recommendations

| Rank | Candidate | Why |
|---|---|---|
| **1** | **2.2 `sync_file_range` preflushing** | Lowest effort, Linux-only but beast is the slow machine, directly attacks the blocking portion of fdatasync. Measure-first cost is tiny (10 lines + one benchmark run). |
| **2** | **2.3 `LazySync` tier** | No measurement required — the impact is definitionally predictable (7 µs ack instead of 1.4 ms). Small implementation. High value for latency-sensitive producers that can accept a 1 ms staleness window. |
| **3** | **2.4 mmap drain** | Independent of the fsync floor, improves the read/replay path, low-risk, naturally follows the `bytes::Bytes` work already done in Phase 3. Best tackled once the write-path work is settled. |
| **4** | **2.1 Cross-shard fsync coalescing** | High potential at `shard_count > 1` but high complexity and zero benefit in the default single-shard config. Defer until multi-shard production workloads are measured. |
| **5** | **2.5 Recovery fast-path (`file_crc32` shortcut)** | Low effort (sub-idea b), easily wins at startup for large segments after an unclean shutdown. Not a throughput lever but a robustness improvement. |
| **6** | **2.6 PLP NVMe hardware** | Highest absolute impact (7–28×) but requires infrastructure change, not code. Recommend as a parallel track, not a substitute for software work. |

---

## 4. Open questions

1. **Multi-shard fsync behaviour on beast.** Phase 3 benchmarks ran with
   `shard_count = 1`. Do multiple shards on a single SATA drive serialise
   fsyncs, or does the drive (or OS) reorder them? This determines whether
   candidate 2.1 is worth pursuing at all.

2. **`sync_file_range` semantics on ext4.** Does ext4 on Linux 6.17.0 actually
   start write-back immediately on `SYNC_FILE_RANGE_WRITE`, or does it batch
   internally anyway? The kernel documentation is ambiguous and the answer may
   be I/O scheduler-dependent.

3. **`LazySync` crash semantics product decision.** Is a "write is in the OS
   page cache but not yet on stable media" guarantee acceptable for any weir
   caller? This is a product/API contract question that must be decided before
   implementation.

4. **beast NVMe availability.** The 970 EVO is NTFS/Windows. Is it practical to
   add a Linux ext4 partition for performance benchmarking without disrupting the
   Windows setup? If so, the NVMe vs SATA SSD comparison (candidate 2.6) is the
   highest-value single measurement we could do.

5. **Drain as bottleneck under sustained Buffered load.** At 157k Buffered RPS
   the drain must consume segments as fast as they're sealed. Is the current
   `BufReader` drain keeping pace, or is there evidence of segment queue buildup?
   This determines whether candidate 2.4 (mmap drain) addresses an actual
   bottleneck or a theoretical one.

6. **Recovery time on full segments.** No measurement of `recover_segment` time
   exists for a 256 MiB segment on beast. Before investing in candidate 2.5
   (fast-path recovery), time a recovery run to establish whether it is a
   meaningful startup cost.

---

*Generated during Phase 3 → Phase 4 scoping pass. All candidates marked
"measure-first" should be validated with `tests/load.rs` + `bench-trace`
before implementation.*
