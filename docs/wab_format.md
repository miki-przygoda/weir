# WAB — Write-Ahead Buffer Format

## Overview

The WAB stores records as a sequence of segment files in per-shard directories under the WAB root. Each segment is an append-only binary file that is atomically renamed from `.wab` (active) to `.wab.sealed` when full or on shutdown. A `.wab.confirmed` sidecar is written by the drain after a segment is fully forwarded to the sink.

The daemon does not create the WAB root directory. The operator must create it before starting the daemon:

```
mkdir -p /path/to/wab && chmod 700 /path/to/wab
```

---

## Directory layout

```
/path/to/wab/
├── shard_00/
│   ├── seg_00000003.wab            ← active (being written)
│   ├── seg_00000002.wab.sealed     ← sealed, awaiting drain
│   └── seg_00000001.wab.confirmed  ← drained; the .wab.sealed file was DELETED
├── shard_01/
│   └── ...
├── dead_letter/
│   └── dl_00000001.wab.sealed      ← permanently-rejected records
└── quarantine/
    └── shard_00__seg_00000004.wab.sealed  ← corrupt segments moved here on recovery
```

Shard directories, the `dead_letter/` directory, and the `quarantine/` directory are created with mode `0o700`. Segment files (including `.confirmed` sidecars and dead-letter segments) are created with mode `0o600`. Permissions are set atomically at creation time via `OpenOptionsExt::mode` and `DirBuilderExt::mode` — there is no post-creation `chmod`.

**The `.wab.sealed` and `.wab.confirmed` files do *not* normally coexist.** On a healthy daemon the drain, after committing a sealed segment to the sink, writes the `.wab.confirmed` sidecar and then **deletes** the `.wab.sealed` segment (see [Post-drain steady state](#post-drain-steady-state)). So a `.confirmed` sidecar usually has **no surviving segment file** beside it. The tree above shows the *transient* states a single segment passes through (`seg_00000001` confirmed, `seg_00000002` sealed-but-not-yet-drained, `seg_00000003` active) — not three files for one segment.

### Reserved subdirectories

Two subdirectories of the WAB root are **not** shards and are skipped by recovery's shard scan:

- **`dead_letter/`** — records the sink **permanently** rejected. Files are named `dl_NNNNNNNN.wab.sealed` (zero-padded `u64` counter starting at 1), written by `DeadLetterWriter` (`crates/weir-server/src/drain/dead_letter.rs`). A crash mid-write can leave an orphan active partial `dl_NNNNNNNN.wab` (no `.sealed`), which recovery does not touch and the next `DeadLetterWriter::open` accounts for. Dead-letter files use the **same WEIR segment format** as the main WAB (header + records + sentinel + footer), so they are readable by the shared `weir-wab` `SegmentReader` with no separate parser — except their header carries the reserved shard id `0xFFFF` as a dead-letter marker. `weir-ctl dl requeue` reads them this way.
- **`quarantine/`** — segments recovery could not safely recover (bad magic, unknown version, CRC mismatch). The original file is **renamed** here with a *flattened* name that prefixes the source shard directory and joins it with the original file name by a **double underscore**: `<shard_name>__<file_name>`, e.g. `shard_00__seg_00000004.wab.sealed` (see `quarantine()` in `crates/weir-server/src/wab/recovery.rs`). The shard prefix prevents same-named segments from different shards clobbering each other once flattened into one directory; an exact-name collision appends `.1`, `.2`, … Quarantined files retain whatever extension they had (`.wab` or `.wab.sealed`).

---

## Segment file format (`FORMAT_VERSION = 1`)

### Header (24 bytes)

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────────────────
 0       4    magic       b"WEIR"
 4       1    version     FORMAT_VERSION (currently 1)
 5       1    reserved    0x00
 6       2    shard_id    u16 little-endian
 8       8    created_at  Unix timestamp, nanoseconds, i64 LE
16       8    reserved    0x00 (padding to 24 bytes)
```

### Records (variable, repeated)

```
Offset  Size       Field        Description
──────  ─────────  ───────────  ──────────────────────────────────
 0       4         payload_len  u32 little-endian
 4       4         crc32        CRC32 of the payload bytes, u32 LE
 8       payload_len  payload   raw payload bytes
```

A `payload_len` of `0` is the end-of-records sentinel (not a valid record).

`payload_len` is checked against `MAX_PAYLOAD_HARD_CAP` (16 MiB, defined in `weir-core`) before any heap allocation during both write and recovery replay.

### Sentinel (4 bytes)

`0x00 0x00 0x00 0x00` — marks the end of the record sequence.

### Footer (32 bytes)

```
Offset  Size  Field         Description
──────  ────  ────────────  ──────────────────────────────────────
 0       8    record_count  u64 little-endian
 8       8    data_bytes    u64 LE — total payload bytes (no overhead)
16       4    file_crc32    CRC32 of all bytes before sentinel, u32 LE
20       8    sealed_at     Unix timestamp, nanoseconds, i64 LE
28       4    reserved      0x00
```

`file_crc32` is accumulated by a running `crc32fast::Hasher` during writes, so no full-file re-read is needed at seal time. During crash recovery the running hasher is unavailable; the CRC is recomputed from scratch over the replayed records.

---

## `.confirmed` sidecar format (36 bytes)

Written by the drain after a segment's records have been forwarded to the sink. A segment with a valid `.confirmed` file is skipped on startup replay.

```
Offset  Size  Field         Description
──────  ────  ────────────  ──────────────────────────────────────
 0       4    magic         b"WCON"
 4       1    version       1
 5       3    reserved      0x00 — zero on write; reserved for future flags
 8       8    sealed_at     i64 LE — copied from segment footer
16       8    record_count  u64 LE — copied from segment footer
24       8    drained_at    i64 LE — Unix timestamp, nanoseconds
32       4    crc32         CRC32 of bytes [0..32], u32 LE
```

The `b"WCON"` magic is distinct from the segment magic `b"WEIR"` so a misplaced segment file cannot be parsed as a confirmation file.

The sidecar's name is derived from the sealed segment by swapping the extension: `seg_NNNNNNNN.wab.sealed` → `seg_NNNNNNNN.wab.confirmed` (`confirmed_path_for` in `crates/weir-wab/src/format.rs` strips `.wab.sealed` and appends `.wab.confirmed`).

Recovery behaviour:

- Magic, version (strict equality), and CRC32 all pass → skip replay of the corresponding sealed segment.
- Wrong magic or CRC mismatch → quarantine both the sidecar and the sealed segment; log with specific failure reason.
- Unknown version → quarantine both and log: "unknown confirmation format version N; cannot safely determine drain status — treating as unconfirmed would risk double-drain, quarantining instead."
- Missing `.confirmed` → not an error; segment was not drained before crash. Replay normally.

---

## Post-drain steady state

On a healthy daemon a sealed segment does **not** linger on disk. After the drain commits a segment to the sink, `confirm_and_delete` (`crates/weir-server/src/drain/confirmed.rs`) does two things, in order:

1. Writes the `.wab.confirmed` sidecar durably (content + directory entry fsynced).
2. **Deletes** the `.wab.sealed` segment file (`std::fs::remove_file`).

So in steady state a `.wab.confirmed` sidecar **frequently has no surviving `.wab.sealed` segment beside it** — the segment was deleted the moment delivery was confirmed. **A tool that derives a sealed path from a `.confirmed` sidecar and opens it will usually hit `ENOENT`.** Treat the `.confirmed` sidecar as a tombstone ("this segment was delivered and removed"), not as a pointer to a still-present segment.

The delete is deliberately second: if the `.confirmed` write fails, the segment is **preserved** (no `.confirmed`, segment still on disk) so recovery re-drains it on the next restart — a duplicate absorbed by the at-least-once + dedup contract, never a deleted-but-unconfirmed segment. The only window where a `.sealed` and its `.confirmed` coexist is a crash *between* step 1 and step 2; recovery resolves it by skipping replay (the `.confirmed` is valid) and the orphaned `.sealed` is left for the operator.

---

## Crash recovery algorithm

On startup, before accepting connections, the daemon runs recovery in the calling thread:

0. **Enumerate shards by directory scan, not by config.** Recovery `read_dir`s the WAB root, skips the reserved `dead_letter/` and `quarantine/` subdirectories, and treats every remaining directory as a shard (`recover_open_segments` in `crates/weir-server/src/wab/recovery.rs`; the sealed-segment replay scan in `crates/weir-server/src/wab/mod.rs` skips the same two subdirectories) — it does **not** reconstruct the shard set from the `shard_count` config, so segments from a previous run with a different `shard_count` are still recovered.
1. **Scan** each shard directory for `.wab` (active) files.
2. **For each active segment:**
   a. Validate header magic and format version. Bad magic or unknown version → quarantine.
   b. Replay records from the header boundary, verifying per-record CRC32.
   c. At the first corrupt or incomplete record, record the last valid offset.
   d. Truncate the file at the last valid offset.
   e. Write sentinel + footer (record count, data bytes, running file CRC, timestamp).
   f. `sync_all()` (full fsync including metadata).
   g. Atomically rename `.wab` → `.wab.sealed`.
3. **Replay sealed segments**: for each `.wab.sealed` without a valid `.wab.confirmed`, send the path to the drain channel.

Key invariant: only records up to the first corrupt entry are replayed. A torn write at crash time never silently corrupts the replay stream — the trailing corrupt record is truncated, not forwarded.

---

## Segment rotation

A segment rotates (`should_rotate() == true`) when `bytes_written >=` the configured `segment_max_bytes` (default 256 MiB via `SEGMENT_MAX_BYTES`; tunable from 4 KiB upward — see [configuration.md](operations/configuration.md)). Rotation happens inline in `ShardWriter::write_record`; the write that pushes the segment past the threshold also triggers the seal. The next write opens a new segment lazily.

---

## Naming convention

```
seg_{counter:08}.wab          active
seg_{counter:08}.wab.sealed   sealed, awaiting drain
seg_{counter:08}.wab.confirmed  drain-confirmed
```

`counter` is a monotonically increasing `u64`, zero-padded to 8 digits, starting at 1. On startup, `ShardWriter::scan_and_advance_counter` advances `next_counter` past the highest existing counter to prevent collisions with existing sealed segments.

---

## Security model

CRC32 detects **accidental corruption** (bit rot, partial writes, torn records). It does not detect malicious modification: a forged WAB segment with a valid CRC32 will be accepted by recovery.

The trust boundary is the WAB directory:

- The directory must be on **local storage** accessible only to the daemon process.
- Directory permissions are `0o700`; segment files are `0o600`.
- If the WAB is on a **network filesystem or shared storage**, the security model does not hold — another process could overwrite or forge segments.

This is an explicit assumption, not a weakness to be fixed. Operators who require tamper detection must add it at the storage layer (e.g. dm-verity, ZFS checksums with access control).
