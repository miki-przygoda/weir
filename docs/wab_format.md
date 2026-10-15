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
├── .weir.lock                      ← ownership lock + holder record (never deleted)
├── shard_00/
│   ├── seg_00000003.wab            ← active (being written)
│   ├── seg_00000002.wab.sealed     ← sealed, awaiting drain
│   └── seg_00000001.wab.confirmed  ← drained; the .wab.sealed file was DELETED
├── shard_01/
│   └── ...
├── dead_letter/
│   └── dl_00000001.wab.sealed      ← permanently-rejected records
└── quarantine/
    ├── shard_00__seg_00000004.wab         ← mid-file-corrupt ACTIVE segment (crash recovery)
    └── shard_01__seg_00000002.wab.sealed  ← corrupt SEALED segment (the drain, or a rejected .confirmed)
```

Shard directories, the `dead_letter/` directory, and the `quarantine/` directory are created with mode `0o700`. Segment files (including `.confirmed` sidecars, their transient `.confirmed.tmp`, and dead-letter segments) and the `.weir.lock` file are created with mode `0o600`. Permissions are set atomically at creation time via `OpenOptionsExt::mode` and `DirBuilderExt::mode` — there is no post-creation `chmod`.

**The `.wab.sealed` and `.wab.confirmed` files do *not* normally coexist.** On a healthy daemon the drain, after committing a sealed segment to the sink, writes the `.wab.confirmed` sidecar and then **deletes** the `.wab.sealed` segment (see [Post-drain steady state](#post-drain-steady-state)). So a `.confirmed` sidecar usually has **no surviving segment file** beside it. The tree above shows the *transient* states a single segment passes through (`seg_00000001` confirmed, `seg_00000002` sealed-but-not-yet-drained, `seg_00000003` active) — not three files for one segment.

### Reserved subdirectories

Two subdirectories of the WAB root are **not** shards and are skipped by recovery's shard scan (the root's `.weir.lock` is a regular file, so no directory walker treats it as a shard either; see [The `wab_dir` lock file](#the-wab_dir-lock-file)):

- **`dead_letter/`** — records the sink **permanently** rejected. **"Permanent" means the sink returned a non-retryable rejection** (e.g. an HTTP sink replying 4xx other than 408/429) — *not* that the sink was unreachable. A **transient** failure (sink down/unreachable, 5xx, timeout, transport error) is **not** dead-lettered: the whole segment is retried and, once its retry budget is exhausted, left on disk as **stranded** (it stays in its shard dir as `seg_*.wab.sealed`, surfaces via `weir_drain_segments_stranded_total`, and auto-resumes once its per-segment cool-down has passed and the sink recovers, or the drain is idle, skipping the records already delivered in this run). So a recovery tool looking for un-delivered data after a sink *outage* should look at stranded sealed segments in the shard dirs, not `dead_letter/`. Files are named `dl_NNNNNNNN.wab.sealed` (zero-padded `u64` counter starting at 1), written by `DeadLetterWriter` (`crates/weir-server/src/drain/dead_letter.rs`). A crash mid-write can leave an orphan active partial `dl_NNNNNNNN.wab` (no `.sealed`), which recovery does not touch and the next `DeadLetterWriter::open` accounts for. Dead-letter files use the **same WEIR segment format** as the main WAB (header + records + sentinel + footer), so they are readable by the shared `weir-wab` `SegmentReader` with no separate parser — except their header carries the reserved shard id `0xFFFF` as a dead-letter marker. `weir-ctl dl requeue` reads them this way.
- **`quarantine/`** — segments crash recovery could not safely recover (bad magic, unknown version, CRC mismatch); sealed segments the drain found corrupt (a corrupt header, on the first attempt; a CRC mismatch or truncated record mid-segment, or a footer that is cut short or disagrees with the record count, after delivering the readable prefix; a media-class read error such as `EIO` that persisted for three strand cycles); plus a `.confirmed` sidecar that fails its own verification during sealed-segment replay. A transient read error (anything other than `InvalidData` / `UnexpectedEof`) is not corruption and is retried, not quarantined. The original file is **renamed** (mid-file-corrupt active segments are *copied*, so the valid prefix can still be truncated+sealed in place) here with a *flattened* name that prefixes the source shard directory and joins it with the original file name by a **double underscore**: `<shard_name>__<file_name>`, e.g. `shard_00__seg_00000004.wab.sealed` (see `quarantine()` / `copy_to_quarantine()` in `crates/weir-server/src/wab/recovery.rs`). The shard prefix prevents same-named segments from different shards clobbering each other once flattened into one directory; an exact-name collision appends `.1`, `.2`, … Quarantined files retain whatever extension they had: `.wab` when crash recovery preserved an **active** segment (the mid-file-corruption case — the one where acked records sit after the corruption), `.wab.sealed` when the drain (or a rejected `.confirmed` sidecar) preserved a **sealed** one, or `.wab.confirmed` for a sidecar that failed verification — the corresponding `.wab.sealed` copy sits beside it.

  **Reading a quarantined segment.** A quarantined segment preserves the corrupt record **and** the (valid) records *after* it — the whole file is moved (or copied) untouched, nothing is truncated. `weir-wab`'s `SegmentReader` stops at the **first** CRC error by contract: it yields every good record up to the corruption, then a single `Err` (for a CRC mismatch, `io::ErrorKind::InvalidData`), and every subsequent call returns `None` (see the iteration contract in `crates/weir-wab/src/lib.rs`) — so a plain `SegmentReader` recovers only the prefix before the bad record, by design (recovery and the drain depend on that stop-at-first-error contract).

  To reach the records *after* the corruption, use `weir_wab::RecoveryReader` instead — a separate, forensic iterator built for exactly this file. It continues past a corrupt record (resync is free: `SegmentReader::next` already reads the full payload before verifying the CRC, so a failure leaves the reader positioned at the next record's boundary with no seek needed) and never fabricates data: an implausible declared length, or a run of consecutive verification failures, ends iteration with `Desynced` rather than guessing. `weir-ctl quarantine list | inspect | requeue` is built on it and is the operator-facing way to do this — see [the quarantine recovery runbook](operations/quarantine-recovery.md) for the triage procedure, including how to read `RecoveryReader`'s "clean end" (every byte accounted for, *not* every record recovered) and why `requeue` re-sends the already-delivered prefix.

  A sealed segment that the drain cannot read because its header names a **newer format** than this build reads (a version above 2, or a v2 flags bit it does not know) is **not** quarantined: it is *parked* — left in place in its shard directory, not retried for the rest of the run, counted in `weir_drain_segments_parked{reason="unsupported_version"}`, and replayed on the next start, where a newer binary delivers it. That keeps a downgrade from pushing every newer-format segment into manual recovery. Crash recovery of an **active** segment with such a header is unchanged: it is quarantined.

### The `wab_dir` lock file

`<wab_dir>/.weir.lock` makes the daemon the only writer of its `wab_dir`. Two daemons on one directory corrupt each other: the second one's crash recovery seals the first one's **active** segment, its drain delivers and deletes it, and the first daemon then acks `Durable` records into an unlinked inode. The lock is what stops that.

- **Taken first.** At startup, after the config is loaded and before anything reads the WAB (crash recovery, the replay, the drain) or any other refuse-to-start check runs, the daemon opens the file (`O_NOFOLLOW`, created mode `0600`) and calls `flock(LOCK_EX | LOCK_NB)`. It holds the lock for the life of the process and never releases it explicitly; the kernel drops it at exit, after the flusher and drain threads have finished. Contention refuses startup with `wab_dir <wab_dir> is locked by another weir-server (…)`; what happens on a filesystem that cannot lock is the [`wab_dir_lock`](operations/configuration.md#wab_dir_lock) setting.
- **A symlink, a directory or any other non-regular file** at the path refuses startup. So does a regular file with **more than one hard link** (taking the lock truncates and rewrites the file, which would overwrite every other name for it, so it is refused before the `flock` and nothing is written) and a file **owned by another uid** (that user could loosen its mode and hold the lock while the daemon is down). The owner refusal names the owner and the fix: run as that uid, or `chown <this daemon's uid> <wab_dir>/.weir.lock` as root while no daemon runs on the directory. A `sudo weir-server` run on a weir-owned `wab_dir` leaves such a file behind. A file of ours with a mode other than `0600` is set to `0600` once the lock is held, with a WARN (`wab_dir lock file was not mode 0600; set it to 0600`).
- **The holder record is untrusted** when read back: only its first 4 KiB is parsed, control and bidi formatting characters are stripped from every value, and a value longer than 256 bytes is dropped rather than echoed into the refusal message or the exit-3 log line.
- **Never unlinked.** weir does not delete the file at exit (unlink-on-exit opens a two-inode race between starters), so it outlives the daemon. A leftover file is harmless: the lock belongs to the open file, not to the file's existence, and the next daemon locks it and overwrites the record. **Do not delete it while a daemon runs**: the daemon checks once a second, by comparing the `(dev, ino)` at the path with its held descriptor and re-issuing the `flock` on that descriptor. If the file was deleted or replaced and nobody else holds the new one, it re-creates and re-locks it, logs an ERROR and increments `weir_wab_dir_lock_integrity_failures_total`. If another instance holds it, the daemon NACKs every new push, logs `CRITICAL: wab_dir ownership lost` and exits with status 3 without sealing. A second daemon started in the gap (up to about a second) can still recover this daemon's live segments; detection cannot close that window.
- **Opened once.** The daemon never re-opens the file in-process, except to re-create it after proving the path no longer names the held file. Where `flock` is emulated with POSIX locks (Linux on NFS), closing *any* descriptor to the file drops the lock. The daemon keeps its own holder record in memory for anything that wants to show it.
- **Ignored by every walker.** It is a regular file at the root, not a directory, and has no segment suffix: crash recovery, the sealed-segment scan, `weir_wab_bytes_on_disk`, the segment-counter scan and `weir-ctl` all skip it. `weir-ctl` never calls `flock` on it, not even as a probe (a momentary probe could make a starting daemon fail with "locked by"), so the offline tools run beside a live daemon. A release from before the lock existed (4.0.1 and earlier) ignores the file entirely, so a downgrade works but runs unguarded.

**Holder record.** Written only after `flock` succeeds (truncate, then write at offset 0, no fsync: it is informational), so a refused contender never touches the owner's record. Plain `key=value` lines, no serde:

```
pid=1234
host=weir-0
started_unix=1791100800
boot_id=0f3c…           # Linux only (/proc/sys/kernel/random/boot_id); omitted elsewhere
version=4.0.1
socket_path=/run/weir/weir.sock
metrics=127.0.0.1:9185
```

`host` is `gethostname(2)`, the pod name in Kubernetes; `metrics` is `<metrics_bind>:<metrics_port>`. A newline inside a value is replaced by a space, so a value cannot forge a key. Readers parse it best effort: unknown keys are ignored, and a file without a parseable `pid=` line reads as no record (the refusal then says `its holder record could not be read`). A pid means nothing across pid namespaces; `host`, `started_unix` and `boot_id` identify the holder. The record names the **recorded** owner, not a live one; whether that daemon is still running is a question for its socket or metrics endpoint.

---

## Segment file format (versions 1 and 2)

### Header (24 bytes)

```
Offset  Size  Field       Description
──────  ────  ──────────  ──────────────────────────────────────
 0       4    magic       b"WEIR"
 4       1    version     1 or 2
 5       1    flags       v1: MUST be 0x00
                          v2: bit 0 = ZSTD; bits 1-7 reserved, MUST be 0
 6       2    shard_id    u16 little-endian
 8       8    created_at  Unix timestamp, nanoseconds, i64 LE
16       8    reserved    0x00 (padding to 24 bytes)
```

### Format v2 — what changed, and what did not

v2 differs from v1 in **one byte**. Byte `[5]`, reserved and zero-on-write since
v1, is now a flags byte whose bit 0 means *every record in this segment is
stored as a complete zstd frame*. The record framing, the end-of-records
sentinel, the footer, and the `.confirmed` sidecar are **byte-for-byte
identical** between the two versions.

**The per-record CRC covers the STORED bytes** — the zstd frame, in a compressed
segment — not the logical payload. That is deliberate and it is what keeps the
change small: crash recovery's torn-tail scan, `verify_sealed_segment`'s
whole-file CRC, and the footer's `record_count` / `data_bytes` cross-check all
work without decompressing anything. Decompression happens in exactly one place,
`SegmentReader::next`, after the CRC has already passed.

The footer's `data_bytes` is likewise **stored** bytes, so it is the
post-compression size in a v2 ZSTD segment. For logical byte totals, read the
`weir_wab_record_logical_bytes_total` metric.

### Which version gets written

A segment is written as **v1 whenever compression is off**, and v2 only when it
is on. With `wab_compression = "none"` — the default — a weir 2.0 daemon
produces segments byte-identical to what 1.x wrote.

That makes upgrade-then-rollback a non-event at the default setting. **Enabling
compression is a one-way door:** a 1.x reader rejects a v2 header outright
(`InvalidData`), which the drain treats as "preserve for retry" and recovery
quarantines — the data is stranded and loud, never misread and silent — but it
stays stranded until a 2.x daemon reads it.

### Reader rules

| Header | Behaviour |
|---|---|
| v1, `flags == 0` | Accept, no compression |
| v1, `flags != 0` | **Reject** — v1 documented the byte as zero, so a set bit is corruption |
| v2, only known flag bits | Accept |
| v2, any unknown flag bit | **Reject** — written by a newer weir; reading it would misinterpret the records |
| version 0, or > 2 | **Reject** — unknown format version |

Rejecting an unknown flag bit rather than ignoring it is the point: a future
weir that adds bit 1 (say, encryption) must not have its segments silently read
as plaintext by this build.

What the daemon does with a rejected header depends on which rule rejected it.
When the drain meets one on a **sealed** segment, it re-reads the header to tell
the two cases apart. "Written by a newer weir" (the right magic with a version
above 2, or a v2 header with an unknown flag bit) **parks** the segment as
`unsupported_version`: bytes untouched, replayed on the next start. Everything
else (bad magic, version 0, a v1 flags byte that is not zero, a file too short
for a header) is corruption, and the segment is **quarantined on the first
attempt**, not retried. Crash recovery still quarantines an **active** segment
for any rejected header.

### Record size caps under compression

A compressed record's **stored** length can legitimately exceed
`MAX_PAYLOAD_HARD_CAP` (16 MiB), because zstd's worst case expands slightly.
So a v2 ZSTD segment accepts stored lengths up to
`compress_bound(MAX_PAYLOAD_HARD_CAP)` = `n + n/255 + 16`, while v1 keeps the
original cap unchanged. The *plaintext* is still capped at 16 MiB, enforced on
the decompression output — which doubles as the decompression-bomb guard, and is
an exact bound rather than a guess because the real cap is known.

Measured, on genuinely random 16 MiB input: the stored form was **394 bytes
larger** than the input, against an allowance of 65,809 bytes.

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
 8       8    data_bytes    u64 LE — total STORED payload bytes, post-compression,
                            excluding per-record overhead (see below)
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

The sidecar write is atomic: the bytes go to a transient `seg_<N>.wab.confirmed.tmp` (mode 0600), which is fsynced, renamed to `seg_<N>.wab.confirmed`, and the shard directory is then fsynced. The final name therefore only ever holds complete, durable contents. A crash or a full disk mid-write leaves at worst a stale `.confirmed.tmp`, which no reader treats as a segment or a sidecar (`weir-ctl` included) and which the next sealed-segment scan deletes.

The delete is deliberately second: if the `.confirmed` write fails, the segment is **preserved** (no `.confirmed`, segment still on disk), never a deleted-but-unconfirmed segment. While the daemon runs, it retries only the failed step (the sidecar, or the delete after a durable sidecar) on every health poll, and never re-delivers the segment; `weir_drain_confirm_failures_total{stage}` counts the failures and `weir_drain_segments_disposition_pending{kind="confirm"}` the segments waiting. A restart before the retry succeeds re-drains the segment once — a duplicate absorbed by the at-least-once + dedup contract.

A `.sealed` and its valid `.confirmed` coexist only after a crash (or a failed delete) *between* step 1 and step 2: an **orphan**. It is never replayed. When the daemon holds the `wab_dir` lock, the startup scan and the drain's stranded-segment rescan delete it: the sidecar and the shard directory are fsynced first, so the tombstone is durable before the segment goes, and the `.confirmed` tombstone stays. Without the lock (`wab_dir_lock = "best-effort"` on a filesystem that cannot lock) orphans are kept, because another instance may still be confirming them; they are skipped and count toward `weir_wab_bytes_on_disk` until removed.

A `.confirmed` that is present but fails to parse (wrong length, bad magic or CRC, unknown version) is still quarantined together with its segment. The atomic write means this version never produces a torn sidecar; one torn by the in-place write of 4.0.x and earlier is quarantined once on upgrade, as before.

---

## Crash recovery algorithm

On startup, before accepting connections, the daemon runs recovery in the calling thread. It runs only after the daemon has taken the [`wab_dir` lock](#the-wab_dir-lock-file) and passed its other refuse-to-start checks, so a second daemon never gets this far on a directory another one owns (unless `wab_dir_lock = "best-effort"` let it start without a lock on a filesystem that cannot take one).

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
3. **Replay sealed segments**: for each `.wab.sealed` without a valid `.wab.confirmed`, send the path to the drain channel. The same scan deletes stale `*.wab.confirmed.tmp` files and, when the lock is held, orphaned sealed segments (see [Post-drain steady state](#post-drain-steady-state)). It is fail-fast: a shard directory it cannot list stops startup, because a silently skipped segment would be acked data silently dropped. (The drain's later rescans skip such a shard with a WARN and retry it on the next poll.)

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
seg_{counter:08}.wab.confirmed.tmp  transient: a sidecar being written (see Post-drain steady state)
```

`counter` is a monotonically increasing `u64`, zero-padded to 8 digits, starting at 1. On startup, `ShardWriter::scan_and_advance_counter` advances `next_counter` past the highest existing counter to prevent collisions with existing sealed segments. It parses the `seg_<digits>` prefix of every name, so a leftover `.confirmed.tmp` can only advance the counter, never rewind it.

---

## Security model

CRC32 detects **accidental corruption** (bit rot, partial writes, torn records). It does not detect malicious modification: a forged WAB segment with a valid CRC32 will be accepted by recovery.

The trust boundary is the WAB directory:

- The directory must be on **local storage** accessible only to the daemon process.
- Directory permissions are `0o700`; segment files are `0o600`.
- If the WAB is on a **network filesystem or shared storage**, the security model does not hold — another process could overwrite or forge segments. The [`wab_dir` lock](#the-wab_dir-lock-file) is not a supported guard there either: NFS can grant it with weaker semantics (with `nolock` the lock is local to one client, silently; with lockd or NFSv4 a lost lease can drop it between the once-a-second checks). See [Platform support](platform-support.md#untested-and-unsupported).

This is an explicit assumption, not a weakness to be fixed. Operators who require tamper detection must add it at the storage layer (e.g. dm-verity, ZFS checksums with access control).
