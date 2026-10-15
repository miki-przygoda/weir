# Recovering quarantined records

`<wab_dir>/quarantine/` holds forensic copies of WAB segments that crash
recovery or the drain could not fully trust. This page is the procedure: how to
notice, how to triage, and how to recover what is still readable. For the
on-disk layout and naming rules, see [`wab_format.md`](../wab_format.md#reserved-subdirectories).

**The one fact that motivates all of this:** when recovery meets **mid-file**
corruption in an active segment, it truncates and seals the valid prefix
(delivered normally) but copies the *whole original file* to `quarantine/` —
because acked-durable records may sit *after* the corrupt one, and those
records exist nowhere else. `weir-ctl quarantine` is how you get them back.

---

## How to notice

| Signal | What it tells you |
|---|---|
| `weir_quarantine_bytes_on_disk` (gauge) | **The complete signal.** Sums every regular file's size under `quarantine/`, regardless of *why* it landed there. Any sustained increase means something new was quarantined. Survives a restart, unlike the counters below. |
| `weir_recovery_segments_quarantined_total` (counter) | One per segment moved or copied into `quarantine/`, whoever did it: crash recovery (a header it could not parse, or mid-file corruption with a copied tail), the drain (see [What lands here](#what-lands-here-and-what-is-parked-instead)), or a `.confirmed` sidecar that fails verification (the segment is counted once; its sidecar goes along uncounted). **Per-process — resets on restart.** |
| `weir_recovery_segments_failed_total` (counter) | Crash recovery of an active segment failed **and left it at its original path**, so nothing will reach it. A quarantined segment is never counted here. Non-zero means acked records **may** be unreachable — check the startup logs for the path and the cause. **Per-process — resets on restart.** |
| `weir_drain_segments_disposition_pending{kind="quarantine"}` (gauge) | Segments the drain decided to quarantine but could not move (`ENOSPC`, `EACCES`, a regular file named `quarantine`). They stay in their shard directory, are **never re-delivered**, and the move is retried on every health poll. `weir_drain_quarantine_failures_total` counts the failed moves. Non-zero means `quarantine/` itself needs attention. |
| `weir_drain_segments_parked{reason}` (gauge) | Segments set aside **instead of** quarantined; they are not in `quarantine/`. See below. |

**Bottom line:** alert on the gauge for "notice that anything happened at
all." Use the counters, together with the logs (`quarantining WAB segment`
names the path, destination and reason), to narrow down why.

## What lands here, and what is parked instead

The drain and the sealed-segment scan quarantine a **sealed** segment only when
its bytes are bad, which no retry changes, or have stayed unreadable past the
escalation limit:

| Condition | When | Delivered before the move |
|---|---|---|
| Corrupt header: bad magic, version 0, a v1 header with its reserved byte set, a file too short for a header | first attempt, no retry | nothing |
| A CRC mismatch, an oversized length or a record cut short mid-segment | when it is reached | the readable prefix |
| The footer is cut short, or its record count disagrees with the records read (a truncated tail) | after the last record | every record read |
| A media-class read error (`EIO`, `ENXIO`, `ESTALE`, `EBADMSG`, anything unrecognised) that persists for three strand cycles and at least 10 minutes | at the escalation limit | the readable prefix, exactly once per run |
| A `.confirmed` sidecar that fails to parse (wrong length, bad magic or CRC, unknown version) — for example one torn by the in-place write of 4.0.1 and earlier; sidecars are now written atomically, so a crash or full disk no longer tears one | the sealed-segment scan, at startup or in the drain's rescan | everything (a sidecar is written only after delivery) |

A **transient** read error is not corruption: the segment is retried and, while
the daemon runs, resumes past the records already delivered; it never goes to
`quarantine/` on a blip.

Some segments are **parked** instead: left in place as `seg_*.wab.sealed` in
their shard directory, not retried for the rest of the run, and replayed from
record 0 on the next start. `weir-ctl quarantine` does not see them; the
`weir_drain_segments_parked{reason}` gauge and an ERROR log naming the path do.

| `reason` | Cause | What to do, then restart |
|---|---|---|
| `unsupported_version` | Written by a newer weir (a format version above what this build reads, or an unknown v2 flag bit). Not corrupt. | Upgrade weir; a newer binary delivers it. |
| `local_read_failed` | A permission-class read error (`EACCES`, `EPERM`, `EISDIR`, `ENOTDIR`, `ELOOP`) that persisted for three strand cycles and at least 10 minutes | Fix the file's permissions or type. |
| `drain_panic` | The drain panicked twice while delivering this segment | Find the panic in the log; usually a sink bug triggered by one record. |

Parking a `drain_panic` segment keeps one poison segment from spending the
drain's whole respawn budget (10 panics per process, never refilled; the 10th
stops delivery) and stopping delivery for every segment. Five distinct poison segments, two panics each, can
still exhaust it.

Crash recovery is unchanged: an **active** segment with a header it cannot
parse (newer format included) is quarantined, and mid-file corruption is copied
here while the valid prefix is sealed and delivered.

---

## How to triage

1. **List what's there:**
   ```
   weir-ctl quarantine list --wab-dir /var/lib/weir/wab
   ```
   Prints every quarantined segment with its size and origin shard. A
   segment's *presence* here does not say how much of it is recoverable —
   that is `inspect`'s job.

2. **Inspect each one:**
   ```
   weir-ctl quarantine inspect --wab-dir /var/lib/weir/wab <segment>
   ```
   Reports how many records verify (safe to re-deliver), how many failed
   verification and at what byte offsets, and whether the reader **desynced**
   — lost track of where the next record begins.

   **Read a clean end correctly.** `inspect` reporting no desync means *every
   byte in the segment is accounted for* — it does **not** mean *every record
   was recovered*. A length field corrupted to a plausible-but-wrong value can
   tile exactly to the end of the file, quietly swallowing intact records
   inside its declared byte range without ever losing the framing. The
   skipped-record list is what verification actually caught; a clean end
   alone is not license to delete the segment.

   **Read a desync correctly.** If `inspect` reports a desync, records past
   that offset are **not recoverable by this tool or any other** — the
   forensic copy in `quarantine/` is the only place they could still exist.
   Do not discard the segment on the strength of a partial recovery; the
   unread bytes are still there.

---

## How to recover

```
weir-ctl quarantine requeue --wab-dir /var/lib/weir/wab               # dry run (default)
weir-ctl quarantine requeue --wab-dir /var/lib/weir/wab --yes          # actually requeue
```

`requeue` re-submits every recoverable record from every quarantined segment
back through the daemon's socket, then deletes a segment once **all** of its
recoverable records have been accepted — never before. Always dry-run first;
`--yes` is destructive.

**The dry run will tell you it re-sends the already-delivered prefix — believe
it.** Recovery delivered the valid prefix normally when it sealed the
segment, and that same prefix lives in the file `quarantine/` preserved. A
`requeue` run therefore **will** re-send records the sink already received.
This is within weir's at-least-once contract, but **a dedup-capable sink will
not filter these duplicates for you**: `DedupToken` is derived from a batch's
contents *and its boundaries*, and a requeue re-batches the records, so the
sink sees genuinely different batches and accepts both. Do not assume
dedup saves you here, in any phrasing — plan for the duplicates at the
consumer.

The same holds for segments the drain quarantined, in proportion to what it
delivered first ([What lands here](#what-lands-here-and-what-is-parked-instead)):
the readable prefix after mid-segment corruption or a persistent media error,
and **every record read** after a footer mismatch or a bad `.confirmed` sidecar,
so a requeue of those is all duplicates. A segment quarantined for a
corrupt header was never delivered, and cannot be requeued either: its header
does not parse for `RecoveryReader` any more than for the drain.

**Two judgment calls baked into `--yes`, so you don't have to reverse-engineer
them from behavior:**

- **`--yes` alone deletes a segment that had skipped (individually corrupt)
  records**, once every *recoverable* record from it has been accepted. There
  is no second flag to gate this behind — every quarantined segment is
  corrupt by definition (that's why it's here), so requiring an extra flag
  for "a corrupt segment" would mean the ordinary case never deletes
  anything. The skipped byte ranges are printed by the dry run and again by
  the real run, before and after the fact.
- **A segment that desyncs is never deleted**, however many records were
  recovered from it first. The recoverable prefix *is* still requeued; only
  the delete is withheld, because bytes past the desync point were never
  read and this tool cannot rule out data still sitting in them.

**Exit codes, for scripting.** A skip alone — the ordinary outcome for a
successfully-recovered-and-deleted segment — does **not** make the run exit
non-zero; treating "some record somewhere was skipped" as a failure would
make a normal, fully successful requeue indistinguishable from a real one.
The run **does** exit non-zero if, after processing every reachable segment,
any of the following happened: a segment **desynced** and was left in place,
a segment yielded **no recoverable records at all** and was left in place, a
segment **could not be opened/parsed** at all (an operator-dropped file, or a
header too damaged even for `RecoveryReader`), or a segment was **fully
recovered but its file could not be deleted** afterward. Any of those means
manual follow-up is still needed even though the run "completed" — check
`weir-ctl quarantine list` again afterward to see what's left.

**`--durability buffered` is refused**, unlike `dl requeue` (which allows
it). A `Buffered` ack means only "entered the daemon's in-memory queue," not
durably written — and this command deletes the quarantined segment, often
the *only* surviving copy of the record, once every push is accepted. A
crash between that ack and the next fsync would destroy the record for good
with nothing left to recover it from. Dead-lettered records don't carry that
same stake (dead-letter usually isn't the last copy of anything), which is
why `dl requeue` documents the risk instead of refusing it. Use `durable`
(the canonical spelling; `sync` and `batched` are still accepted as legacy
aliases, and remain the flag's literal default) for `quarantine requeue`.

---

## Naming, for reading the directory by hand

Quarantine entries are named `{shard_name}__{original_file_name}`:

- Crash recovery quarantines **active** segments, so its copies keep the
  `.wab` extension: `shard_00__seg_00000004.wab`.
- The drain, and the sealed-segment scan at startup and in the drain's
  rescan, quarantine **sealed** segments (a `.confirmed` sidecar that fails
  verification takes its sealed segment down with it), so those end
  `.wab.sealed`: `shard_00__seg_00000004.wab.sealed`.
- A name collision (the same shard+counter recurring across a restart)
  appends `.1`, `.2`, … up to `.10000` to either form.

**The directory can also hold `.wab.confirmed` sidecars** — when a bad
`.confirmed` file is what triggered the quarantine, both it and its sealed
segment are moved in, e.g. `shard_00__seg_00000004.wab.confirmed` alongside
`shard_00__seg_00000004.wab.sealed`. `weir-ctl quarantine` **deliberately
ignores** these: there is nothing to recover from a confirmation-metadata
file, and the sealed segment beside it is the one the tooling surfaces. If
you list the directory by hand and see a `.wab.confirmed` file `quarantine
list` never mentions, that is expected, not a bug in the tool.
