# Bounding the WAB, and making preserved records reachable

**Status:** Design, approved. No implementation yet.
**Date:** 2026-08-12
**Branch:** `v2/main-line`.

---

## 1. Why this exists

Two holes, both found by a codebase sweep on 2026-08-12 and both verified
directly against the code rather than inferred.

### Hole 1 — the WAB is unbounded, and producers keep getting acked

There is **no WAB size cap and no backpressure**: greps for a cap knob across
`config/mod.rs` return nothing, and `NackReason` (`weir-core/src/nack.rs`) has
no backpressure variant. When the drain gives up, its own log line says what
happens:

> `"drain thread panicked too many times; giving up — delivery is stopped and
> the WAB will accumulate on disk until restart"` — `drain/mod.rs:241-245`

Producers continue receiving successful acks throughout. This is not a false
ack — every acked record really is on disk — but it is the nearest neighbour to
one: **the disk fills behind a green light.** A slow sink produces the same
shape more gradually.

### Hole 2 — quarantined records are preserved where nothing can reach them

Every "we preserved your acked records" path moves or copies them to
`<wab_dir>/quarantine/` rather than confirming and deleting. Nothing ships that
reads them back:

- `weir-ctl` contains the string "quarantine" **exactly once**, in a test
  comment (`weir-ctl/src/main.rs:1391`). The command set is `segments` and
  `dl list|drop|requeue`.
- Both quarantine metrics are per-process `Counter`s
  (`metrics/mod.rs:253, 260`) — after one restart the signal is gone. Compare
  `dead_letter_bytes_on_disk` and `wab_bytes_on_disk`, which are gauges.
  Quarantine has no on-disk gauge.

So the branch that exists *specifically* to preserve acked records when
recovery meets corruption terminates in a directory with no tooling, no
persistent metric, and no runbook.

### Related, and in scope because it is the same failure shape

`recover_shard_dir` catches `recover_segment` errors, logs at `error!`, and
continues (`wab/recovery.rs:128-130`) with **no counter** — verified: zero
matches for a recovery-failure metric in `metrics/mod.rs`. The daemon then
starts normally and the segment counter steps past the leftover. Acked records
sit unreachable, evidenced by one startup log line. On a read-only mount this
repeats every boot.

---

## 2. Out of scope

- **A new `NackReason::Backpressure` byte.** See §3.2 — deferred deliberately,
  not overlooked.
- **Making the ingest path observe drain state.** No such signal exists today,
  and §3.1's size cap subsumes the dead-drain case without building one.
- **Anything requiring the i9.** Every test in §7 runs on a laptop.

---

## 3. The WAB size cap

### 3.1 Where the check lives

A background task already computes `compute_wab_bytes_on_disk` every 5 s inside
`spawn_blocking` to feed the `weir_wab_bytes_on_disk` gauge
(`main.rs:566-584`). The value exists; nothing observes it for control.

The cap reuses it. That task additionally stores the byte count in a shared
`AtomicU64`, created in `main` and threaded into the connection layer. A
connection handler loads it (`Ordering::Relaxed`) before accepting a push.

**No new I/O and no new scan.** An atomic load on the ingest path is free, and
the directory walk stays where it already is — off the hot path, inside
`spawn_blocking`, at a cadence that is already deemed acceptable.

### 3.2 What it sends

Over the cap, the handler Nacks **`NackReason::InternalError` (0x06)** — the
existing byte, already used by `connection.rs` for a non-durable flusher
outcome.

This is deliberate and the reasoning is verified. `WeirClient::is_recoverable`
maps `Nack(InternalError) => true` but `UnknownNack(_) => false`
(`weir-client/src/unix.rs:108, 113`). A new `0x0A` byte would therefore make
every client built before it **tear down and reconnect**, because it assumes an
unknown Nack means the daemon closed the connection. Backpressure fires exactly
when the daemon is already under strain; rewarding that with a reconnect herd
from every producer makes the failure worse.

Reusing `InternalError` costs the structured `retry_after` hint and conflates
"WAB full" with "queue saturated" in the reason byte. §3.5's dedicated counter
buys that distinction back for operators. A `Backpressure` variant becomes
correct once a client that understands it exists — the reserved range
`0x0A..=0xFF` and `#[non_exhaustive]` keep that door open.

### 3.3 The frame must be fully consumed before Nacking

The check happens **after** the complete push frame has been read off the
socket, not before. Nacking mid-frame leaves unread payload bytes in the
stream, which the client would mis-read as a later reply — a stream desync, and
the client explicitly poisons its connection on exactly that (`unix.rs:299-307`).
The cost is that an over-cap push still crosses the wire; the benefit is that
the connection stays usable, which is the entire point of §3.2.

### 3.4 Soft cap, stated as such

The observed value is **up to 5 seconds stale**, so the WAB can overshoot by up
to 5 s of peak ingest. `wab_max_bytes` is a **soft high-water mark, not a hard
limit**, and the documentation must say so in those words. Operator guidance:
leave headroom of at least 5 s of peak ingest below actual free space.

Tracking bytes incrementally in the flusher would be exact. It is rejected:
that needs cross-shard aggregation on the hot path to tighten a bound operators
should be leaving margin on regardless.

### 3.5 Behaviour

- **All three durability tiers are rejected.** `Buffered` still writes to the
  WAB — it acks earlier, it does not skip the disk — so "we cannot store this"
  applies equally.
- **Low-water mark at `cap × 0.9`** to prevent flapping at the boundary. Not
  configurable; a second knob for this earns nothing.
- **New counter `weir_wab_cap_rejections_total`**, so cap rejections are
  distinguishable in metrics from queue-saturation `InternalError`s. This is
  what pays for reusing the reason byte.
- **Default `0` = disabled**, matching the `wab_segment_max_age_secs`
  convention. No existing deployment changes behaviour on upgrade.

---

## 4. The growth warning

Default-off protection only reaches operators who already know to ask for it.
Rather than pick a default cap that could Nack a deployment legitimately
buffering through a long outage — which is weir's job — the daemon warns at the
moment the hole is actually opening.

The 5 s task keeps a ring of the last 12 samples (60 s). It warns when **all
three** hold:

1. `wab_max_bytes` is unset (`0`), **and**
2. the samples show sustained growth across the window, **and**
3. the sink is not `Healthy` **or** the drain is not `Draining`

**Condition 3 is what makes the warning trustworthy.** Sustained WAB growth
under a healthy drain is a fast producer, and warning on that trains operators
to ignore the message. Growth while the sink is down is the hole opening. Both
`sink_health` and `drain_state` are `Family<_, Gauge>` (`metrics/mod.rs:246,
308`), so the task reads them back with no new plumbing.

Rate-limited to **once per 5 minutes** while the condition holds. The message
names the current bytes, the growth across the window, and `wab_max_bytes` as
the knob that would bound it.

---

## 5. Quarantine tooling

### 5.1 What a quarantined file actually contains

This drives the whole design, and getting it wrong makes the feature useless.

`copy_to_quarantine` copies the **whole original segment**, and its doc comment
says why: *"acked-durable records sitting after the corrupt one"*
(`wab/recovery.rs:575-586`). Separately, the valid **prefix** is truncated,
sealed in place, and delivered normally — which is why a mid-file corruption
counts as both `quarantined` and `sealed` (CHANGELOG, 1.3.0).

So a quarantined segment decomposes into three regions:

| Region | Status |
|---|---|
| Records before the corruption | **Already delivered** via the truncated prefix |
| The corrupt record | Unrecoverable |
| Records **after** the corruption | Preserved *only* here — the entire point |

**`SegmentReader` cannot reach the third region.** On a CRC mismatch it sets
`done = true` and returns `Err`, ending iteration — it is a sequential reader
and that contract is depended on by recovery and the drain.

Therefore mirroring `dl requeue`, which skips a corrupt segment **wholesale**,
would recover nothing of value: every quarantined segment is corrupt by
definition, so it would either skip them all or re-deliver only the prefix that
already reached the sink — manufacturing duplicates while recovering none of
the data quarantine exists to preserve.

### 5.2 `RecoveryReader` — skip-and-resync

A **new** reader type in `weir-wab`, alongside `SegmentReader` rather than a
mode flag on it. `SegmentReader`'s "stops at the first bad record" contract is
load-bearing for recovery and the drain and must not change.

`RecoveryReader` yields an item per record and **continues past failures**:

```rust
pub enum RecoveryItem {
    /// A record whose CRC verified.
    Record(Payload),
    /// A record that failed verification and was stepped over.
    Skipped { offset: u64, declared_len: u32, reason: String },
    /// Resync became impossible; iteration ends after this item.
    Desynced { offset: u64, reason: String },
}
```

Resync mechanics: a record at offset `O` declaring length `L` occupies
`[O, O + 8 + L)`, so the next candidate header is at `O + 8 + L`. On a CRC
failure the length field is usually still intact, which is what makes this work.

**Two guards, because a corrupted *length* field would otherwise walk the reader
into garbage:**

1. `O + 8 + L` must fall within the file.
2. The four bytes at that offset must be a plausible next length — non-zero (or
   the end-of-records sentinel) and within the segment's stored-record cap.

If either fails, emit `Desynced` and stop. Guessing past an implausible header
risks re-delivering fabricated records, which is worse than recovering nothing.

### 5.3 `weir-ctl quarantine list | inspect | requeue`

Quarantined files are named `<shard>__<segment>.wab.sealed`, with the origin
shard in the name.

- **`list`** — segments, bytes, origin shard. Mirrors `dl list`.
- **`inspect <segment>`** — per-record report from `RecoveryReader`: how many
  records verified, how many were skipped and at what offsets, and whether the
  reader desynced. This is the diagnostic that does not exist today at all.
- **`requeue`** — re-pushes the `Record` items through the daemon's socket and
  deletes the segment once they are all accepted. **Defaults to a dry run**,
  `--yes` to apply.

**Duplicate delivery is expected and must be documented, not discovered.** The
pre-corruption prefix was already delivered when recovery sealed it, so
requeueing re-sends it. That is within weir's at-least-once contract and sinks
dedupe on the batch token — but an operator deserves to be told before they run
it, so the dry run prints the count that will be re-sent, and `--yes` requires
the operator to have seen it.

A segment that yields `Desynced` before any `Record` is **not** deleted after a
requeue: there is nothing to confirm, and deleting it would destroy the only
copy of forensic evidence.

### 5.4 `weir_quarantine_bytes_on_disk`

A gauge, refreshed on the drain's idle poll that already calls
`dead_letter.rescan()` (`drain/mod.rs:486`). Closes the asymmetry in §1: today
quarantined bytes are invisible to monitoring across a restart, while
dead-letter and live-WAB bytes are not.

### 5.5 `weir_recovery_segments_failed_total`

A counter incremented in the arm at `wab/recovery.rs:128-130` that currently
only logs. Three lines, and it belongs here: the whole batch is *make
preserved-but-unreachable data visible and reachable*.

---

## 6. Configuration

| Knob | Type | Default | Range |
|---|---|---|---|
| `wab_max_bytes` | u64 (bytes) | `0` (disabled) | 0, or `wab_max_bytes / 10 * 9 > shard_count × wab_segment_max_bytes` |

The lower bound matters, and it is not merely "one segment". Each shard holds one
*open* active segment whose bytes count toward the cap, and an active segment
seals only on a write that crosses its rotation threshold — so
`shard_count × wab_segment_max_bytes` is a floor the daemon cannot drain away
while the cap is rejecting (no writes ⇒ no seals ⇒ no drain). The cap must clear
that floor by the hysteresis margin, or ingest wedges until restart with a
healthy sink. Validate it at load, like every other bounded knob, and name the
minimum in the error.

Plumbed through CLI / env / TOML including `BASE_SERVER_KEYS`, and documented in
`docs/operations/configuration.md` — which the config-doc drift guard in the
project-hygiene spec would enforce, if that lands first.

---

## 7. Testing

Everything here runs on a laptop; none of it needs the i9.

**The cap**
- Trips over the threshold and releases below the low-water mark, without
  flapping at the boundary.
- Rejects all three durability tiers.
- **The Nack keeps the connection usable** — asserted through a real
  `WeirClient`, not just a byte comparison. This is the entire justification for
  reusing `InternalError`, so it must be tested at the level the claim is made.
- `weir_wab_cap_rejections_total` moves, and queue-saturation rejections do not
  move it.
- A cap below `wab_segment_max_bytes` is rejected at config load.

**The warning**
- Fires on sustained growth with an unhealthy sink.
- Does **not** fire on sustained growth with a healthy drain (condition 3).
- Does not fire when the cap is set.
- Rate limiting holds across repeated polls.

**Quarantine**
- **The round trip that justifies the feature**: corrupt a segment mid-file, let
  recovery quarantine it, `requeue` it, and assert the records **after** the
  corruption point reach the sink. `SegmentReader` cannot do this (§5.1) — this
  test is what proves `RecoveryReader` can.
- `RecoveryReader` skips exactly the corrupt record and resumes at the next
  valid one, emitting one `Skipped` with the right offset.
- **A corrupted *length* field produces `Desynced`, not fabricated records.**
  Craft a segment whose length field is corrupted (not just its payload) and
  assert the reader stops rather than resyncing into garbage. This is the guard
  in §5.2 and it is the difference between recovering data and inventing it.
- A segment that desyncs before any valid record is **not deleted** by requeue.
- `list` on an empty/absent quarantine dir is not an error.
- Dry run by default and prints the count that will be re-sent, including the
  already-delivered prefix; `--yes` applies.
- `weir_quarantine_bytes_on_disk` tracks add and remove.

**Recovery counter**
- Increments when `recover_segment` fails. Reuse the failure-injection approach
  from `recovery_mid_file_corruption_fails_closed_when_quarantine_copy_fails`
  (`recovery.rs:1108`) rather than inventing a new one.

---

## 8. Risks

| Risk | Mitigation |
|---|---|
| The 5 s staleness lets the WAB overshoot onto a full disk | Documented as a soft high-water mark with explicit headroom guidance (§3.4); the growth warning (§4) fires long before the cap would |
| Reusing `InternalError` hides cap rejections from operators | `weir_wab_cap_rejections_total` (§3.5) distinguishes them; the docs state the conflation plainly |
| The growth warning becomes noise and gets ignored | Condition 3 plus 5-minute rate limiting; a test asserts it does **not** fire under a healthy drain |
| `quarantine requeue` re-injects genuinely corrupt records | `RecoveryReader` CRC-checks every record and only ever yields `Record` for one that verified; a failure becomes `Skipped`, never a push |
| A corrupted length field makes the reader resync into garbage and re-deliver fabricated records | Two guards in §5.2 (in-file bound + plausible next header) and a dedicated test; on failure it emits `Desynced` and stops rather than guessing |
| Requeue re-delivers the already-delivered prefix | Inherent — the prefix and the tail live in one file. Within the at-least-once contract and deduped by the batch token; the dry run states the count up front so it is a decision, not a surprise |
| Cap rejection desyncs the connection | §3.3 — the frame is fully consumed before the Nack, and a real-client test covers it |
