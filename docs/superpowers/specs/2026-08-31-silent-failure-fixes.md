# 2.0.1 — the failures that do not announce themselves

## Problem

weir's central claim is *"an ack is never a false ack."* A five-agent audit of
the 2.0.0 tree, followed by an adversarial verification pass that was told to
**refute** each finding, produced four confirmed paths where weir either loses
acknowledged data or stops delivering — **and says nothing while it happens.**

Every finding below survived a verifier whose brief was to break it. The ones
that did not survive are recorded under "Refuted or downgraded" so the record is
honest about the hit rate.

## What is broken

### The silent ones destroy data

**S1 — recovery treats mid-file zeros as a seal sentinel, truncates the tail,
then rewrites the footer so the loss is undetectable.**
`crates/weir-server/src/wab/recovery.rs:321-323` breaks on `payload_len == 0`
leaving `quarantine_reason = None`; `:512` then truncates and `:515` writes a
fresh footer whose `record_count` matches the *truncated* prefix — so the
drain's footer cross-check (`drain/mod.rs:1088-1119`) passes. Logged at `INFO`.

Both neighbouring branches quarantine: oversized `payload_len` (`:355-372`) and
CRC mismatch (`:405-418`). The sentinel branch is the missing third case.

Verifier test — A/B/C written, B's bytes zeroed in place, C intact:

```
recovered = [Payload(7 bytes)]   // only A. C destroyed.
quarantine_counter = 0
quarantine_dir_exists = false
```

A zero `payload_len` mid-file with valid data after it is **always** corruption
and never a partial seal: `seal()` consumes the handle
(`wab/segment.rs:383`) and `write_record` rejects empty payloads at two layers
(`segment.rs:117`, `:546`).

**S2 — with zstd enabled, a ~16 MiB incompressible payload is acked durable,
then truncated away by crash recovery.**
The writer (`wab/segment.rs:139-142`) and `SegmentReader`
(`weir-wab/src/lib.rs:279`) bound a record by `max_stored_record_bytes()` =
`compress_bound(16 MiB)` ≈ 16,843,025. Recovery (`wab/recovery.rs:326`) uses
`MAX_PAYLOAD_HARD_CAP` = 16,777,216. Demonstrated: `zstd -1` over 16 MiB of
`/dev/urandom` yields **16,777,614 bytes — 398 over the recovery cap.**

So a pre-compressed or encrypted 16 MiB blob is written, fsynced and acked
`true`; after a crash, recovery calls it "oversized payload_len — likely
corruption" and truncates that record *and every acked record behind it* out of
the delivery path. Active-segment recovery only; sealed segments are unaffected.

**S3 — a failed fsync does not poison the segment.**
`WabSegment` poisons on a failed *write* (`segment.rs:175,182`) precisely because
the file offset has advanced past stray bytes. A failed *fsync* gets no such
treatment: `fsync_observed` (`wab/mod.rs:863-891`) logs, counts, returns `false`;
`ShardWriter::fsync_current` takes `&self` (`segment.rs:600`) and so cannot
touch `self.active`.

Verifier test, store failing only the first fsync:

```
first fsync = Err("injected EIO on fdatasync")
active_after_failed_fsync = true
second fsync = Ok(())  -> acks TRUE
active segment files = ["seg_00000001.wab"]   (same segment reused)
```

Batch N is correctly nacked, so this alone loses no acked record — the hole
becomes acked-data loss only via S1. **Downgraded to a contributing cause, not
an independent false ack**, and fixed on that basis.

### The silent ones stop delivery while acking

**S4 — a drain panic strands the queued backlog permanently.**
`run_drain_supervised` respawns `drain_thread`, which rebuilds `pending` empty
(`drain/mod.rs:483`) and sets `prev_health_ok = true` (`:498`). The only in-run
requeue is gated on the `now_ok && !prev_health_ok` edge (`:379`), which with a
healthy sink never fires again. `scan_unconfirmed_sealed` has exactly two
callers — startup (`wab/mod.rs:430`) and that edge — so nothing rescans.

```
panics=1 confirmed=0 stranded=0 resumed=0 seg1_exists=true seg2_exists=true
```

`weir_drain_panics_total` does increment, so it is not *fully* silent; but
`weir_drain_segments_stranded_total` stays 0 and the backlog is never
re-delivered before a restart.

**S5 — a transient dead-letter scan error stops delivery for the process
lifetime.** `DeadLetterWriter::open` failure logs and `return`s
(`drain/mod.rs:474-479`); the supervisor reads that `Ok(())` as a clean
channel-closed exit (`:237`) — no respawn, no metric. `open` → `scan_dir`
propagates `entry.metadata()?` (`dead_letter.rs:153`), so an ENOENT from a file
vanishing between `read_dir` and `stat` is enough. The daemon then boots, binds,
and acks producers forever while nothing is ever delivered.

**S6 — a thread-blocking sink wedges delivery, and the one warning that could
catch it is gated on exactly the wedged state.**
The drain is `new_current_thread` (`drain/mod.rs:469`), so the `tokio::time::timeout`
backstops on `commit` (`:1196`) and `health` (`:345`) can only fire if the sink's
future yields.

```
probe_health(timeout=50ms)  elapsed=1.504s  healthy=true
drain(commit_timeout=50ms, blocking commit=1.2s)  elapsed=1.214s  confirmed=1
```

No watchdog exists anywhere in `crates/`. The 5 s WAB-growth warning is
suppressed by `should_warn_wab_growth` when `drain_healthy`
(`main.rs:182-183`) — which is precisely the frozen state — and both gauges are
**pre-initialised** to it (`metrics/mod.rs:700`, `:724`), so a wedge on the very
first probe is suppressed too.

### The polyglot clients are already broken

**S7 — three of five demo clients fail conformance today, and CI cannot see it.**

```
python  15/30 vectors passed, 15 FAILED
go      FAIL
c       RESULT: FAIL
```

All from `durability='Sync' != 'Durable'`. Commit `2116377` renamed the tier in
`docs/conformance/wire_v1_vectors.json`; the last commit touching `demos/`
predates it. `grep -rn "run_vectors|demos/|conformance" .github/workflows/`
returns nothing, so no job runs them. This shipped in 2.0.0.

The Rust-side vectors are fine — `python3 docs/conformance/run_vectors.py`
reports `30/30`. It is only the from-spec clients that rotted, which is exactly
the population the conformance suite exists to serve.

## Decisions

**Scope is "silent". A loud failure is out of scope for 2.0.1** — an error a
reader can see is a different class of problem from one they cannot. Every fix
here either prevents the loss or makes an invisible failure visible.

**Every task starts with a test that must be seen to fail.** These findings came
from agents; the failing test *is* the verification. If a test does not fail
before the fix, the finding is refuted and the task is dropped rather than
implemented. Two findings (S4's seal path, S6's detection gap) reached only one
agent and are flagged accordingly.

**No API or wire change.** 2.0.1 is a patch. Anything requiring a signature or
digest change waits.

## Out of scope, and why

- **The dedup token collision** (`weir-sink-sdk/src/lib.rs:349-357` hashes only
  `len ++ bytes`, so byte-identical records collapse under the default
  per-record `Idempotency-Key`). Confirmed and live by default, but the fix
  changes the digest and voids the `dedup_token_is_unchanged_from_weir_1_x` pin
  (`sink/clickhouse.rs:707`). Breaking; needs its own release.
- **Shipping `--features tls`** so the release binaries and image have an ingest
  listener. Non-breaking and high value, but it changes what the artifact
  contains and pulls rustls into the image — a minor, not a patch.
- **The 16 MiB pre-allocation per connection** (`socket/connection.rs:232`).
  A real resource-exhaustion vector; the honest fix is chunked reading, which is
  too large for a patch.
- **`pending`'s missing high-water mark**, so backpressure is released during an
  outage. Behaviour-changing: producers begin blocking where they were acked.
- **The client's over-broad `#[cfg(unix)]`.** Additive feature work, not a fix.
- **Wire-level batching.** Needs a `WIRE_VERSION` bump — 3.0.

## Refuted or downgraded, recorded for honesty

- **No false-ack path in the write/ack split.** Every `write_record` error exit
  drains and nacks `pending_acks` at the moment `active` becomes `None`.
- **No lock-discipline surface at all** — there is no production `Mutex` or
  `RwLock` in `weir-server`; it is channels and atomics.
- **No reachable panic from wire input.** Every `unwrap`/`expect` under
  `socket/` and in `weir-core` is `#[cfg(test)]`.
- **S3 downgraded** from an independent false ack to a contributing cause.
- **"Shipped binaries have no listener" imprecise** — there is a loopback
  metrics listener; the correct claim is "no *ingest* listener".
- **"Nothing outside Rust can produce to weir" is false** — five from-spec
  clients exist. They are AF_UNIX-only, same-uid, and now partly broken.
