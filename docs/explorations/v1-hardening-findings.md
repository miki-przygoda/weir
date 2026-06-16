# weir — codebase hardening findings (scout, 2026-06-14)

> **What this is:** output of a 3-agent audit (logical-bug hunt · security-by-design
> · refactor/code-quality) of the codebase on `v1/phase-4-cleanup`, ahead of 1.0.
> Companion doc: [`v1-feature-directions.md`](v1-feature-directions.md) (things to add).
>
> ⚠️ **These are SCOUT findings — grounded in file:line with arguments, but not yet
> independently re-verified.** The CRITICAL bugs below should be **confirmed in code
> (and ideally turned into a failing test / DST scenario) before fixing.** Severity
> ordering and file:line are the scout's; treat as a prioritized to-verify list.

Effort key: **S** ≈ <1 day · **M** ≈ a few days · **L** ≈ a week+.

---

## Part 1 — Logical bugs & silent failures

The bug hunt's framing: weir already had a false-ack bug in the *flusher* that the
DST harness caught and fixed. The new findings are the **downstream analogue in the
drain** — the same "confirm-and-delete on a path that didn't actually persist"
shape, one layer past where the DST invariant (`i1_acked_true_is_durable`) stops.

> **STATUS (2026-06-14): ALL of B1–B8 confirmed in code and RESOLVED**, one commit
> each with regression tests — B1 `e14fe02`, B2 `f51db90`, B3 `cf72749`,
> B4 `d4e4ff5`, B8 `25198f5`, B6 `635e5c4`, B5 `b3e4cfe`, B7 `434be38`. Full bin
> suite (234) + DST sweep (300 seeds) + clippy/fmt green. Part 1 is fully closed;
> **Part 2 (security F1–F5) and Part 3 (refactor R1–R10) remain.**

### 🔴 CRITICAL — silent data loss / startup hang

**B1. Permanent sink error + dead-letter write failure ⇒ segment confirmed &
deleted (silent data loss).**
`drain/mod.rs:570-590` (and the partial-batch twin at `:518-533`).
A permanent sink error dead-letters the batch via `dead_letter.write_records()`.
If *that* write fails (ENOSPC on the dead-letter dir — precisely when dead-letter
pressure peaks — EIO, a partial `write_record` in `dead_letter.rs:72-75`), the
error is only `error!`-logged and the code falls through to `BatchResult::Ok` →
`Confirmed` → `confirm_and_delete` **deletes the sealed source segment**. Records
were never delivered (permanent error) and never dead-lettered → gone, no replay
path (the `.confirmed` sidecar makes recovery skip it). The comment at `:587-589`
explicitly chooses to confirm "we've either dead-lettered the records *or logged
the failure*" — logging is not durability. **Fix:** a failed dead-letter write must
return `Transient`/`Blocked`, never `Ok`, so the source segment is preserved.
*Severity: silent data-loss. Confidence: high.*

**B2. `SegmentReader::open` failure ⇒ segment confirmed & deleted (silent loss on
transient I/O).** `drain/mod.rs:406-411`.
On *any* open `Err`, `process_segment` logs "skipping" and returns
`Confirmed { record_count: 0 }` → deletes the segment. But `open` fails on
*transient* conditions too (EMFILE/ENFILE fd exhaustion, ENOMEM, a momentary header
read error), not just permanent corruption. A transient open failure discards a
good segment full of undelivered records. **Fix:** distinguish transient (→
`Transient`, retry) from permanent (→ quarantine, not delete), mirroring how
`recovery.rs` already quarantines bad-magic/version. *Severity: silent data-loss.
Confidence: high.*

**B3. Startup deadlock: recovery replay blocks on a bounded drain channel with no
consumer yet.** `wab/mod.rs:303-345` (`replay_unconfirmed`, blocking `drain_tx.send`)
+ `main.rs:177,197,236`.
`main` creates `bounded(256)` for the drain channel, then calls `wab::spawn` (which
runs `replay_unconfirmed` on the calling thread, blocking-`send`ing every
sealed-but-unconfirmed segment) — but the **drain thread (the only consumer) isn't
spawned until later** (`drain::spawn`, `main.rs:236+`). If recovery finds >256
unconfirmed sealed segments (a daemon that crashed while behind on draining — a long
sink outage), the 257th `send` blocks forever and the daemon never starts. Reachable
exactly in the recovery scenario this code exists to serve. **Fix:** spawn the drain
before recovery replay, or use an unbounded hand-off for replay. *Severity:
durability-availability (data is safe, daemon won't start to drain it). Confidence:
high.*

### 🟠 HIGH — durability gap

**B4. No parent-directory fsync after segment seal/rename or `.confirmed` write.**
`wab/segment.rs:213-241`, `recovery.rs:283-296`, `drain/confirmed.rs:46-57`,
`drain/dead_letter.rs:75`.
`seal()` fsyncs file *contents* then `rename(.wab → .wab.sealed)`, but the dirent
change isn't durable until the *parent directory* is fsynced — and there is **no
directory fsync anywhere** in the WAB lifecycle (grep-confirmed). A crash right after
a rename can lose the dirent: a sealed segment can reappear under its old name
(tolerable — recovery re-seals) or a `.confirmed` sidecar's creation can be lost
(→ re-drain = duplicate delivery), or a data file's dirent can be lost entirely on
some filesystems. This is the classic WAL gap (data fsynced, dirent not).
`finalize_to_disk` calls itself "the durability commit point," but the rename that
*publishes* it isn't itself crash-durable. The DST harness models the rename-loss via
re-sealing the orphan but operates on an in-memory ledger — it doesn't exercise true
dirent durability. **Fix:** fsync the parent dir after each rename and each
`.confirmed`/dead-letter seal. *Severity: durability-violation (crash window).
Confidence: medium-high.*

### 🟡 MEDIUM — correctness / robustness

- **B5. Mid-segment record read error truncates delivery, then confirms** —
  `drain/mod.rs:418-425,451-453`. A per-record CRC mismatch (post-seal corruption)
  or transient mid-read error `break`s the loop, commits what accumulated, and
  returns `Confirmed` — every record *after* the corrupt one is silently dropped (not
  dead-lettered, not quarantined). Surviving tail should be dead-lettered + a metric
  bumped; a transient read error should retry. *Silent loss, narrow.*
- **B6. `confirm_and_delete` deletes the segment even when the `.confirmed` write
  failed** — `drain/confirmed.rs:30-57`. Not data loss (records *were* delivered),
  but the stated "replayed on restart" contract becomes false, and a partial/un-fsynced
  `.confirmed` + crash can leave a CRC-failing sidecar that `check_confirmed` then
  quarantines against an already-deleted segment. Inconsistent post-conditions.
- **B7. Rotated/sealed segment silently dropped if the drain channel is
  disconnected** — `wab/mod.rs:483,587` (`drain_tx.send(sealed).ok()`). If the drain
  thread died (it has **no panic supervision**, unlike the flushers), `.ok()`
  discards the error; the segment is durable but never queued — a silent delivery
  stall with no metric/log. The bigger gap is **the drain has no `catch_unwind`** — a
  panic in `process_segment`/sink kills delivery permanently.
- **B8. `read_segment_record_count` failure silently reports 0 replayed** —
  `wab/mod.rs:327` (`.unwrap_or(0)`). Observability-only undercount; segment still
  queued correctly.

### ✅ Checked and cleared (NOT bugs — don't re-investigate)

- `flush_batch` durable/pending ack split (`wab/mod.rs:497-647`) — the false-ack fix
  holds; mid-batch write-Err correctly Nacks pending + leaves rotated acks intact.
- `ShardWriter::write_record` seal-failure-on-rotation (`segment.rs:413-435`) — Nacks
  then may later recover+deliver (safe direction).
- `poisoned` short-write handling (`segment.rs:90-173`) — correct.
- HTTP sink classification (`sink/http.rs`) — 4xx-except-408/429 permanent,
  408/429/5xx/transport transient; mid-batch transient retries whole segment under
  idempotency. Correct.
- Socket ack path (`socket/connection.rs:325-362`) — ack=false / dropped sender /
  timeout all → `Nack(InternalError)`, never a false ack. Worker offline-shard Nacks.
- Worker per-shard FIFO + `check_confirmed`/`parse_confirmed` version+CRC quarantine —
  correct (modulo B6's deleted-segment interaction).

> **Recommended:** extend the DST harness with a **drain-side scenario** that
> fault-injects `dead_letter.write_records` and `SegmentReader::open` and asserts "a
> confirmed segment's records are either delivered or durably dead-lettered" — the
> drain analogue of `i1_acked_true_is_durable`. This would lock B1/B2/B5 permanently.

---

## Part 2 — Security-by-design

> **STATUS (2026-06-14): F1 RESOLVED** (`8114a69` — both SQL build-error sites route
> through `redact_password`) and **F3 RESOLVED** (`a19b1d7` — `bind_hardened` warns when
> the socket parent dir is group/world-writable; opt-in `require_private_parent` refusal
> left as a follow-up). F2 folds into R1 (Payload newtype). F4/F5 are accepted-risk /
> docs-only as noted.

**Verdict:** weir is genuinely well-hardened for its single-node, local-trust model.
One real (low-severity) leak, a few undocumented/violable assumptions, lots of
already-solid areas.

- **F1 — SQL sink build errors can leak the connection-string password** *(REAL GAP,
  Low)* — `sink/mysql.rs:149-150`, `sink/postgres.rs:165-167`. `InvalidUrl(e.to_string())`
  takes the driver's error verbatim, which can embed `user:password@`; `main.rs`
  surfaces it to stderr/logs. Bypasses the otherwise-correct `redact_password`
  (wired only into `Debug`). **Fix:** route both sites through `redact_password()`,
  or make the build-error enum carry a redacted-URL field (class fix). ~30 min + test.
- **F3 — bind-time TOCTOU closed only by an undocumented operator assumption** *(Low)*
  — `socket/mod.rs:304-371`. `bind_hardened` is excellent; the one irreducible
  `bind`→`fstatat` window is closed operationally by requiring a daemon-private parent
  dir — but that lives only in prose (`docs/security/socket-bind.md`). An operator
  pointing `socket_path` at a world-writable parent (`/tmp`) silently re-opens the
  race with no warning. **Fix:** implement the already-designed opt-in
  `require_private_parent` check (one `fstatat` on the dirfd already held); warn by
  default. ~1 hr.
- **F2 — `Payload = bytes::Bytes` Debug-prints raw bytes** *(DESIGN, Informational)* —
  `weir-core/src/payload.rs:4`. No leak today (no log site interpolates a payload),
  but a future `debug!(?payload)` would silently log opaque/sensitive record bytes.
  **Fix:** newtype `Payload` with a length-only `Debug`, or a lint + CONTRIBUTING note.
- **F4 — `audit_segment_modes` warns but never enforces on tampered WAB perms**
  *(ACCEPTED RISK — confirm intent)* — `recovery.rs:48-79`. Defensible within the
  trust model (refusing to start = dropping durability is worse); flagged only so it's
  a conscious 1.0 decision. Recovery does use `O_NOFOLLOW`. No change required.
- **F5 — "valid CRC32 = trusted" + network-FS assumption** *(Informational)* —
  `format.rs:48-56`. Sound under the model (forging a WAB file already needs daemon-uid
  access). Only surprise: running the WAB on NFS/shared-volume silently violates the
  "daemon-private filesystem" assumption. **Fix:** elevate the network-FS caveat from a
  code comment into the operator threat-model doc; optional `statfs` warn.

**Audited and solid (credit, no action):** frame-parser DoS hardening
(validate-before-allocate, 16 MiB cap before the single alloc, no integer overflow,
slowloris-guarded `read_timeout` × 3, shutdown-raced header read) — *the strongest
part of the codebase*; peer-UID check (fails closed, unspoofable, default-on); shared
connection semaphore (cap not 2×); dead-letter byte cap; TLS path (mandatory mTLS,
Deny-anonymous, fail-safe SIGHUP reload, handshake-under-timeout, CN not used for
authz, plaintext-TCP fatal); SQL injection (strict identifier allowlist + bound
params); metrics info-leak (all labels bounded enums, 127.0.0.1 default justified by
the nack-reason decode oracle, metrics endpoint has its own conn cap).

---

## Part 3 — Refactor & code quality

> **STATUS (2026-06-14): ALL of R1–R10 RESOLVED**, one commit each. API-freeze
> gates: **R1** `ba34cf7` (Payload alias→newtype over Bytes — Deref/AsRef + length-only
> Debug, closes **F2**), **R2** `841a3c4` (Header/Envelope fields→private + accessors;
> Envelope::new derives payload_len so it can't desync), **R3** `6b14669`
> (`#![deny(missing_docs)]` on weir-core/client/sink-sdk + weir-core crate doc + 25
> filled gaps). Safe refactors: **R4** `6504915` (sealed→confirmed path hoisted to
> `wab::format::confirmed_path_for`), **R5** `d48c719` (path-validator drift guard +
> stale-comment fix), **R6** `fb423c0` (main.rs sink-selection helpers), **R7** `d1f3325`
> (drain ProcessResult→DrainState single source of truth). Housekeeping: **R8** `f147e58`
> (dead `_UNUSED_DURATION_TAG`), **R9** `f049801` (WabSegment::create O_EXCL doc), **R10**
> `33f8b9c` (wab/* pub→pub(crate) outside the format facade). Module-split was
> CONSIDERED-AND-SKIPPED (user's call; the scout judged the production code cohesive and
> the bulk is colocated tests). Full gate green across default·tls·clickhouse-sink·dst·
> no-default-features. **Part 3 fully closed.**

**Headline:** the codebase is in unusually good shape — the "large modules" are large
mostly from dense, high-quality *colocated tests* (drain ~66% tests, wab ~52%, config
~40%). **No structural change is recommended inside the ack/fsync path.** The
high-value/low-risk wins are in the public API surface (the freeze gate) + a few safe
mechanical extractions.

### 1.0-GATING (API shape — can't change post-1.0 without a breaking release)

- **R1. `Payload` is a bare `type Payload = bytes::Bytes` alias** —
  `weir-core/src/payload.rs:4`. Leaked internal: the whole `bytes::Bytes` API + its
  semver leak into yours (a `bytes` 2.0 = a breaking change to *your* 1.0). **Decide:**
  newtype `Payload(Bytes)` with `Deref<[u8]>` + explicit `From`/`Into` (preserves O(1)
  clone, mostly compiler-guided), OR keep the alias but *document the `bytes` semver
  commitment*. M / Low risk. *(Also closes F2 if newtyped with a length-only `Debug`.)*
- **R2. `Header`/`Envelope` expose all fields `pub`** — `envelope.rs:72-79,163-166`.
  A caller can desync `payload_len` from the actual payload or mutate `version` off
  `WIRE_VERSION`, bypassing the safe `Header::new`. **Fix:** read-only accessors +
  constructor-only mutation, deriving `payload_len` from the payload. M / Low.
- **R3. `#![deny(missing_docs)]` is OFF on all three lib crates** — and `weir-core`
  has *no crate-level `//!` doc at all*. **Fix:** add the lint to weir-core/
  weir-client/weir-sink-sdk (NOT weir-server), add the crate doc, fill the gaps. S–M /
  none. *(This is the API-freeze workstream's concrete task.)*

### 1.0-NICE-TO-HAVE (safe-mechanical, no behavior change)

- **R4. Duplicated `.confirmed`-path derivation** — `drain/confirmed.rs:62-66` vs
  `wab/recovery.rs:333-337` (identical strip-suffix/format). Two copies of a rule the
  write side and recovery read side must agree on exactly. Hoist into `wab/format.rs`
  (owns the extension constants). S / behavior-sensitive-adjacent (keep both tests +
  add a byte-identity test).
- **R5. `socket::validate_socket_path` ≈ `config::validate_path_format_inner`** —
  `socket/mod.rs:254-278` vs `config/mod.rs:742-769` (same absolute/no-`..`/no-null
  rules, twice; a stale comment already points at the wrong location). Trust-boundary —
  prefer the *minimal* fix (shared test vector + comment fix) over merging. S / low.
- **R6. `main.rs` sink-selection match is ~130 lines of per-arm boilerplate** —
  `main.rs:236-366`. Extract a `build_and_spawn_drain<S>` tail helper + a
  `require_sink_url` helper. S–M / safe (startup-only, not durability path).
- **R7. `drain/mod.rs` retry-transition logic duplicated** between the `Draining` and
  `RetryingTransient` arms (`:350-368` vs `:266-278`). Factor the `ProcessResult →
  DrainState` mapping into one helper. M / **behavior-sensitive** (the at-least-once
  retry state machine — not the false-ack path, but do it only with the rich drain
  tests as guard, or defer for max conservatism near release).

### POST-1.0 / housekeeping

- **R8.** Delete dead `sql_common.rs:185` `_UNUSED_DURATION_TAG` (misleading rationale
  comment). S.
- **R9.** Fix inaccurate `WabSegment::create` doc — it claims the caller must guarantee
  non-existence, but `O_EXCL` enforces it. S, doc-only.
- **R10.** Tighten `wab/*` `pub` → `pub(crate)` (only `wab::format` is re-exported by
  the server facade; the rest are `pub` only for cross-module use — note
  `WabSegment::create`/`segment_path` are used by `dead_letter.rs`, so `pub(crate)`,
  not private). M / none (no external consumers).

### Explicitly NOT recommended

Splitting drain/config/wab by line count (length is tests; production code is cohesive
and already sub-divided); touching `flush_batch`/`write_record`/`finalize_to_disk` (the
false-ack-prevention core); merging the two `InsertMode` enums (dialect-specific).

---

## Part 4 — Prioritized pre-1.0 action order (synthesis)

1. **Verify + fix B1, B2** (drain silent-data-loss on dead-letter/open failure) — the
   active analogues of the false-ack bug, one layer downstream, NOT covered by the
   current DST invariant. **Then add the drain-side DST scenario** to lock them.
2. **Verify + fix B3** (startup deadlock) — turns a recoverable backlog into a
   non-starting daemon.
3. **B4** (parent-directory fsync) — the remaining true durability gap.
4. **F1** (redact SQL build-error URL) + **F3** (`require_private_parent`) — cheap
   security class-fixes.
5. **R1, R2, R3** (the API-shape freeze gate: `Payload`, `Header`/`Envelope`,
   `deny(missing_docs)`) — irreversible after 1.0; do before the freeze.
6. **B5–B8** (drain/recovery hardening) + **R4–R7** (safe refactors) as the cleanup
   pass.
7. **F2/F5 docs+lint, R8–R10** housekeeping — opportunistic.

---

## Part 5 — Second-pass findings (N1–N7) — ALL RESOLVED

After Part 1 (B1–B8) closed, a second multi-agent hunt re-swept the codebase on the
hypothesis that the CRITICAL bugs had "overpowered" the first pass and masked smaller
ones. It did — a backlog of real (mostly data-loss / data-leak) issues the first sweep
missed. All confirmed in code and fixed, one commit + regression test each.

- **N1 — `user:password@` URL parsing split at the FIRST `@`** *(HIGH — data loss +
  credential leak)* — `sink/sql_common.rs`, `sink/clickhouse.rs`. A `@` in the password
  split mid-secret: wrong creds (every insert fails → dead-letter) AND the secret tail
  spliced into the base URL → request + access logs. Fixed: split at the LAST `@` within
  the authority. `35e8bf7`.
- **N2 — empty payload serializes to the WAB sentinel** *(HIGH — silent data loss)* —
  an empty payload `[0,0,0,0]` IS the end-of-records sentinel; storing one truncates the
  segment. Fixed: reject empty **Push** payloads at ingest with `Nack(EmptyPayload)`
  (new wire reason `0x07`) + a `write_record` guard. `5e4d4ba`.
  - **N2 regression (this pass):** the ingest reject ran before message dispatch, so it
    also Nack'd HealthCheck frames (zero-length by spec) → every `health_check()` failed.
    Caught by the system + load integration suites (not the bin suite). Fixed by gating
    on `message_type == Push`. `b30f90f`.
- **N3 — ClickHouse 429/408 dead-lettered live data** *(MED — data loss under load)* —
  back-pressure (429 Too Many Requests, 408) was classified permanent → dead-lettered
  instead of retried. Fixed: `status_is_transient` treats 5xx/408/429 as transient,
  matching the HTTP sink. `e8ef824`.
- **N4 — bind parent-dir race not surfaced** *(LOW — security)* = F3 above. `a19b1d7`.
- **N5 — accept(2) EMFILE/ENFILE busy-spin** *(MED — availability)* — on fd/buffer
  exhaustion the pending connection stays queued, so the accept loop spun at 100% CPU.
  Fixed: 50ms backoff on EMFILE/ENFILE/ENOBUFS/ENOMEM (Unix + TCP loops) + a
  `weir_accept_resource_exhaustion` metric. `14c69ce`.
- **N6 — quarantine cross-shard filename collision** *(MED — forensic data loss)* —
  segment counters are shard-local, so `seg_00000001.wab` exists in every shard; the flat
  quarantine dir let one shard's corrupt segment clobber another's via `rename`. Fixed:
  namespace by shard + refuse-to-clobber. `31d0c49`.
- **N7 — minors sweep** *(LOW)* — stale CLI `--help` (worker-count default; missing
  postgres/clickhouse sinks) `0de857e`; HTTP `health()` wrongly degraded auth-challenge
  endpoints `76375a2`; dead `any_buffer_below_batch_size` guard `ebb8988`; cryptic error
  on a short segment footer `60f767e`; ClickHouse dedup token ignored batch boundaries
  (distinct batches → same token → dropped as dup) `60d9998`; ClickHouse URL unvalidated
  until first commit `b1d7925`.

> **Verification:** full bin suite (default 247 / all-features 281) + system (38) + load
> (13) + tls_listener (4) + DST 300-seed sweep + clippy/fmt across default·tls·
> clickhouse-sink·dst — all green.
