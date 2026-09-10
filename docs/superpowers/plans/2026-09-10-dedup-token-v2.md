# Dedup identity that survives a changed `sink_max_batch_size`

**Status:** Plan.
**Date:** 2026-09-10.
**Branch:** `feat/dedup-token-v2`, off `main` @ 2.1.0.
**Supersedes for the dedup half:** `docs/superpowers/specs/2026-08-09-wab-format-v2-and-dedup-token-design.md`.

---

## 1. Verdict on the 2026-08-09 spec

**The dedup half of that spec has already shipped, and the part of it that is
still unshipped is a different feature. Neither is what fixes this.**

Line by line against today's tree:

| Spec section | State on `main` @ 2.1.0 |
|---|---|
| §6.1 `SinkBatch` + `DedupToken` | **Shipped** in 2.0.0, `crates/weir-sink-sdk/src/lib.rs` |
| §6.2 `commit(SinkBatch)` | **Shipped** — and went further than the spec: `Sink::Record` was dropped too |
| §6.5 ClickHouse / HTTP migrations | **Shipped** |
| §8.5 known-answer token vector | **Shipped** (`dedup_token_is_unchanged_from_weir_1_x`) |
| §4, §5 WAB format v2 + zstd | Not shipped. **Unrelated to this defect** — see below |
| §6.4 "the batch-size precondition is a documented precondition" | Shipped as documentation. **This is the thing we are here to remove** |

Two independent judgements follow.

### 1.1 No WAB format change is needed, and the spec already says so

§3.1 of the spec establishes that the dedup token "needs no on-disk storage at
all", and §2 ("The two halves are independent") states outright that the format
work and the `SinkBatch` work "touch disjoint code and can be built, reviewed,
and merged separately." Format v2 exists in the spec to carry **zstd
compression**, which is a WAB *capacity* feature (§3.3: "compression does not
buy latency"; what it buys is outage headroom). It has nothing to do with
idempotency.

So the answer to "does this need a format bump" is **no**, and that is not a
deviation from the spec — it is the spec's own position. Implementing format v2
here would be shipping an unrelated feature under this defect's name.

### 1.2 §3.1 is still right, and it is why the token itself cannot be repaired

The spec's §3.1 argues the token must be batch-scoped, because
`process_segment` splits a segment into `sink.max_batch_size()` chunks and a
segment-scoped token shared across them would make a dedup-capable sink discard
chunks `2..N` — **losing** data in the name of preventing duplicates. That
argument holds exactly as written; `process_segment`
(`crates/weir-server/src/drain/mod.rs:1135`) still splits the same way.

But it has a consequence the spec did not draw, and §6.4 papered over instead:

> **No batch-level key can survive re-batching.** A batch-level key is a
> function of the batch. Re-batching changes the batch. Therefore the key
> changes. This is not a defect in `DedupToken`'s construction that a better
> digest would fix — it is what "batch-scoped" *means*.

Concretely, three schemes all fail for the same reason, and it is worth writing
down so nobody re-proposes them:

- Hash the batch's contents (today) — different contents per chunk, different token.
- Hash the batch's WAB coordinate range `(segment, first_index, count)` —
  the range itself moves when the boundaries move.
- Cut on a power-of-two grid so boundary sets nest — a size-2ᵏ batch then
  *contains* whole size-2ʲ batches, but its key is still a fourth value the
  downstream has never seen.

So §6.4's precondition is not a caveat that better engineering removes. It is a
true statement about batch-scoped identity, and the fix is to **stop putting the
guarantee on the batch**.

### 1.3 What the spec could not have known: `RecordId` already exists

`RecordId` (`weir-sink-sdk`, shipped 2.0.3 — seven months after the spec was
written) is `sha256(segment_name ++ index ++ payload)`, where `index` is the
record's **absolute ordinal within its WAB segment**. The drain populates it in
`process_segment` from `read_index`, which is incremented once per record read
from the segment and is never a function of `max_batch`:

```rust
read_index += 1;
if read_index <= skip { continue; }
batch_ids.push(RecordId::for_record(&segment_name, read_index, &payload));
```

That is precisely "a WAB coordinate that does not move under re-batching". The
identity the fix needs is already computed, already parallel to the records, and
already handed to every sink on every commit as `SinkBatch::record_ids()`.

**The primitive is not missing. Three things around it are.**

---

## 2. What is actually broken

### G1 — the invariant is unpinned

Nothing in the test suite asserts that the `RecordId`s a sink receives are
independent of `sink_max_batch_size`. `read_index` sits three lines from
`batch.len()` and `max_batch`; a plausible refactor ("use the batch-relative
index, it's right there") silently reintroduces the hazard, and every existing
test still passes. The one test that would catch it does not exist.

### G2 — the two SQL sinks whose tables hold billing rows cannot use it

`sink/postgres.rs` and `sink/mysql.rs` call `batch.into_records()` and discard
the ids. Their documented idempotency recipe
(`docs/operations/configuration.md`) is a `UNIQUE` constraint on the payload or
a generated `payload_sha256`:

```sql
payload_sha256 BYTEA GENERATED ALWAYS AS (sha256(payload)) STORED,
UNIQUE (payload_sha256)
```

That recipe is worse than the defect this branch is about. It is **content**
identity, so `ON CONFLICT DO NOTHING` / `INSERT IGNORE` discards two
*genuinely distinct* records that happen to carry identical bytes — which is
the normal case for exactly the workloads that motivated this work: a metering
"one unit consumed" event, a heartbeat, any fixed-shape event. weir acks those
records, writes them to disk, delivers them, and the operator's own recommended
schema drops them. That is silent **loss**, not duplication.

This is the same distinction `RecordId`'s own rustdoc already draws between
itself and `DedupToken`; the SQL sinks' schema advice simply predates it.

### G3 — the docs state the precondition as unconditional

`docs/sinks/s3.md` says `sink_max_batch_size` "must be **constant for the life
of the bucket**", and the `DedupToken` rustdoc says "the guarantee above holds
only while that setting is stable." Both are true *of the thing they describe*
and both read as properties of weir. Neither points at the escape hatch sitting
in the same struct.

---

## 3. What ships

Additive throughout. `weir-sink-sdk` gains no breaking change and no new type;
`weir-wab` is untouched; the on-disk format does not move.

### T1 — pin the invariant (the test that matters)

`MockSink` in `drain/mod.rs` grows two recorders (`seen_record_ids`,
`seen_dedup_tokens`). Then:

- **`record_ids_survive_a_changed_sink_max_batch_size`** — write one sealed
  segment, drain it through a sink with `max_batch_size = 3`, drain the *same
  bytes* again through a sink with `max_batch_size = 7`, assert the flattened
  `RecordId` sequences are equal. This is the whole feature in one assertion.
- **`the_dedup_token_deliberately_does_not_survive_it`** — the companion, in the
  house style of pinning a known limitation: assert the `DedupToken` sequences
  *differ*, and say in the comment why that is correct (§1.2) and what a future
  reader should do instead (use `record_ids`). Without this, a later reader sees
  only the first test and assumes the token is safe too.

Falsification for the first: change `RecordId::for_record(&segment_name,
read_index, …)` to a batch-relative index and confirm the test fails with a
message that names the two batch sizes.

### T2 — let Postgres and MySQL key on the record, not the bytes

Two new optional knobs, default unset, no behaviour change when unset:

| Knob | Type | Default |
|---|---|---|
| `sink_postgres_id_column` | identifier, optional | unset |
| `sink_mysql_id_column` | identifier, optional | unset |

Per-engine names, matching the existing `sink_postgres_column` /
`sink_mysql_column` convention rather than inventing a shared knob that reads
like it applies to every sink.

When set, the INSERT carries two columns per row and binds the record's
`RecordId` hex into the first:

```sql
INSERT INTO "t" ("record_id", "payload") VALUES ($1, $2), ($3, $4), …
  ON CONFLICT DO NOTHING
INSERT IGNORE INTO `t` (`record_id`, `payload`) VALUES (?, ?), (?, ?), …
```

The operator puts the `UNIQUE` constraint on `record_id` instead of on the
payload hash, and gets an idempotency key that is
(a) unique across records with identical bytes — closing G2's loss, and
(b) invariant under re-batching — closing the defect this branch is named for.

Bounds that make this safe to land:

- `sink_max_batch_size` is already capped at 10 000 by config validation
  (`config/mod.rs:815`), so two parameters per row is 20 000 — well inside
  Postgres's 65 535-parameter wire ceiling. No new cap needed; the reasoning
  gets a comment so a future raise of the cap trips over it.
- The id column goes through the same `validate_identifier` as every other
  identifier, so the `format!`-built SQL keeps its zero-injection-surface
  property.
- A batch whose `record_ids()` is `None` (only reachable from a hand-built
  `SinkBatch`, never from the drain) must not silently insert a wrong or empty
  key. It returns a **permanent** error naming the cause: with an id column
  configured, a missing id is a programming error, and dead-lettering it loudly
  beats writing rows whose dedup key is a lie.

### T3 — say the true thing in the docs

- `weir-sink-sdk`: `DedupToken`'s precondition section gains the escape hatch —
  the guarantee is batch-scoped *because a batch key cannot be otherwise*, and a
  sink needing re-batch survival must key on `SinkBatch::record_ids()`.
  `SinkBatch::record_ids()` states the invariance as a named guarantee.
- `docs/sinks/s3.md`: the precondition stays (it is inherent to one object per
  batch — an object key names a batch), but it is scoped to S3 and explains that
  it is a consequence of the object-per-batch model, not a weir-wide rule, with
  a pointer to the per-record sinks for workloads that cannot accept it.
- `docs/operations/configuration.md`: the two new knobs, and the reference
  schemas switch to a `record_id` UNIQUE with the payload-hash version kept and
  explicitly marked as the lossy choice.
- `docs/getting-started/integrating.md`: the per-record dedup recipe for sink
  authors.
- `CHANGELOG.md` under `## [Unreleased]`.

### Explicitly not in scope

- **WAB format v2 / zstd.** §1.1. Different feature, different release.
- **ClickHouse.** Its only batch-level mechanism is
  `insert_deduplication_token`, which is block-scoped by ClickHouse's own
  design; a per-row key there needs a `ReplacingMergeTree` and an eventual
  merge, which is not a guarantee weir can make on the operator's behalf. The
  honest fix is to document the limitation, which T3 does.
- **HTTP NDJSON mode.** One request, one `Idempotency-Key` — the key names the
  request, so it is batch-scoped by construction. Per-record mode already keys
  on `RecordId` (2.0.3) and is the answer for anyone who needs the guarantee.
- **S3.** One object per batch means the object key *is* a batch name. Making
  it re-batch-stable requires abandoning object-per-batch, which is a redesign
  of a sink shipped three days ago, not a fix.

---

## 4. Task list

1. Plan committed (this file).
2. T1: `MockSink` recorders + the two drain tests. Falsify both.
3. T2a: `sink/postgres.rs` — config field, identifier validation, two-column
   `build_insert_sql`, `commit` binding, missing-id permanent error. Unit tests
   on the SQL shape and the binding order; falsify against a swapped column
   order.
4. T2b: `sink/mysql.rs` — the mirror.
5. T2c: config plumbing (`cli.rs`, `env.rs`, `file.rs`, `mod.rs`) and the two
   construction sites in `main.rs`.
6. T3: docs + CHANGELOG.
7. Gate: `cargo fmt --all --check`; `cargo clippy --all-targets --all-features
   -- -D warnings`; workspace suite; `cargo test -p weir-server --bins
   --all-features -- --test-threads=1`.
