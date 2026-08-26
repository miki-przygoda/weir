# Durability Tier Collapse + 2.0.0 Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse `Durability` from three tiers to the two that actually exist (`Durable`, `Buffered`), keeping the wire backward-compatible, then merge the 136-commit chain to `main` and release 2.0.0.

**Architecture:** `Durable = 0x01` and `Buffered = 0x03`; `0x02` is retired but still *decodes* to `Durable` so live 1.3.1 clients keep working, while the encoder only ever emits `0x01`. `Sync`/`Batched` survive as `#[deprecated]` associated consts, which covers the ~96% of usage that constructs rather than matches. The `tier` metric label renames to `durable`/`buffered` to match.

**Tech Stack:** Rust (workspace of 8 crates), Prometheus metrics, a JSON wire-conformance vector suite driven by Python.

**Spec:** `docs/superpowers/specs/2026-08-26-durability-tier-collapse.md`

## Global Constraints

- **`0x02` must continue to decode** to the durable tier. weir 1.3.1 is live on crates.io and `0x02` is a published contract in `docs/wire_protocol.md` and `docs/conformance/wire_v1_vectors.json`. Rejecting it is a regression, not a cleanup.
- **The encoder must only ever emit `0x01`** for the durable tier. Decode is permissive; encode is canonical.
- **`0x02` is never reassigned.** It stays permanently reserved.
- **Do not change what either tier does.** The fsync behaviour is untouched — this is a surface change only. Any diff that alters when `fdatasync` is called is out of scope and wrong.
- **`WIRE_VERSION` is not bumped.** Frame layout is unchanged and `0x02` still decodes.
- `Display` for the durable tier emits exactly `"durable"`; `Buffered` still emits `"buffered"`.
- Deprecated consts must be `Durability::Sync` and `Durability::Batched`, both equal to `Self::Durable`.
- **The gate is NOT `cargo test --workspace`.** weir-server's bin unit tests must run with `--test-threads=1` (process-global umask in `socket::bind_hardened`); running them in parallel yields ~75 spurious `PermissionDenied` failures. The real gate is the five commands in Task 6 Step 3, mirroring `.github/workflows/ci.yml:65-67`.
- Publish order is fixed by internal deps: `weir-core` → `weir-wab` → `weir-sink-sdk` → `weir-client` → `weir-server` → `weir-ctl` → `weir-rs`.

## File Structure

| File | Responsibility |
|---|---|
| `crates/weir-core/src/durability.rs` (modify) | The enum, deprecated consts, permissive `TryFrom`, `Display` |
| `crates/weir-core/tests/conformance.rs` (modify) | Wire round-trip + the `0x02` compatibility case |
| `crates/weir-server/src/socket/connection.rs`, `src/wab/mod.rs`, `src/metrics/mod.rs` (modify) | The 7 in-tree match arms and the tier label |
| `crates/weir-client/`, `weir-ctl/`, tests, examples (modify) | Mechanical constructor sweep |
| `docs/conformance/wire_v1_vectors.json`, `gen_vectors.py`, `run_vectors.py` (modify) | Keep the `Batched` vector as a decode-only case |
| `docs/wire_protocol.md`, `monitoring.md`, `architecture.md`, + 30 more (modify) | Documentation sweep |
| `CHANGELOG.md` (modify) | The 2.0.0 entry and the dashboard migration note |

---

### Task 1: Collapse the enum in `weir-core`

**Files:**
- Modify: `crates/weir-core/src/durability.rs`

**Interfaces:**
- Produces: `Durability::Durable`, `Durability::Buffered`, `Durability::Sync` (deprecated const), `Durability::Batched` (deprecated const), permissive `TryFrom<u8>`, `Display` emitting `"durable"`/`"buffered"`.

- [ ] **Step 1: Write the failing tests**

Replace the `mod tests` block in `crates/weir-core/src/durability.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_values_are_frozen() {
        assert_eq!(Durability::Durable as u8, 0x01);
        assert_eq!(Durability::Buffered as u8, 0x03);
    }

    #[test]
    fn legacy_batched_byte_still_decodes_to_durable() {
        // weir 1.3.1 is live and 0x02 is a published contract. A 1.x client
        // pushing Batched must keep working against a 2.0 daemon.
        assert_eq!(Durability::try_from(0x02).unwrap(), Durability::Durable);
    }

    #[test]
    fn encoder_never_emits_the_retired_byte() {
        // Decode is permissive, encode is canonical.
        assert_eq!(u8::from(Durability::Durable), 0x01);
        assert_ne!(u8::from(Durability::Durable), 0x02);
    }

    #[test]
    fn unknown_bytes_are_still_rejected() {
        assert!(Durability::try_from(0x00).is_err());
        assert!(Durability::try_from(0x04).is_err());
        assert_eq!(Durability::try_from(0x7f).unwrap_err(), UnknownDurability(0x7f));
    }

    #[test]
    fn deprecated_aliases_resolve_to_durable() {
        #![allow(deprecated)]
        assert_eq!(Durability::Sync, Durability::Durable);
        assert_eq!(Durability::Batched, Durability::Durable);
    }

    #[test]
    fn display_matches_the_metric_label() {
        assert_eq!(Durability::Durable.to_string(), "durable");
        assert_eq!(Durability::Buffered.to_string(), "buffered");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p weir-core durability`
Expected: FAIL — `no variant named 'Durable' found for enum 'Durability'`.

- [ ] **Step 3: Write the implementation**

Replace everything above `#[cfg(test)]` in `crates/weir-core/src/durability.rs` with:

```rust
//! The [`Durability`] tier — the per-record durability guarantee a producer
//! requests in the frame header.

/// Durability tier requested by the producer for a given record.
///
/// There are two tiers because there are two behaviours. `Durable` writes and
/// `fdatasync`s the record — as part of one batch-boundary group fsync —
/// *before* the ACK, so an ack means the bytes are on stable storage and will
/// replay after a crash. `Buffered` trades that for latency: it acks after the
/// in-memory write, before any fsync, so a `Buffered` ack survives a process
/// crash but not power loss.
///
/// # Wire values
///
/// `0x01` and `0x03` are fixed. **`0x02` is retired and permanently reserved**:
/// it was `Batched`, which fsynced once per batch while `Sync` fsynced once per
/// record. Both moved to the batch-boundary group fsync, at which point the two
/// tiers became the same thing. `try_from` still accepts `0x02` so producers
/// built against weir 1.x keep working; the encoder only ever emits `0x01`.
/// Reusing `0x02` for a future tier would turn every 1.x client into a silent
/// mis-tier, so it stays reserved.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Durable before ACK: written and `fdatasync`ed via the batch-boundary
    /// group fsync before the ACK is sent.
    Durable = 0x01,
    /// Memory write only. ACK is sent after the record enters the in-memory
    /// queue — survives a process crash, but not power loss.
    Buffered = 0x03,
}

impl Durability {
    /// Former name of [`Durability::Durable`].
    #[deprecated(since = "2.0.0", note = "renamed to `Durable`")]
    pub const Sync: Self = Self::Durable;
    /// Former tier that was identical to `Sync` in behaviour. See the type docs.
    #[deprecated(
        since = "2.0.0",
        note = "`Batched` was always identical to `Sync`; use `Durable`"
    )]
    pub const Batched: Self = Self::Durable;
}

impl From<Durability> for u8 {
    /// The canonical wire byte for this tier. Never returns the retired `0x02`.
    fn from(d: Durability) -> u8 {
        d as u8
    }
}

impl std::fmt::Display for Durability {
    /// Also the value of the `tier` label on
    /// `weir_records_{accepted,ack,nack}_total`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Durability::Durable => "durable",
            Durability::Buffered => "buffered",
        };
        write!(f, "{s}")
    }
}

/// Error returned when a `u8` does not map to a known `Durability` variant.
/// Preserves the raw byte for logging by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownDurability(pub u8);

impl std::fmt::Display for UnknownDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown durability byte: {:#04x}", self.0)
    }
}

impl std::error::Error for UnknownDurability {}

impl TryFrom<u8> for Durability {
    type Error = UnknownDurability;

    /// Permissive by design: `0x02` (retired `Batched`) decodes to
    /// [`Durability::Durable`] so weir 1.x producers keep working.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 | 0x02 => Ok(Durability::Durable),
            0x03 => Ok(Durability::Buffered),
            v => Err(UnknownDurability(v)),
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p weir-core durability`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/weir-core/src/durability.rs
git commit -F - <<'MSG'
feat(core)!: two durability tiers, because there are two behaviours

Sync and Batched have carried the same guarantee since both moved to the
batch-boundary group fsync — the type's own docstring and wire_protocol.md
both said so. Three names for two behaviours is a tax every reader pays.

0x02 is retired but still DECODES to Durable: weir 1.3.1 is live on crates.io
and 0x02 is a published contract, so a 1.x producer must keep working against a
2.0 daemon. The encoder only ever emits 0x01. Decode permissive, encode
canonical. 0x02 is never reassigned — reusing a retired wire value is how a
protocol acquires a permanent trap.

Sync and Batched survive as deprecated consts. Measured across the workspace:
190 constructions against 7 match arms, so nearly all real usage keeps
compiling with a warning rather than an error.

BREAKING CHANGE: `Durability` has two variants; an exhaustive match over three
no longer compiles. `Display` emits "durable", so the `tier` metric label
changes from sync/batched to durable.
MSG
```

---

### Task 2: Fix the in-tree match arms and the metric label

**Files:**
- Modify: `crates/weir-core/tests/conformance.rs`, `crates/weir-core/tests/proptest_envelope.rs`
- Modify: `crates/weir-server/src/socket/connection.rs`, `crates/weir-server/src/wab/mod.rs`, `crates/weir-server/src/metrics/mod.rs`

**Interfaces:**
- Consumes: Task 1's `Durability::Durable`, `Durability::Buffered`, `Display`.

- [ ] **Step 1: Find every match arm and construction that no longer compiles**

```bash
cargo build --workspace --all-targets 2>&1 | grep -E '^error' | head -40
```

Expected: errors at the 7 match-arm sites plus any exhaustive matches.

- [ ] **Step 2: Rewrite each match arm**

Every arm of the form:

```rust
Durability::Sync | Durability::Batched => { /* durable path */ }
Durability::Buffered => { /* buffered path */ }
```

becomes:

```rust
Durability::Durable => { /* durable path */ }
Durability::Buffered => { /* buffered path */ }
```

Where `Sync` and `Batched` had *separate* arms with identical bodies, collapse them into the single `Durable` arm — do not preserve the duplication. Where a match had only `Durability::Sync =>` and relied on a `_` fallback, replace with `Durability::Durable =>` and keep the fallback only if `Buffered` is genuinely handled there.

**Do not change any fsync call or its position.** If collapsing an arm would move when `fdatasync` happens, stop and report it — that is out of scope and means the arms were not actually identical.

- [ ] **Step 3: Verify the metric label follows `Display`**

The `tier` label is produced from `Durability`'s `Display`. Confirm no site hardcodes `"sync"` or `"batched"`:

```bash
grep -rn '"sync"\|"batched"' crates --include='*.rs' | grep -v '^crates/weir-core/src/durability.rs'
```

Expected: only doc-comment references, which Task 5 sweeps. Any live string literal building a label must be changed to use `Display`.

- [ ] **Step 4: Build and test**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: PASS. If a test asserts `tier="sync"`, update the assertion to `tier="durable"` — that is the intended change, not a regression.

- [ ] **Step 5: Commit**

```bash
git add crates/
git commit -F - <<'MSG'
refactor!: collapse the in-tree durability match arms

Every site that matched Sync and Batched separately ran identical bodies. They
are now one Durable arm. The tier metric label follows Display, so it moves from
sync/batched to durable with no hardcoded label strings left behind.

No fsync call moved. This is a surface change; the durability behaviour is
byte-for-byte what it was.
MSG
```

---

### Task 3: Sweep the constructor call sites

**Files:**
- Modify: `crates/weir-client/src/lib.rs`, `src/unix.rs`, `tests/protocol.rs`, `tests/client_server.rs`, `examples/push_simple.rs`, `examples/push_tls.rs`
- Modify: `crates/weir-ctl/src/main.rs`, `crates/weir-server/tests/load.rs`, `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes: Task 1's `Durability::Durable`.

- [ ] **Step 1: Rewrite the constructions**

These are ~190 mechanical substitutions. weir's own code should use the new name, not the deprecated alias:

```bash
grep -rl 'Durability::\(Sync\|Batched\)' crates --include='*.rs' \
  | grep -v 'crates/weir-core/src/durability.rs' \
  | xargs sed -i '' -e 's/Durability::Sync/Durability::Durable/g' \
                    -e 's/Durability::Batched/Durability::Durable/g'
```

`crates/weir-core/src/durability.rs` is excluded because it *defines* the deprecated consts.

- [ ] **Step 2: Check for now-duplicate arguments**

A call site that previously exercised both tiers may now pass `Durable` twice:

```bash
grep -rn 'Durability::Durable.*Durability::Durable' crates --include='*.rs'
```

Any hit is a test that meant to cover two distinct tiers and now covers one twice. Change one of them to `Durability::Buffered` if the test's intent was tier coverage, or drop the duplicate if it was iterating a list.

- [ ] **Step 3: Build, test, lint**

```bash
cargo fmt
cargo test --workspace 2>&1 | tail -15
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -15
```

Expected: all PASS, no warnings. Clippy must be clean — a `deprecated` warning inside weir's own code means a call site was missed.

- [ ] **Step 4: Commit**

```bash
git add crates/
git commit -F - <<'MSG'
refactor!: use Durable at weir's own call sites

Mechanical rename across client, ctl, server tests and examples. weir does not
use its own deprecated aliases — a deprecation warning in this workspace means a
site was missed, so clippy -D warnings is the check that this is complete.
MSG
```

---

### Task 4: Keep the retired byte covered by conformance

**Files:**
- Modify: `docs/conformance/wire_v1_vectors.json`, `docs/conformance/gen_vectors.py`, `docs/conformance/run_vectors.py`
- Modify: `crates/weir-core/tests/conformance.rs`

**Interfaces:**
- Consumes: Task 1's permissive `TryFrom`.

- [ ] **Step 1: Read what the vectors currently assert**

```bash
python3 -c "
import json; v=json.load(open('docs/conformance/wire_v1_vectors.json'))
ks=v if isinstance(v,list) else v.get('vectors',v)
print(type(ks), len(ks) if hasattr(ks,'__len__') else '?')
print(json.dumps(ks[0] if isinstance(ks,list) else ks, indent=2)[:600])
"
grep -n 'Batched' docs/conformance/*.py docs/conformance/wire_v1_vectors.json
```

- [ ] **Step 2: Retain the `Batched` vector as a decode-only case**

The existing vector with `"durability": "Batched"` is the *only* frozen artifact proving `0x02` is accepted. Do not delete it. Relabel its intent so a reader knows it is legacy-decode, not round-trip:

- keep its bytes byte-for-byte identical
- rename the vector's `durability` field value to `"Batched (retired 0x02, decodes to Durable)"` **only if** the runner treats that field as a label; if the runner parses it into an enum, leave the value as `"Batched"` and add a sibling `"note"` field instead
- add an explicit expectation that decoding yields the durable tier

Whichever shape the runner requires, the vector must continue to assert: **these bytes decode, and they decode to the durable tier.**

- [ ] **Step 3: Add a round-trip guard**

In `crates/weir-core/tests/conformance.rs`:

```rust
#[test]
fn retired_batched_byte_decodes_but_never_round_trips() {
    // The frozen vector proves 0x02 still decodes. This proves we never emit
    // it back — decode permissive, encode canonical.
    let decoded = Durability::try_from(0x02).expect("0x02 must still decode");
    assert_eq!(decoded, Durability::Durable);
    assert_eq!(u8::from(decoded), 0x01, "re-encoding must canonicalise to 0x01");
}
```

- [ ] **Step 4: Run the conformance suite**

```bash
cargo test -p weir-core --test conformance
python3 docs/conformance/run_vectors.py
```

Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/conformance crates/weir-core/tests/conformance.rs
git commit -F - <<'MSG'
test(conformance): the retired 0x02 stays covered

The Batched vector is the only frozen artifact proving a 1.x producer's tier
byte still decodes, so it is retained rather than deleted, and paired with a
round-trip guard asserting we canonicalise back to 0x01 and never re-emit 0x02.
MSG
```

---

### Task 5: Documentation sweep

**Files:**
- Modify: `docs/wire_protocol.md`, `docs/architecture.md`, `docs/monitoring.md`, `docs/operations/configuration.md`, `docs/getting-started/quickstart.md`, `docs/getting-started/integrating.md`, `docs/conformance.md`, `README.md`
- Leave alone: everything under `docs/explorations/`, `docs/superpowers/`, and `docs/benchmarks/` dated before today

**Interfaces:**
- Consumes: Task 1's naming.

- [ ] **Step 1: Update the wire protocol tier table**

In `docs/wire_protocol.md`, replace the three-row tier table and the note beneath it with:

```markdown
| Byte | Name      | Guarantee                                                            |
|------|-----------|----------------------------------------------------------------------|
| 0x01 | Durable   | Group fdatasync at the batch boundary before Ack — on stable storage  |
| 0x02 | *retired* | Formerly `Batched`. Still **decodes** to `Durable`; never emitted.    |
| 0x03 | Buffered  | Ack after memory write; fsync is deferred                             |

> **`0x02` is retired and permanently reserved.** It was `Batched`, which
> fsynced once per batch while `Sync` fsynced once per record; both moved to the
> batch-boundary group fsync and the distinction stopped existing. A daemon
> still accepts `0x02` so producers built against weir 1.x keep working, and
> canonicalises it to `Durable` — the encoder never emits `0x02`. It will not be
> reassigned to a future tier, because that would silently mis-tier every 1.x
> client.
```

- [ ] **Step 2: Update the metric label docs and add the migration note**

In `docs/monitoring.md`, change every `(`sync`/`batched`/`buffered`)` to
``(`durable`/`buffered`)``, and add immediately above the metrics table:

```markdown
> **Changed in 2.0.0 — dashboards need a one-line edit.** The `tier` label was
> `sync`/`batched`/`buffered` and is now `durable`/`buffered`. `sync` and
> `batched` were separate series for identical behaviour; they are one series
> named `durable`. Any panel or alert selecting `tier="sync"` or
> `tier="batched"` will silently return no data until updated — panels go blank
> rather than erroring, so grep your dashboards rather than waiting to notice.
```

- [ ] **Step 3: Sweep the remaining prose**

```bash
grep -rn 'Batched\|Durability::Sync' README.md docs/*.md docs/getting-started docs/operations 2>/dev/null
```

For each hit: if it describes the *current* tiers, rewrite for two tiers. If it is a historical statement ("Batched used to…"), keep it but make the past tense explicit. **Do not touch `docs/explorations/`, `docs/superpowers/` or dated benchmark files** — those are historical records of what was true when written, and rewriting them falsifies the archive.

- [ ] **Step 4: Verify no stale claim survives in live docs**

```bash
grep -rn 'Sync and Batched\|three durability tiers\|three tiers' README.md docs/*.md docs/getting-started docs/operations 2>/dev/null || echo "clean"
```

Expected: `clean`.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/
git commit -F - <<'MSG'
docs: two tiers everywhere, and a loud note about the metric label

The tier label moves from sync/batched to durable, which breaks dashboards
SILENTLY — panels go blank rather than erroring — so monitoring.md leads with
the migration rather than burying it in a table.

docs/explorations, docs/superpowers and dated benchmarks are deliberately
untouched: they record what was true when written, and editing them would
falsify the archive.
MSG
```

---

### Task 6: Version bump to 2.0.0 and the full gate

**Files:**
- Modify: `Cargo.toml` (workspace version), each crate's `Cargo.toml` internal dep pins, `Cargo.lock`, `CHANGELOG.md`

**Interfaces:**
- Consumes: Tasks 1–5 complete and green.

- [ ] **Step 1: Bump the workspace version and internal pins**

```bash
grep -rn '^version = "1\.3\.1"' Cargo.toml crates/*/Cargo.toml
grep -rn '1\.3\.1' crates/*/Cargo.toml
```

Set the workspace `version` to `2.0.0` and update every internal dependency pin (`weir-core = { version = "1.3.1", ... }` → `"2.0.0"`). Then:

```bash
cargo update --workspace
```

- [ ] **Step 2: Write the CHANGELOG entry**

Add at the top of `CHANGELOG.md`, under the existing header:

```markdown
## [2.0.0] - 2026-08-26

### Breaking

- **`Durability` has two tiers, not three.** `Sync` and `Batched` carried the
  same guarantee since both moved to the batch-boundary group fsync. They are
  now one tier, `Durable`. `Durability::Sync` and `Durability::Batched` remain
  as deprecated aliases, so code that *constructs* a tier keeps compiling with a
  warning; an exhaustive `match` over three variants does not.
- **The `tier` metric label changed** from `sync`/`batched`/`buffered` to
  `durable`/`buffered`. Dashboards selecting the old values return no data
  silently. See `docs/monitoring.md`.
- Wire compatibility is **preserved**: `0x02` still decodes to `Durable`, so
  producers built against 1.x keep working. The encoder only ever emits `0x01`,
  and `0x02` is permanently reserved.

### Added

- `SinkBatch` with a dedup token, replacing `SinkRecord`.
- WAB on-disk format v2 with optional zstd compression (`wab_compression`).
  Default remains `none`, which is byte-identical to 1.x. **Enabling it is a
  one-way door** — a 1.x daemon refuses to read a v2 segment.
- `wab_max_bytes` backpressure cap with a rejection counter and growth warning.
- Quarantine tooling: `weir-ctl quarantine list | inspect | requeue`, plus a
  gauge and recovery-failure counter.

### Testing

- Five chaos soaks across three venues and two architectures:
  **100,606,180 acked records over 1,226 `kill -9` crashes, zero durability
  violations, zero records excused by the frontier exemption.** Recovery
  truncated exactly 4.000 torn tails per kill in all 1,226. See
  `docs/benchmarks/chaos-soak/2026-08-26-three-venue-comparison.md`.
- Known gap: **power loss remains untested.** Every fault above is a process
  kill, which does not lose the page cache.
```

- [ ] **Step 3: Run the full gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
# weir-server's bin unit tests MUST run serially — socket::bind_hardened sets a
# PROCESS-GLOBAL umask, so a parallel `cargo test --workspace` produces ~75
# spurious PermissionDenied failures. See CONTRIBUTING.md:48 and ci.yml:65-67.
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
```

Expected: all clean. **Do not proceed past this step on a failure** — every later step is harder to undo.

- [ ] **Step 4: Verify the packages actually build for publish**

```bash
for c in weir-core weir-wab weir-sink-sdk weir-client weir-server weir-ctl weir-rs; do
  echo "=== $c ==="; cargo package -p $c --allow-dirty --quiet && echo OK || echo FAILED
done
```

Expected: all OK. This catches missing files and bad dep pins before anything is irreversible.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/*/Cargo.toml CHANGELOG.md
git commit -m "chore(release): 2.0.0"
```

---

### Task 7: Merge to main — STOP AND ASK FIRST

**This task modifies a shared branch. Do not begin it without the human partner's explicit go-ahead.**

**Files:** none — git operations only.

- [ ] **Step 1: Confirm the chain is still linear**

```bash
git merge-base --is-ancestor v2/main-line HEAD && echo "linear" || echo "DIVERGED — stop"
git rev-list --count main..HEAD
git log --oneline main..HEAD | tail -3
```

Expected: `linear`, ~137 commits.

- [ ] **Step 2: Merge**

```bash
git checkout main
git pull
git merge --no-ff chaos/wab-v2-soak -m "Merge weir 2.0.0"
```

- [ ] **Step 3: Re-run the full gate on the merged result**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
# weir-server's bin unit tests MUST run serially — socket::bind_hardened sets a
# PROCESS-GLOBAL umask, so a parallel `cargo test --workspace` produces ~75
# spurious PermissionDenied failures. See CONTRIBUTING.md:48 and ci.yml:65-67.
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
```

Expected: clean. **A failure here stops everything** — nothing is pushed yet, so the merge is local and recoverable with `git reset --hard origin/main`.

- [ ] **Step 4: Push and tag**

```bash
git push origin main
git tag -a v2.0.0 -m "weir 2.0.0"
git push origin v2.0.0
```

**Tag the merge commit, not a `[skip ci]` bot commit** — tagging a skipped commit means `release.yml` never runs and no GitHub Release is built.

---

### Task 8: Publish to crates.io — STOP AND ASK FIRST

**Publishing is irreversible. A published version can be yanked but never replaced. Do not begin without the human partner's explicit go-ahead, and confirm CI is green on the tag first.**

- [ ] **Step 1: Confirm CI is green on the tag**

```bash
gh run list --limit 5
```

Expected: the `v2.0.0` run passing, including Windows.

- [ ] **Step 2: Dry-run every crate in dependency order**

```bash
for c in weir-core weir-wab weir-sink-sdk weir-client weir-server weir-ctl weir-rs; do
  echo "=== $c ==="; cargo publish -p $c --dry-run || break
done
```

A dry run **cannot** catch a 403 ownership error — it only validates packaging. Ownership was the failure mode that forced the `weir` → `weir-rs` facade rename, so if any crate name is new, verify ownership before the real publish.

- [ ] **Step 3: Publish in dependency order, waiting between each**

```bash
cargo publish -p weir-core       # then wait for the index to update
cargo publish -p weir-wab
cargo publish -p weir-sink-sdk
cargo publish -p weir-client
cargo publish -p weir-server
cargo publish -p weir-ctl
cargo publish -p weir-rs
```

Each crate must be live on the index before the next one referencing it will resolve. If a publish fails midway, **stop** — the already-published crates cannot be unpublished, and the remaining ones can be published once the cause is fixed.

- [ ] **Step 4: Verify**

```bash
for c in weir-core weir-wab weir-sink-sdk weir-client weir-server weir-ctl weir-rs; do
  printf "%-16s " "$c"; curl -s "https://crates.io/api/v1/crates/$c" | python3 -c "import sys,json; print(json.load(sys.stdin)['crate']['max_version'])"
done
```

Expected: `2.0.0` for all seven.

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| Two variants, `Durable = 0x01`, `Buffered = 0x03` | 1 |
| `0x02` decodes to `Durable` | 1, and frozen by 4 |
| Encoder only emits `0x01` | 1, guarded by 4 |
| `0x02` never reassigned | 1 (docs), 5 (wire_protocol.md) |
| Deprecated `Sync`/`Batched` consts | 1 |
| `Display` emits `"durable"` | 1 |
| Metric label renames | 2, documented in 5 |
| Behaviour unchanged (no fsync moves) | Global constraint; Task 2 Step 2 calls it out explicitly |
| `WIRE_VERSION` not bumped | Global constraint; no task touches it |
| 2.0.0, merge, publish order | 6, 7, 8 |

**Placeholder scan:** No TBDs. Every code step carries the actual code; every command step names the expected output.

**Type consistency:** `Durability::Durable` and `Durability::Buffered` defined in Task 1 and used with those exact names in 2, 3, 4. `UnknownDurability` unchanged. The deprecated consts are `Durability::Sync` / `Durability::Batched` in both Task 1 and the CHANGELOG in Task 6.

**Known risk, accepted:** Task 4 Step 2 is conditional on the vector runner's shape, which the plan cannot know without reading it — Step 1 exists to establish that first, and the requirement it must satisfy is stated unconditionally regardless of shape.

**Deliberate stop-points:** Tasks 7 and 8 both begin with an explicit gate. Merging to `main` and publishing to crates.io are outward-facing and effectively irreversible; an agent must not perform either on its own initiative.
