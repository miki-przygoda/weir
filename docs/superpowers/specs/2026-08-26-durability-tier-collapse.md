# Two tiers, because there are two behaviours

## Problem

`Durability` offers three tiers. It has two behaviours.

`crates/weir-core/src/durability.rs` says so itself:

> `Sync` and `Batched` carry the **same durability guarantee today**: the record
> is written and `fdatasync`ed — as part of one batch-boundary group fsync —
> *before* the ACK is sent.

So does `docs/wire_protocol.md`:

> **Sync and Batched share the same durability guarantee.** Both fdatasync at the
> batch boundary before acking every record in the batch … there is no
> durability or speed distinction between the two tiers.

This is documented confusion rather than hidden confusion, which makes it worse,
not better: every reader has to learn the distinction, discover it is not a
distinction, and then carry that knowledge. The historical difference (`Sync`
fsynced per record, `Batched` per batch) stopped existing when both moved to the
batch-boundary group fsync.

It leaks into observability too. `weir_records_{accepted,ack,nack}_total{tier}`
carries `sync` and `batched` as separate series for identical behaviour, so any
dashboard that breaks out by tier shows a distinction that is not real.

## Decision

Collapse to the two tiers that exist:

```rust
#[repr(u8)]
pub enum Durability {
    Durable  = 0x01,   // fdatasync at the batch boundary before ACK
    Buffered = 0x03,   // ACK after the in-memory write; no fsync
}
```

`Buffered` keeps `0x03`. The durable tier keeps `0x01`. **`0x02` is not
reassigned** — it is retired, and remains permanently reserved.

### The wire stays backward-compatible

`0x02` still *decodes*:

```rust
0x01 | 0x02 => Ok(Durability::Durable),
0x03        => Ok(Durability::Buffered),
v           => Err(UnknownDurability(v)),
```

The encoder only ever emits `0x01`. This matters because **weir 1.3.1 is live on
crates.io**, `0x02` is a published contract in `docs/wire_protocol.md` and in
`docs/conformance/wire_v1_vectors.json`, and a 1.x client pushing `Batched`
today must keep working against a 2.0 daemon. Rejecting `0x02` would buy nothing
and strand those users.

Asymmetric decode/encode is the point, not an oversight: **accept what old
clients send, emit only what the current model describes.**

### Rust callers get a deprecation path, not a wall

```rust
impl Durability {
    #[deprecated(note = "renamed to `Durable`")]
    pub const Sync: Self = Self::Durable;
    #[deprecated(note = "`Batched` was always identical to `Sync`; use `Durable`")]
    pub const Batched: Self = Self::Durable;
}
```

Measured across the workspace: **190 constructions/comparisons versus 7 match
arms**, and all 7 match arms are inside weir's own code. So ~96% of real usage is
`push(payload, Durability::Sync)`, which keeps compiling with a deprecation
warning. `Durability` derives `PartialEq, Eq`, so the consts are usable in
patterns as well, though a `match` with both `Sync` and `Durable` arms will
warn about an unreachable pattern — correctly, since they are the same value.

### The metric label renames too

`Display` emits `"durable"`, so the tier label becomes `durable` / `buffered`.

This **breaks dashboards silently** — panels go blank rather than erroring — and
it is still the right call. `tier="batched"` disappears under any collapse, and
that part is a fix: it was a duplicate series for identical behaviour. Keeping
the remaining label as `"sync"` while the API says `Durable` would bake the
name-versus-reality drift that caused this problem back in one layer lower.
A major version is when a coherent break is cheapest.

The migration is a one-line dashboard edit, and it gets stated loudly in
`CHANGELOG.md` and `docs/monitoring.md`.

## What breaks, precisely

| Surface | Effect |
|---|---|
| Rust: `Durability::Sync` / `::Batched` as a value | compiles, deprecation warning |
| Rust: `match` on `Durability::Sync` alongside `Durable` | unreachable-pattern warning |
| Rust: exhaustive `match` over all three variants | **breaks** — only two exist |
| Wire: client sends `0x02` | works, decodes to `Durable` |
| Wire: daemon emits tier byte | always `0x01`, never `0x02` |
| Metrics: `tier="sync"` | **becomes `tier="durable"`** |
| Metrics: `tier="batched"` | **disappears** |
| Conformance vectors | the `Batched` vector is retained as a *decode* case |

## Out of scope

- Changing what either tier actually does. This is a naming and surface change;
  the fsync behaviour is untouched.
- `WIRE_VERSION`. The frame layout is unchanged and `0x02` still decodes, so no
  wire-version bump is warranted.
- Reassigning `0x02` to any future tier. It stays reserved — reusing a retired
  wire value is how a protocol acquires a permanent trap.

## Release context

This lands as part of **2.0.0**. The branch chain is linear —
`main (0b8151b)` → … → `v2/main-line` → `chaos/dense-oracle` →
`chaos/wab-v2-soak` — **136 commits ahead of main**, carrying SinkBatch and the
dedup token, WAB format v2 with zstd, the `wab_max_bytes` backpressure cap, the
quarantine tooling, the dense verification oracle, and five chaos soaks totalling
100,606,180 acked records across 1,226 crashes.

Publishing order, which the workspace's internal dependencies require:
`weir-core` → `weir-wab` → `weir-sink-sdk` → `weir-client` → `weir-server` →
`weir-ctl` → `weir-rs`.
