# Buffering and recovery — how much burst weir absorbs, and what a restart costs

weir exists to absorb a burst that arrives faster than the sink can take it. Two
questions follow from that and neither had an answer: **how much** does it
absorb before something gives, and **how long** is it unavailable after a crash
while holding that buffer?

The A6 design named the first as a shipping obligation and it shipped without
one:

> A6 multiplies the rate at which the WAB outruns the drain by ~100x, against a
> `wab_max_bytes` that **defaults to 0 (disabled)**. A6 must not ship without a
> recommended value and a sizing rule.
> — `docs/superpowers/specs/2026-09-13-a6-wire-batching-design.md` §8

## What happens when ingest outruns the drain

It does not grow without limit. The buffer climbs to a ceiling, and from that
point the **producer advances at exactly the drain rate** — backpressure reaches
all the way back to `push`. On beast, ingesting flat out against a sink held to
three different rates:

| sink delay | ingest rec/s | drain rec/s | WAB settles at |
|---:|---:|---:|---:|
| 2 ms | 30,848 | 28,582 | 17.0 MB |
| 10 ms | 10,801 | 7,635 | 17.0 MB |
| 50 ms | 4,821 | 1,618 | 17.0 MB |

The same ceiling at every rate, including one where ingest outruns delivery 3:1.

## The ceiling is a segment count

Three hypotheses were tested; two were wrong.

| hypothesis | test | result |
|---|---|---|
| the bounded work queue (`QUEUE_CAPACITY = 65,536` records) | sweep payload 64 B → 4 KiB | **refuted** — record count moves 57x while bytes hold to 1.008x |
| a byte figure (~17 MB) | sweep `wab_segment_max_bytes` | **refuted** — bytes move 8x while segment count holds |
| `256 + shard_count` segments | sweep `shard_count` 1, 4, 8 | **refuted** — 259.0 at all three, identical to the byte |

Payload sweep (beast), holding segments at 64 KiB — bytes constant, records not:

| payload | plateau | backlog records |
|---:|---:|---:|
| 64 B | 16.92 MB | 234,052 |
| 256 B | 16.98 MB | 64,117 |
| 1 KiB | 17.06 MB | 16,448 |
| 4 KiB | 16.96 MB | 4,112 |

Segment-size sweep — segments constant, bytes not:

| `wab_segment_max_bytes` | plateau | plateau in segments |
|---:|---:|---:|
| 32 KiB | 8.53 MB | 260.3 |
| 64 KiB | 16.98 MB | 259.0 |
| 256 KiB | 67.65 MB | 258.1 |

**~259 segments, and the constant is in the tree.** `main.rs` wires the WAB to
the drain with `crossbeam_channel::bounded::<PathBuf>(256)` — one global channel
of sealed-segment paths, not one per shard. When the drain falls behind it fills,
the writer's blocking send stalls, and that stall is the backpressure. Shard
count does not enter it: these scenarios push from one connection, which lands on
one shard, so the number of open segments never varies with how many are
configured.

## This does **not** make the default safe

`wab_segment_max_bytes` defaults to **256 MiB**. At that size ~259 segments is
**~66 GB** — on any realistic disk the filesystem fills long before the ceiling
binds. §8's concern therefore stands at shipped defaults; the plateau above is a
property of the small 64 KiB segments those scenarios pin, not a safety net weir
gives you for free.

What the measurement *does* give an operator is the knob:

```
buffer before backpressure  ~=  256 x wab_segment_max_bytes
```

That is `wab_segment_max_bytes` — **not** `wab_max_bytes` — and it is a far
blunter instrument than it looks: the default puts the ceiling past the disk, and
lowering it to bound the buffer also makes segments small enough to affect drain
batching. Set `wab_max_bytes` if you want a byte bound; use the relation above to
understand what happens if you do not.

## What a restart costs

Fill the WAB with the sink held down, `kill -9`, restart, time until the socket
accepts again.

beast — ext4 on a Samsung 850 EVO SATA SSD:

| backlog held | WAB on disk | time to ready |
|---:|---:|---:|
| 65,536 records | 67.7 MB | **20 ms** |
| 262,144 records | 228.2 MB | 60 ms |
| 524,288 records | 536.5 MB | **100 ms** |

M3 Max — APFS on NVMe:

| backlog held | WAB on disk | time to ready |
|---:|---:|---:|
| 65,536 records | 61.9 MB | 54 ms |
| 262,144 records | 251.1 MB | 163 ms |
| 524,288 records | 525.5 MB | 322 ms |

**Linear in the backlog, and fast** — roughly 0.19 ms per MiB on beast and
0.64 ms per MiB on the Mac. Half a gigabyte of unconfirmed buffer comes back in
a tenth of a second on beast. Recovery time is not a reason to keep the buffer
small.

## Reproducing

```sh
cargo test -p weir-server --test load_drain --release --features http-sink -- \
  --ignored --nocapture --test-threads=1 wab_growth_when_ingest_outruns_the_sink
# ... what_sets_the_wab_plateau
# ... the_plateau_law_predicts_the_shard_count_sweep
# ... recovery_time_vs_backlog_size
```

A note on the recovery harness, because it was wrong first and the way it was
wrong is easy to repeat: its first version used the noop sink, which drains as
fast as ingest fills, so the WAB was empty at kill time and it timed the startup
of a daemon holding nothing — 160/20/20 ms for nominally 64/256/512 MiB. It now
holds the sink failing, and **asserts the WAB is non-empty before killing**, so
it cannot silently regress to measuring startup again.
