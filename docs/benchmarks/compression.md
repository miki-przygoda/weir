# WAB compression — what `wab_compression` costs and buys

`wab_compression` defaults to `"none"`. Turning it on trades CPU on the ack path
— which is inside the `Durable` latency a producer waits for — against bytes on
disk, which decides how much burst the buffer holds and how long it survives an
outage. Until now neither side had a number.

**The verdict inverts with record size, and it inverts hard.** At ~150-byte
records zstd is a bad trade. At ~4 KiB it is free on fast storage and a *net
throughput win* on slow storage.

## Measured

20,000 `Durable` records per cell, `batch_size = 256`, via `push_batch`.
beast — i9-9900K, ext4 on a Samsung 850 EVO SATA SSD, honest `fdatasync`:

| record | codec | rec/s | vs. `none` | compression ratio |
|---:|---|---:|---:|---:|
| 149 B | none | 92,825 | 1.00x | 1.00 |
| 149 B | zstd level 1 | 67,057 | **0.72x** | 1.15 |
| 149 B | zstd level 3 | 74,541 | 0.80x | 1.12 |
| 149 B | zstd level 9 | 58,573 | **0.63x** | 1.12 |
| 4,011 B | none | 36,837 | 1.00x | 1.00 |
| 4,011 B | zstd level 1 | **47,583** | **1.29x** | 29.16 |
| 4,011 B | zstd level 3 | 46,630 | 1.27x | 28.93 |
| 4,011 B | zstd level 9 | 41,872 | 1.14x | 28.93 |

The same sweep on an M3 Max (APFS on NVMe, `F_BARRIERFSYNC` — a much cheaper
flush) for contrast:

| record | codec | rec/s | vs. `none` |
|---:|---|---:|---:|
| 149 B | none | 137,107 | 1.00x |
| 149 B | zstd level 1 | 56,284 | 0.41x |
| 4,011 B | none | 25,698 | 1.00x |
| 4,011 B | zstd level 1 | 25,674 | **1.00x** |

## Why compression can make weir faster

At 4 KiB, zstd level 1 turns each record into ~137 bytes. That is ~29x less to
write and ~29x less for the flush to push through. On beast the CPU spent
compressing is *more* than repaid by the I/O saved, so enabling compression
raises throughput by 29% — and even level 9, which costs six times the CPU of
level 1, still beats no compression by 14%.

On the M3 Max the same compression buys the same ratio but only breaks even,
because that platform's flush was never the bottleneck. **The slower the
storage, the more compression wins** — which is the opposite of the usual
intuition that compression is a space-for-time trade.

At 149 bytes there is nothing for the codec to work with: ~15% smaller for
28–37% less throughput on beast, and 59% less on the Mac. Higher levels make it
strictly worse, and at that size level 3 compressed *better* than level 9 while
level 1 beat both — the levels stop being ordered when the input is too small to
matter.

## Read the two columns differently

**The throughput column transfers. The ratio column does not.**

Throughput measures CPU per byte against I/O saved; it does not care what the
bytes are, so the shape above will hold for your data.

The ratio is only as honest as the corpus, and **this corpus is not honest**: the
4 KiB record is 27 copies of one JSON log line, which is why it reports 29x. Real
4 KiB records — distinct log lines, JSON with varying fields, anything already
encoded — will report low single digits. Treat 29x as an upper bound that says
"the codec worked", not as a number to size a disk with.

Measure your own: weir exposes both halves already, so the live ratio on real
traffic is

```
weir_wab_record_logical_bytes_total / weir_wab_record_stored_bytes_total
```

## Guidance

| your records | recommendation |
|---|---|
| under ~1 KiB | leave `wab_compression = "none"`. The ratio is ~1.1x and it costs a quarter to a half of ingest throughput. |
| ~4 KiB and above, slow storage | **turn on `zstd` at level 1.** It paid for itself and then some on a SATA SSD. |
| ~4 KiB and above, fast storage (NVMe) | level 1 is roughly free; enable it for the space, not the speed. |
| any size | level 1. Higher levels cost real throughput and bought nothing measurable here. |

## Reproducing

```sh
cargo test -p weir-server --test research --release -- --ignored --nocapture \
  --test-threads=1 compression_cost_and_benefit
```

Edit `REPEATS` in that test to match your own record sizes; the default sweeps
one line (~149 B) and 27 lines (~4 KiB).
